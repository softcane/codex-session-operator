"""Bounded actual Pod/network/helper faults; simulated provider outcomes are labeled."""

import ipaddress
import json
import secrets
import subprocess
import time
import uuid

import sandbox
import support

NS, ensure = support.NS, support.ensure


def run(checks, api, base, report, evidence, runtime, counts):
    image, architecture, commits = runtime
    def detail(path):
        status, value = api.request(path)
        return value if status == 200 else None
    def create(name, text):
        status, receipt = api.request(base+"/conversations", "POST", {"route_id": name, "text": text}, name+"-start")
        ensure(status == 202, "Fault input was not durably accepted")
        return receipt["conversation_url"]
    def close(name, path):
        ensure(api.request(path+"/close", "POST", {}, name+"-close")[0] == 202, "Fault prevented durable close")
        checks.wait("fault conversation cleaned", lambda: value if (value := detail(path)) and support.cleaned_up(value) else None, 240)
        checks.kubectl("delete", "profile/"+name, "environment/"+name, "-n", NS, "--wait=true", "--timeout=120s")
    def published(name, url=None):
        sandbox.profile(checks, name, image, architecture, commits["public"], repository_url=url)
    # A11: observe a real running child marker before deleting its exact owned Pod.
    name="sandbox-interrupted-command"
    published(name)
    before=counts()
    path=create(name,"SANDBOX_LONG_COMMAND: run the maintained interruption fixture")
    _,session,pod=support.wait_executor(checks,lambda:detail(path),"long-command current executor")
    pod_name=pod["metadata"]["name"]
    marker=checks.wait("actual child command start marker",lambda: child_marker(checks,pod_name),30)
    remote_id=session["status"]["openaiSessionID"]
    running=checks.wait("saved matching root remains in progress",lambda:running_root(detail(path),marker,counts(),remote_id),20)
    roots=[turn for turn in (running.get("history") or {}).get("turns") or [] if not turn.get("subagent_id")]
    ensure(len(roots)==1 and roots[0]["status"]=="in_progress" and marker["stage"]=="running",
           "Interruption was not observed during an actual running command")
    root_id=roots[0]["id"]
    old_uid=pod["metadata"]["uid"]
    current=checks.get("pod",pod_name)
    ensure(current["metadata"]["uid"]==old_uid and current["status"]["containerStatuses"][0]["containerID"]==pod["status"]["containerStatuses"][0]["containerID"],
           "Interruption target Pod/process instance changed")
    checks.kubectl("exec","-n",NS,pod_name,"-c","executor","--","node","-e",
        "try{process.kill(Number(process.argv[1]),0);console.log('running');}catch{process.exit(1);}",str(marker["pid"]),timeout=5)
    checks.kubectl("delete","pod",pod_name,"-n",NS,"--wait=true","--timeout=120s")
    interrupted=checks.wait("interrupted original root has honest failed/unknown outcome",lambda:value if
        (value:=detail(path)) and any(turn["id"]==root_id and turn["status"]=="failed" for turn in (value.get("history") or {}).get("turns") or []) else None,180)
    ensure(len([turn for turn in (interrupted.get("history") or {}).get("turns") or [] if not turn.get("subagent_id")])==1,
           "Interruption allocated an extra root or transparently replayed input")
    ensure(any(item.get("type")=="command_execution" and item.get("status")=="failed" and
               "outcome unknown" in item.get("output","") for item in (interrupted.get("history") or {}).get("items") or []),
           "Interrupted command was falsely reported completed")
    ensure(counts()["executorStarts"]==before["executorStarts"]+1 and counts()["messageCommits"]==before["messageCommits"]+1,
           "Interrupted command was dispatched or accepted again")
    latest=checks.get("session",session["metadata"]["name"])
    ensure(latest["metadata"]["uid"]==session["metadata"]["uid"] and latest["status"]["openaiSessionID"]==remote_id,
           "Interruption replaced the managed conversation identity")
    evidence.write("interrupted-command-proof.json",{"rootID":root_id,"oldPodUID":old_uid,"sessionUID":session["metadata"]["uid"],
        "remoteID":remote_id,"outcome":"failed with unknown command effects; no automatic replay","actualChildPID":marker["pid"]})
    report["scenarios"].append({"id":"mid_command_loss_no_transparent_replay","status":"passed"})
    close(name,path)
    # A17: a nonempty but rejected fixture grant reaches real compute and fails there.
    name="sandbox-rejected-executor-grant"
    rejected="fixture-rejected-executor-grant"
    evidence.protect([rejected]);checks.keys["rejected-executor-grant"]=rejected
    checks.secret("codex-executor",{"CODEX_API_KEY":rejected})
    try:
        published(name)
        before=counts()["messageCommits"]
        path=create(name,"This input must stay held behind a rejected executor grant")
        session=checks.wait("actual executor grant rejection",lambda:failed_container(checks,detail(path)),180)
        ensure(counts()["messageCommits"]==before,"Rejected executor grant delivered normal task input")
        close(name,path)
        report["scenarios"].append({"id":"actual_pod_simulated_executor_grant_rejection","status":"passed"})
    finally:
        checks.secret("codex-executor",{"CODEX_API_KEY":"fixture-executor-key-not-real"})
    # A17: approved HTTPS source exists in policy but its owned Service is absent.
    name="sandbox-unreachable-git"
    published(name,sandbox.UNREACHABLE_URL)
    before=counts()["messageCommits"]
    path=create(name,"This input must stay held behind unreachable Git")
    checks.wait("unreachable approved Git fails real init",lambda:failed_container(checks,detail(path),init=True),240)
    ensure(counts()["messageCommits"]==before,"Unreachable preparation delivered a task")
    close(name,path)
    report["scenarios"].append({"id":"unreachable_approved_https_git_real_init","status":"passed"})
    network_fault(checks,api,base,report,evidence,runtime,counts,detail,create,close,published)
    storage_fault(checks,image,commits["public"],report,evidence)


def running_root(value,marker,counts,session_id):
    roots=[turn for turn in ((value or {}).get("history") or {}).get("turns") or [] if not turn.get("subagent_id")]
    job=next((job for job in counts.get("jobs",[]) if job["id"]==marker.get("job") and job["session_id"]==session_id and job.get("started")),None)
    return value if job and job.get("instance") and len(roots)==1 and roots[0]["id"]==job["turn_id"] and roots[0]["status"]=="in_progress" else None


def child_marker(checks,pod):
    try:
        return json.loads(checks.kubectl("exec","-n",NS,pod,"-c","executor","--","cat","/workspace/interrupted-started.json",timeout=10))
    except support.CommandError as error:
        if "No such file or directory" in error.detail:return None
        raise


def failed_container(checks,value,init=False):
    if not value or not value["summary"].get("session_uid"):return None
    session=checks.get("session",value["summary"]["session_name"])
    name=(session or {}).get("status",{}).get("podName")
    pod=checks.get("pod",name) if name else None
    key="initContainerStatuses" if init else "containerStatuses"
    for instance in (pod or {}).get("status",{}).get(key,[]):
        state=instance.get("state",{}).get("terminated") or instance.get("lastState",{}).get("terminated")
        if state and state.get("exitCode")!=0 and not support.current_ready(session):return session
    return None


def packet_probe(checks,pod,provider_ip=None):
    address=str(ipaddress.IPv4Address(provider_ip or checks.get("pod","provider-fixture")["status"]["podIP"]))
    program="""const net=require('node:net');process.stdout.write('probe-started\\n');
let settled=false;const socket=net.createConnection({host:process.argv[1],port:8080,family:4});
const hard=setTimeout(()=>finish('denied'),2000);
function finish(value,code=0){if(settled)return;settled=true;clearTimeout(hard);socket.destroy();process.stdout.write(value+'\\n',()=>process.exit(code));}
socket.once('connect',()=>finish('reachable'));socket.setTimeout(1500,()=>finish('denied'));
socket.once('error',error=>{const denied=['ECONNREFUSED','ECONNRESET','ETIMEDOUT','EHOSTUNREACH','ENETUNREACH','EACCES'].includes(error.code);finish(denied?'denied':'probe-error',denied?0:2);});"""
    try:
        output=checks.kubectl("exec","-n",NS,pod,"-c","executor","--","node","-e",program,address,timeout=15)
    except subprocess.TimeoutExpired as error:
        observed=error.stdout or ""
        if isinstance(observed,bytes):observed=observed.decode(errors="replace")
        error.add_note("Packet probe transport timed out; started sentinel observed: "+str("probe-started" in observed))
        raise
    lines=output.splitlines()
    ensure(lines and lines[0]=="probe-started" and lines[-1] in ("reachable","denied"),
           "Packet probe did not produce an independent started/network outcome")
    return lines[-1]


def network_fault(checks,api,base,report,evidence,runtime,counts,detail,create,close,published):
    name="sandbox-denied-egress"
    published(name)
    path=create(name,"Establish the maintained network fault baseline")
    _,session,pod=support.wait_executor(checks,lambda:detail(path),"network fault healthy baseline")
    checks.wait("network baseline root completed",lambda:support.completed_history(detail(path),1),90)
    pod_name,uid=pod["metadata"]["name"],pod["metadata"]["uid"]
    environment_id=session["status"]["environmentID"]
    def connection():
        program="fetch('http://127.0.0.1:8080/v1/agents/environments/'+process.argv[1],{signal:AbortSignal.timeout(2000)}).then(r=>{if(!r.ok)process.exit(2);return r.json();}).then(v=>process.stdout.write(JSON.stringify(v)+'\\n',()=>process.exit(0))).catch(()=>process.exit(2));"
        return json.loads(checks.kubectl("exec","pod/provider-fixture","-n",NS,"-c","provider","--","node","-e",program,environment_id,timeout=10))
    baseline_connection=connection()
    service_ip=str(ipaddress.IPv4Address(checks.get("service","provider-fixture")["spec"]["clusterIP"]))
    ensure(packet_probe(checks,pod_name)=="reachable","Network baseline connection did not succeed")
    ensure(packet_probe(checks,pod_name,service_ip)=="reachable","Actual service-route baseline connection did not succeed")
    checks.apply({"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":name,"namespace":NS},
        "spec":{"podSelector":{"matchLabels":{"sandbox.users.io/session-uid":session["metadata"]["uid"]}},"policyTypes":["Egress"],"egress":[]}})
    rule=None
    boundary="actual CNI NetworkPolicy enforcement"
    firewall_packets=None
    denied_connection=None
    try:
        probe=packet_probe(checks,pod_name)
        service_probe=packet_probe(checks,pod_name,service_ip)
        # Existing channels may remain usable after NetworkPolicy blocks new TCP.
        # Wait for actual heartbeat expiry instead of manufacturing disconnection.
        cutoff=time.monotonic()+8
        current_connection=connection()
        while current_connection.get("status")=="connected" and time.monotonic()<cutoff:
            time.sleep(0.5)
            current_connection=connection()
        if probe!="denied" or service_probe!="denied" or current_connection.get("status")=="connected":
            source=str(ipaddress.IPv4Address(pod["status"]["podIP"]))
            destination=str(ipaddress.IPv4Address(checks.get("pod","provider-fixture")["status"]["podIP"]))
            node=pod["spec"]["nodeName"]
            latest=checks.get("pod",pod_name)
            ensure(node==checks.cluster+"-control-plane" and latest["metadata"]["uid"]==uid and latest["status"]["podIP"]==source,
                   "Node firewall fixture target ownership changed")
            # Source UID/IP + destination port covers service NAT and established
            # channels without affecting any other Pod or controller traffic.
            rule=["-s",source,"-p","tcp","--dport","8080","-m","comment","--comment","codex-fixture-"+uid,"-j","DROP"]
            checks.run(["docker","exec",node,"iptables","-t","raw","-I","PREROUTING","1",*rule])
            boundary="CNI new-connection policy plus fixture node firewall for established transport" if probe=="denied" else "fixture node firewall (Kind NetworkPolicy enforcement unavailable)"
        ensure(packet_probe(checks,pod_name)=="denied","Declared/test-fixture egress denial did not block a real packet probe")
        ensure(packet_probe(checks,pod_name,service_ip)=="denied","Actual executor service route remained usable during fixture denial")
        denied_connection=checks.wait("actual executor heartbeats cease",lambda:value if
            (value:=connection()).get("status")=="disconnected" else None,30)
        last_seen=denied_connection.get("connectedAt")
        if rule:
            listing=checks.run(["docker","exec",pod["spec"]["nodeName"],"iptables","-t","raw","-L","PREROUTING","-n","-v","-x"])
            matching=[line for line in listing.splitlines() if "codex-fixture-"+uid in line]
            ensure(len(matching)==1 and int(matching[0].split()[0])>0,"Captured firewall did not observe real blocked packets")
            firewall_packets=int(matching[0].split()[0])
        checks.wait("disconnected executor gates queued input",lambda:(value:=detail(path)) and not value["lifecycle"]["execution_ready"],30)
        before=counts()["messageCommits"]
        ensure(api.request(path+"/messages","POST",{"text":"Input must stay held during denied executor connectivity"},name+"-held")[0]==202,
               "Denied connectivity lost durable input admission")
        until=time.monotonic()+5
        observations=0
        while True:
            current=checks.get("pod",pod_name)
            ensure(current and current["metadata"]["uid"]==uid and current["status"]["containerStatuses"][0]["containerID"]==pod["status"]["containerStatuses"][0]["containerID"],
                   "Denied-connectivity observation changed its executor instance")
            value=detail(path)
            ensure(value and not value["lifecycle"]["execution_ready"] and value["lifecycle"]["delivery_held"] and
                   counts()["messageCommits"]==before,"Denied connectivity delivered input or lost its qualified hold")
            observed_connection=connection()
            ensure(observed_connection.get("status")=="disconnected" and observed_connection.get("connectedAt")==last_seen,
                   "Actual executor heartbeat resumed during denied connectivity")
            observations+=1
            if time.monotonic()>=until and observations>=3:break
            time.sleep(0.5)
        # Close remains usable while connection/readiness is held.
        ensure(api.request(path+"/close","POST",{},name+"-close")[0]==202,"Denied connectivity prevented close")
        checks.wait("denied executor closed and cleaned",lambda:(value:=detail(path)) and support.cleaned_up(value),240)
        evidence.write("network-denial-proof.json",{"podUID":uid,"sessionUID":session["metadata"]["uid"],"boundary":boundary,
            "baselineProbe":"reachable","deniedProbe":"denied","heldObservations":observations,"minimumHoldSeconds":5})
        evidence.write("network-heartbeat-proof.json",{"baselineCount":baseline_connection.get("heartbeatCount",0),
            "deniedCount":denied_connection.get("heartbeatCount",0),"lastAcceptedHeartbeat":last_seen,
            "blockedPacketCount":firewall_packets,"providerPodIP":checks.get("pod","provider-fixture")["status"]["podIP"],
            "serviceIP":service_ip,"serviceProbe":"denied","firewallTable":"raw PREROUTING" if rule else None,"boundary":boundary})
        report["scenarios"].append({"id":"owned_executor_denied_connectivity","status":"passed","boundary":boundary})
    finally:
        identity_changed=False
        observation_error=None
        try:
            if rule:
                try:
                    current=checks.get("pod",pod_name)
                    identity_changed=current is not None and (current["metadata"]["uid"]!=uid or current["status"]["podIP"]!=pod["status"]["podIP"])
                except Exception as error:
                    observation_error=error
                ensure(pod["spec"]["nodeName"]==checks.cluster+"-control-plane","Captured firewall node is not the owned cluster")
                # Captured source/destination/UID comment targets only our old rule,
                # even if the same Pod name now resolves to a replacement instance.
                checks.run(["docker","exec",pod["spec"]["nodeName"],"iptables","-t","raw","-D","PREROUTING",*rule])
        finally:
            checks.kubectl("delete","networkpolicy",name,"-n",NS,"--ignore-not-found","--wait=true","--timeout=90s")
        if observation_error:raise observation_error
        ensure(not identity_changed,"Firewall target identity changed; exact captured rule and policy were removed")
    checks.kubectl("delete","profile/"+name,"environment/"+name,"-n",NS,"--wait=true","--timeout=120s")


def storage_fault(checks,image,commit,report,evidence):
    name="bootstrap-storage-fault-"+uuid.uuid4().hex[:8]
    program="""const fs=require('node:fs'),cp=require('node:child_process');
fs.mkdirSync('/workspace-storage/lost+found');
let full=false;try{fs.writeFileSync('/workspace-storage/lost+found/fill',Buffer.alloc(2*1024*1024));}catch(e){if(e.code==='ENOSPC')full=true;else throw e;}
if(!full){console.log('Expected enforced tmpfs exhaustion');process.exit(2);}
const args=['--storage-root','/workspace-storage','--session-uid',process.argv[1],'--url',process.argv[2],'--commit',process.argv[3]];
const invoke=()=>cp.spawnSync('/workspace-bootstrap',args,{env:{PATH:process.env.PATH,HOME:'/tmp'},encoding:'utf8',timeout:10000});
const result=invoke(),stage='repository provenance could not be persisted; audit required';
const claim='/workspace-storage/staging.json';
const regular=fs.existsSync(claim)&&fs.lstatSync(claim).isFile()&&!fs.lstatSync(claim).isSymbolicLink();
const data={fault:'ENOSPC',bootstrapExit:result.status,bootstrapStage:result.stderr.trim(),projectExists:fs.existsSync('/workspace-storage/project'),emptyLocalClaim:regular&&fs.statSync(claim).size===0};
if(result.error||typeof result.status!=='number'||result.status===0||data.projectExists||data.bootstrapStage!==stage||!data.emptyLocalClaim){console.log(JSON.stringify(data));process.exit(1);}
if(process.argv[4]==='write-only'){data.recovery='not attempted: standalone network disabled';console.log(JSON.stringify(data));process.exit(0);}
fs.unlinkSync('/workspace-storage/lost+found/fill');
// Explicit fixture audit cleanup: only this observed zero-byte local claim is removed.
// No controller claims, finalizers, remote objects, or populated provenance are changed.
fs.unlinkSync(claim);
const retry=invoke();data.retryExit=retry.status;data.recovery='same recipe after removing filler and audited empty local claim';
data.preparedAfterRecovery=fs.existsSync('/workspace-storage/preparation.json');
console.log(JSON.stringify(data));if(retry.error||retry.status!==0||!data.preparedAfterRecovery)process.exit(1);"""
    checks.apply({"apiVersion":"v1","kind":"Pod","metadata":{"name":name,"namespace":NS},"spec":{
        "automountServiceAccountToken":False,"restartPolicy":"Never",
        "securityContext":{"runAsNonRoot":True,"runAsUser":1000,"runAsGroup":1000,"fsGroup":1000,"seccompProfile":{"type":"RuntimeDefault"}},
        "containers":[{"name":"fault","image":image,"imagePullPolicy":"IfNotPresent","command":["node","-e",program,str(uuid.uuid4()),sandbox.PUBLIC_URL,commit],
            "securityContext":{"allowPrivilegeEscalation":False,"readOnlyRootFilesystem":True,"capabilities":{"drop":["ALL"]}},
            "resources":{"requests":{"cpu":"100m","memory":"32Mi"},"limits":{"cpu":"1","memory":"128Mi"}},
            "volumeMounts":[{"name":"storage","mountPath":"/workspace-storage"},{"name":"tmp","mountPath":"/tmp"}]}],
        "volumes":[{"name":"storage","emptyDir":{"medium":"Memory","sizeLimit":"1Mi"}},{"name":"tmp","emptyDir":{}}]}})
    pod=checks.wait("real constrained helper filesystem fault completes",lambda:pod if
        (pod:=checks.get("pod",name)) and pod.get("status",{}).get("phase") in ("Succeeded","Failed") else None,60)
    output=checks.kubectl("logs",name,"-n",NS,"-c","fault")
    ensure(pod["status"]["phase"]=="Succeeded","Real storage fault qualification failed")
    value=json.loads(output)
    ensure(value["fault"]=="ENOSPC" and value["bootstrapExit"]!=0 and not value["projectExists"] and
           value["bootstrapStage"]=="repository provenance could not be persisted; audit required" and
           value["emptyLocalClaim"] and value["retryExit"]==0 and value["preparedAfterRecovery"],
           "Storage failure/write stage or independent recovery was not qualified")
    evidence.write("bootstrap-enospc-proof.json",value)
    report["scenarios"].append({"id":"production_bootstrap_real_tmpfs_enospc","status":"passed",
        "boundary":"actual restricted helper Pod 1Mi tmpfs; PVC request quotas not claimed"})
    checks.kubectl("delete","pod",name,"-n",NS,"--wait=true","--timeout=90s")
