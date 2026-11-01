// Controlled Agents API contract fixture. No inference, external writes or real keys.
const http = require('http');
const agents = new Map(), vaults = new Map(), sessions = new Map();
const templates = new Map(), runtimes = new Map(), credentials = new Map();
const executorJobs = new Map();
const eventStreams = new Map();
const heldToolResponses = new Map(), toolResultCommits = new Map();
let executorCommands = 0, executorStarts = 0;
function finishJob(job, result, unknown=false) {
  const session=sessions.get(job.session_id);
  if(!session)return;
  const command=session.items.find(item=>item.id==='command_'+job.turn_id);
  const item={id:'command_'+job.turn_id,type:'command_execution',turn_id:job.turn_id,command:job.argv.join(' '),
    cwd:result.cwd||'/workspace',status:unknown||result.exit_code!==0?'failed':'completed',
    output:unknown?'Executor process was replaced during command; outcome unknown; automatic replay disabled':result.output};
  if(!unknown)item.exit_code=result.exit_code;
  if(command)Object.assign(command,item);else session.items.push(item);
  session.items.push({id:'assistant_'+job.turn_id,type:'message',role:'assistant',turn_id:job.turn_id,
    content:[{type:'output_text',text:unknown?'Interrupted command outcome is unknown; no automatic replay':'Simulated executor observed actual Pod files and test output'}]});
  session.turns.find(turn=>turn.id===job.turn_id).status=unknown||result.exit_code!==0?'failed':'completed';
  session.remote.status='idle';executorCommands++;executorJobs.delete(job.id);
  if(!unknown && session.researchRole) {
    workflowAction(session,job.turn_id,'workflow_result',{outcome:result.exit_code===0?'success':'failed',outputs:[{
      name:{argument:'argument',experiment:'observations',review:'findings'}[session.researchRole],content:result.output,
      checks:[{command:item.command,output:item.output,exit_code:result.exit_code,source_item_id:item.id}],
      limitations:'Bounded integer fixture, controlled inference; no general scientific validation.'}],
      references:session.researchReferences||[]});
  } else if(!unknown && session.workflowMember) {
    const content='Actual workspace diagnosis: '+result.output;
    workflowAction(session,job.turn_id,'workflow_result',{outcome:result.exit_code===0?'success':'failed',outputs:[{name:'diagnosis',content,
      checks:[{command:item.command,output:item.output,exit_code:result.exit_code,source_item_id:item.id}],
      limitations:'Controlled model fixture; command output is from the deployed executor.'}]});
  } else if(!unknown)emit(session.id,{type:'agent.session.completed',session_id:session.id});
}
function emit(sessionID, value) {
  for (const response of eventStreams.get(sessionID) || []) response.write('data: '+JSON.stringify(value)+'\n\n');
}
function environmentEvent(runtime, sessionID) {
  return {type:'agent.session.environment.'+runtime.status,session_id:sessionID,
    environment:{id:runtime.id,type:'self_hosted',status:runtime.status}};
}
let creates = 0, deletes = 0, eventPosts = 0, messageCommits = 0;
let agentCreates = 0, vaultCreates = 0, templateCreates = 0, credentialCreates = 0, credentialWrites = 0;
let control = {};
const reply = (res, code, body) => {
  res.writeHead(code, {'Content-Type': 'application/json'});
  res.end(JSON.stringify(body));
};
const page = data => ({object: 'list', data, has_more: false});
const clone = value => JSON.parse(JSON.stringify(value));
function publicAuth(auth) {
  const result = {};
  for (const key of ['type', 'mcp_server_url', 'secret_name', 'networking', 'expires_at']) {
    if (key in auth) result[key] = clone(auth[key]);
  }
  if (auth.refresh) {
    result.refresh = {};
    for (const key of ['token_endpoint', 'client_id', 'resource', 'scope']) {
      if (key in auth.refresh) result.refresh[key] = clone(auth.refresh[key]);
    }
    if (auth.refresh.token_endpoint_auth) result.refresh.token_endpoint_auth = {type: auth.refresh.token_endpoint_auth.type};
  }
  return result;
}
// These branches replace only inference. Required actions travel through the same
// provider API as model actions; the gateway still owns admission and all state.
function workflowAction(session, turn, name, args) {
  session.callSequence=(session.callSequence||0)+1;
  const call={type:'function_call',turn_id:turn,call_id:'call_'+turn+'_'+name+'_'+session.callSequence,name,arguments:args};
  session.remote.required_actions.push(call);session.remote.status='requires_action';
  Object.assign(session.turns.find(value=>value.id===turn),{status:'requires_action',session_id:session.id,subagent_id:null});
  emit(session.id,{type:'agent.session.requires_action',session_id:session.id});
}
function workflowOutput(session,event) {
  const action=session.remote.required_actions.find(value=>value.call_id===event.call_id && value.turn_id===event.turn_id);
  if(!action)return;
  session.items.push({id:'tool_'+event.call_id,type:'message',role:'assistant',turn_id:event.turn_id,
    content:[{type:'output_text',text:JSON.stringify({tool:action.name,arguments:action.arguments,success:event.success,output:event.output,error:event.error})}]});
  session.remote.required_actions=session.remote.required_actions.filter(value=>value!==action);
  session.remote.status=session.remote.required_actions.length?'requires_action':'idle';
  const pending=session.remote.required_actions.some(value=>value.turn_id===event.turn_id);
  session.turns.find(value=>value.id===event.turn_id).status=pending?'requires_action':event.success?'completed':'failed';
  if(event.success && action.name==='workflow_read_result' && session.workflowFault==='FAIL') {
    workflowAction(session,event.turn_id,'workflow_delegate',{role:'review',text:'Review the failed diagnosis.',dependencies:[action.arguments.result_id]});
  }
  if(event.success && action.name==='workflow_read_result' && session.researchCoordinator) {
    const role=Object.keys(session.researchResults).find(key=>session.researchResults[key]===action.arguments.result_id);
    session.researchRead[role]=event.output;
    if(session.researchRead.argument && session.researchRead.experiment && !session.reviewDelegated) {
      session.reviewDelegated=true;
      const ids=[session.researchResults.argument,session.researchResults.experiment];
      workflowAction(session,event.turn_id,'workflow_delegate',{role:'review',text:'RESEARCH_REVIEW '+JSON.stringify(ids),dependencies:ids});
    }
    if(role==='review' && !session.synthesisSubmitted) {
      session.synthesisSubmitted=true;
      workflowAction(session,event.turn_id,'workflow_result',{outcome:'success',outputs:[{name:'synthesis',
        content:JSON.stringify({claim:'sum of first n odd integers = n squared',bounded_n:1000,
          worker_results:session.researchResults,conclusion:'Bounded computations and supplied argument reviewed; controlled fixture only.'}),
        limitations:'Controlled inference and finite computations do not prove arbitrary scientific claims.'}],
        references:['argument','experiment','review'].map(key=>session.researchResults[key])});
    }
  }
  if(event.success && action.name==='workflow_read_result' && session.researchRole==='review') {
    session.researchRead[action.arguments.result_id]=event.output;
    if(session.researchReferences.every(id=>id in session.researchRead) && !session.researchCommandQueued) {
      session.researchCommandQueued=true;researchJob(session,event.turn_id);
    }
  }
  if(!session.remote.required_actions.length && session.remote.status!=='in_progress')emit(session.id,{type:'agent.session.completed',session_id:session.id});
}
function researchJob(session,turn) {
  const input={role:session.researchRole,references:session.researchReferences||[],evidence:session.researchRead||{}};
  const program=`const fs=require('node:fs'),os=require('node:os');const input=${JSON.stringify(input)};
const max=1000;let verified=true;const samples=[];
for(let n=0;n<=max;n++){let value;
 if(input.role==='argument'){value=n===0?0n:BigInt(n)*BigInt(n)-BigInt(n-1)*BigInt(n-1);if(n>0 && value!==2n*BigInt(n)-1n)verified=false;}
 else {value=0n;for(let k=1;k<=n;k++)value+=2n*BigInt(k)-1n;if(value!==BigInt(n)*BigInt(n))verified=false;}
 if([0,1,2,10,1000].includes(n))samples.push({n,value:String(value)});
}
if(input.role==='review')for(const id of input.references){const raw=JSON.parse(input.evidence[id]);const report=JSON.parse(raw.outputs[0].content);if(!report.verified||report.max_n!==max||report.case_count!==max+1)verified=false;for(const sample of report.samples){const n=BigInt(sample.n),expected=report.role==='argument'?(n===0n?0n:2n*n-1n):n*n;if(sample.value!==String(expected))verified=false;}}
const report={role:input.role,hostname:os.hostname(),cwd:process.cwd(),claim:'sum of first n odd integers = n squared',max_n:max,case_count:max+1,verified,samples,references:input.references};
fs.writeFileSync('research-report.json',JSON.stringify(report));console.log(JSON.stringify(report));if(!verified)process.exitCode=1;`;
  session.turns.find(value=>value.id===turn).status='in_progress';session.remote.status='in_progress';
  executorJobs.set(session.remote.environment.id+':'+turn,{id:session.remote.environment.id+':'+turn,
    environment_id:session.remote.environment.id,session_id:session.id,turn_id:turn,
    argv:['node','-e',program],timeout_ms:15000,claimed:false,started:false});
}
function message(session, text) {
  const turn = 'turn_' + (session.turns.length + 1);
  session.items.push({id: 'user_' + turn, type: 'message', role: 'user', turn_id: turn,
    content: [{type: 'input_text', text}]});
  messageCommits++;
  const workflowFault=text.match(/^WORKFLOW_FAULT_(ACK|PENDING|LONG|STOP|OBSERVE|MISSING|FAIL)\b/);
  if(workflowFault) {
    session.workflowFault=workflowFault[1];
    session.turns.push({id:turn,status:'in_progress',session_id:session.id,subagent_id:null});
    if(session.workflowFault==='PENDING')Object.assign(control,{sessionGetStatus:503,sessionFaultID:session.id});
    const text=['LONG','STOP'].includes(session.workflowFault)?'SANDBOX_LONG_COMMAND WORKFLOW_DIAGNOSIS':
      session.workflowFault==='FAIL'?'WORKFLOW_DIAGNOSIS_FAIL':'WORKFLOW_DIAGNOSIS: inspect actual repository';
    workflowAction(session,turn,'workflow_delegate',{role:'diagnose',text,dependencies:[]});return;
  }
  if(text.includes('RESEARCH_PILOT')) {
    session.researchCoordinator=true;session.researchResults={};session.researchRead={};
    session.turns.push({id:turn,status:'in_progress',session_id:session.id,subagent_id:null});
    for(const role of ['argument','experiment'])workflowAction(session,turn,'workflow_delegate',{
      role,text:'RESEARCH_'+role.toUpperCase()+' supplied telescoping argument: 2k-1=k²-(k-1)²; compute n=0..1000.',dependencies:[]});
    return;
  }
  const research=text.match(/^RESEARCH_(ARGUMENT|EXPERIMENT|REVIEW)\b/);
  if(research) {
    session.researchRole=research[1].toLowerCase();session.researchRead={};
    session.turns.push({id:turn,status:'in_progress',session_id:session.id,subagent_id:null});
    if(session.researchRole==='review') {
      session.researchReferences=JSON.parse(text.split('\n',1)[0].slice('RESEARCH_REVIEW '.length));
      for(const id of session.researchReferences)workflowAction(session,turn,'workflow_read_result',{result_id:id});
    } else researchJob(session,turn);
    return;
  }
  if(text.includes('WORKFLOW_FIRST_DELEGATION')) {
    session.turns.push({id:turn,status:'in_progress',session_id:session.id,subagent_id:null});
    workflowAction(session,turn,'workflow_delegate',{role:'diagnose',text:'WORKFLOW_DIAGNOSIS: inspect the actual repository and run its test.',dependencies:[]});
    return;
  }
  const savedResult=text.match(/^Saved workflow result (\S+) from assignment \S+ \(([^)]+)\)/);
  if(savedResult) {
    if(session.researchCoordinator)session.researchResults[savedResult[2]]=savedResult[1];
    session.turns.push({id:turn,status:'in_progress',session_id:session.id,subagent_id:null});
    workflowAction(session,turn,'workflow_read_result',{result_id:savedResult[1]});
    return;
  }
  if(text.includes('WORKFLOW_DIAGNOSIS'))session.workflowMember=true;
  if(text.includes('WORKFLOW_DIAGNOSIS_FAIL'))session.workflowFail=true;
  if (session.remote.environment.type === 'self_hosted') {
    session.turns.push({id: turn, status: 'in_progress',...(session.workflowMember?{session_id:session.id,subagent_id:null}:{})});
    session.remote.status = 'in_progress';
    const interrupted=text.startsWith('SANDBOX_LONG_COMMAND');
    const program = interrupted ? `const fs=require('node:fs');fs.writeFileSync('interrupted-started.json',JSON.stringify({pid:process.pid,stage:'running',job:process.argv[1]}));setInterval(()=>{},1000);` : `const fs=require('node:fs'),cp=require('node:child_process'),os=require('node:os');
const nonce=fs.existsSync('proof.txt')?fs.readFileSync('proof.txt','utf8'):'';
const baseline=cp.spawnSync('node',['--test','checkout.test.js'],{encoding:'utf8'});
const value={nonce,hostname:os.hostname(),cwd:process.cwd(),test_exit:baseline.status,output:baseline.stdout+baseline.stderr};
fs.writeFileSync('fixture-report.json',JSON.stringify(value));console.log(JSON.stringify(value));`;
    const actualProgram=program+(session.workflowFail?'\nprocess.exitCode=1;':'');
    executorJobs.set(session.remote.environment.id + ':' + turn, {id:session.remote.environment.id + ':' + turn,
      environment_id:session.remote.environment.id,session_id:session.id,turn_id:turn,
      argv:['node','-e',actualProgram,session.remote.environment.id+':'+turn],timeout_ms:interrupted?(session.workflowMember?120000:30000):15000,claimed:false,started:false});
    return;
  }
  session.items.push({id: 'assistant_' + turn, type: 'message', role: 'assistant', turn_id: turn,
    content: [{type: 'output_text', text: 'Fixture evidence observed; no external write performed'}]});
  session.turns.push({id: turn, status: 'completed'});
  emit(session.id,{type:'agent.session.completed',session_id:session.id});
}
http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://fixture');
  let body = {};
  if (req.method === 'POST') {
    let raw = '';
    for await (const chunk of req) raw += chunk;
    try { body = JSON.parse(raw || '{}'); } catch { return reply(res, 400, {}); }
  }
  if (url.pathname === '/executor/connect' || url.pathname === '/executor/disconnect') {
    const runtime = runtimes.get(body.environment_id);
    if (!runtime) return reply(res, 404, {});
    const status = url.pathname.endsWith('disconnect') ? 'disconnected' : 'connected';
    const changed = runtime.status !== status || runtime.instance !== body.instance;
    if(status==='connected' && runtime.instance && runtime.instance!==body.instance) {
      for(const job of [...executorJobs.values()]) if(job.environment_id===runtime.id && job.started && job.instance!==body.instance) finishJob(job,{},true);
    }
    runtime.status = status; runtime.instance = body.instance;
    if(status==='connected')runtime.heartbeatCount=(runtime.heartbeatCount||0)+1;
    runtime.connectedAt = Date.now(); runtime.hostname = body.hostname;
    if (changed) for (const session of sessions.values()) if (session.remote.environment.id === runtime.id) emit(session.id,environmentEvent(runtime,session.id));
    return reply(res, 200, {ok:true});
  }
  if (url.pathname === '/executor/commands') {
    const jobs = control.executorHold ? [] : [...executorJobs.values()].filter(job => job.environment_id === url.searchParams.get('environment_id') && !job.claimed);
    for (const job of jobs) job.claimed=true;
    return reply(res, 200, jobs.map(job => ({id:job.id,argv:job.argv,timeout_ms:job.timeout_ms})));
  }
  if (url.pathname === '/executor/started') {
    const job=executorJobs.get(body.id);
    if(!job||job.environment_id!==body.environment_id)return reply(res,404,{});
    if(!job.started) {
      job.started=true;job.instance=body.instance;executorStarts++;
      const session=sessions.get(job.session_id);
      session.items.push({id:'command_'+job.turn_id,type:'command_execution',turn_id:job.turn_id,
        command:job.argv.join(' '),cwd:'/workspace',status:'in_progress',output:''});
    }
    return reply(res,200,{ok:true});
  }
  if (url.pathname === '/executor/result') {
    const job = executorJobs.get(body.id);
    if (!job || job.environment_id !== body.environment_id) return reply(res,404,{});
    const session = sessions.get(job.session_id);
    if (!session) return reply(res,404,{});
    finishJob(job,body);
    return reply(res,200,{ok:true});
  }
  if (url.pathname === '/control') {
    if (body.releaseToolResponses || body.reset) {
      for(const responses of heldToolResponses.values())for(const response of responses)response.destroy();
      heldToolResponses.clear();control.heldToolIdentity=null;
    }
    if (body.reset) control = {};
    control = {...control, ...body};
    if (body.dropStreams) for (const streams of eventStreams.values()) for (const response of streams) response.destroy();
    if (body.mutateSession) {
      const session = sessions.get(body.mutateSession.id);
      if (session) Object.assign(session.remote, clone(body.mutateSession.fields));
    }
    return reply(res, 200, {ok: true});
  }
  if (url.pathname === '/counts') return reply(res, 200, {creates, deletes, eventPosts, messageCommits,
    agentCreates, vaultCreates, templateCreates, credentialCreates, credentialWrites, executorCommands, executorStarts,
    toolResultCommits:[...toolResultCommits.values()],heldToolResponses:[...heldToolResponses.keys()],
    pendingWorkflowActions:[...sessions.values()].flatMap(session=>session.remote.required_actions.filter(action=>action.name.startsWith('workflow_')).map(action=>({session_id:session.id,call_id:action.call_id,turn_id:action.turn_id,name:action.name}))),
    jobs:[...executorJobs.values()].map(job=>({id:job.id,session_id:job.session_id,turn_id:job.turn_id,started:job.started,instance:job.instance})),
    agents: [...agents.values()].map(a => ({id: a.id, name: a.name, metadata: a.metadata})),
    vaults: [...vaults.values()].map(v => ({id: v.id, metadata: v.metadata})),
    templates: [...templates.values()].map(t => ({id: t.id, name: t.name})),
    credentials: [...credentials.values()].map(c => ({id: c.id, vault_id: c.vault_id, metadata: c.metadata, type: c.auth.type})),
    sessions: [...sessions.values()].map(s => ({id: s.id, metadata: s.metadata, input: s.input, keys: [...s.keys.keys()]}))});
  const parts = url.pathname.split('/').filter(Boolean);
  // Vaults are project resources at /v1/vaults in the pinned Go SDK.
  if (parts[1] === 'vaults') parts.splice(1, 0, 'agents');
  if (parts[0] !== 'v1' || parts[1] !== 'agents') return reply(res, 404, {});
  if (parts[2] === 'environments') {
    if (parts[3] === 'templates') {
      if (parts.length === 4) {
        if (req.method !== 'POST') return reply(res, 200, page([...templates.values()]));
        const template = {...body, id: 'envtpl_fixture_' + (++templateCreates), object: 'agent.environment_template'};
        templates.set(template.id, template); return reply(res, 200, template);
      }
      const template = templates.get(parts[4]);
      if (!template) return reply(res, 404, {});
      if (req.method === 'DELETE') { templates.delete(template.id); return reply(res, 200, {id: template.id, deleted: true}); }
      if (req.method === 'POST') Object.assign(template, body);
      return reply(res, 200, template);
    }
    const runtime = runtimes.get(parts[3]);
    return reply(res, runtime ? 200 : 404, runtime || {});
  }
  if (parts.length === 2) {
    if (req.method === 'POST') {
      const id = 'agent_fixture_' + (++agentCreates);
      const agent = {...body, id, object: 'agent'};
      agents.set(id, agent); return reply(res, 200, agent);
    }
    return reply(res, 200, page([...agents.values()]));
  }
  if (!['sessions', 'vaults'].includes(parts[2])) {
    const agent = agents.get(parts[2]);
    if (!agent) return reply(res, 404, {});
    if (req.method === 'DELETE') { agents.delete(parts[2]); return reply(res, 200, {id: parts[2], deleted: true}); }
    if (req.method === 'POST') Object.assign(agent, body);
    return reply(res, 200, agent);
  }
  const objects = parts[2] === 'vaults' ? vaults : sessions;
  if (parts.length === 3) {
    if (req.method !== 'POST') return reply(res, 200, page(control.hideDiscovery && parts[2] === 'sessions' ? [] : [...objects.values()].map(s => s.remote || s)));
    if (parts[2] === 'vaults') {
      const vault = {...body, id: 'vault_fixture_' + (++vaultCreates), object: 'agent.vault', created_at: 1791532800};
      vaults.set(vault.id, vault); return reply(res, 200, vault);
    }
    creates++;
    if (control.failCreate) return reply(res, 503, {error: {message: 'fixture precommit failure'}});
    const id = 'sess_fixture_' + creates;
    const agent = {...clone(agents.get(body.agent_id) || {}), ...clone(body.agent || {})};
    const environment = clone(body.environment || {});
    if (environment.type === 'openai_hosted' || environment.type === 'self_hosted') {
      environment.id = 'env_fixture_' + creates;
      if (environment.type === 'self_hosted') environment.remote_url = 'https://provider-fixture.codex-session-operator-system.svc:8080/';
      runtimes.set(environment.id, {id: environment.id, ...(environment.type==='self_hosted'?{type:'self_hosted'}:{}),
        status:environment.type === 'self_hosted' ? 'pending' : 'connected', object: 'agent.environment'});
    }
    const remote = {id, object: 'agent.session', status: 'idle', agent,
      environment, vault_ids: body.vault_ids || [], metadata: body.metadata, spend_control:body.spend_control ? {...body.spend_control,consumed:0} : null, required_actions: []};
    const session = {id, remote, metadata: body.metadata, input: body.input, items: [], turns: [], keys: new Map()};
    sessions.set(id, session);
    if (body.input) message(session, body.input);
    if (control.loseCreate) { control.loseCreate = false; req.socket.destroy(); return; }
    return reply(res, 200, remote);
  }
  const object = objects.get(parts[3]);
  if (!object) return reply(res, 404, {});
  if (parts[2] === 'vaults') {
    if (parts[4] === 'credentials') {
      if (parts.length === 5) {
        if (req.method !== 'POST') return reply(res, 200, page([...credentials.values()].filter(c => c.vault_id === object.id)));
        const credential = {id: 'cred_fixture_' + (++credentialCreates), object: 'vault.credential',
          vault_id: object.id, name: body.name, metadata: body.metadata, auth: publicAuth(body.auth)};
        credentials.set(credential.id, credential); credentialWrites++;
        return reply(res, 200, credential);
      }
      const credential = credentials.get(parts[5]);
      if (!credential || credential.vault_id !== object.id) return reply(res, 404, {});
      if (req.method === 'DELETE') { credentials.delete(credential.id); return reply(res, 200, {id: credential.id, deleted: true}); }
      if (req.method === 'POST') {
        if (body.auth) {
          const update = publicAuth(body.auth);
          if (update.refresh && credential.auth.refresh) {
            update.refresh = {...credential.auth.refresh, ...update.refresh};
          }
          credential.auth = {...credential.auth, ...update};
          credentialWrites++;
        }
        if ('name' in body) credential.name = body.name;
        if (body.metadata) credential.metadata = body.metadata;
      }
      return reply(res, 200, credential);
    }
    if (req.method === 'DELETE') { objects.delete(parts[3]); return reply(res, 200, {id: parts[3], deleted: true}); }
    if (req.method === 'POST') Object.assign(object, body);
    return reply(res, 200, object);
  }
  if (parts.length === 4) {
    if (req.method === 'GET' && control.sessionGetStatus && (!control.sessionFaultID || control.sessionFaultID === object.id)) {
      return reply(res, control.sessionGetStatus, {error: {message: 'controlled observation fault'}});
    }
    if (req.method === 'DELETE') {
      if (control.failDelete) return reply(res, 503, {error: {message: 'fixture cleanup failure'}});
      deletes++; objects.delete(parts[3]); return reply(res, 200, {id: parts[3], deleted: true});
    }
    if (req.method === 'POST') {
      if (body.agent) Object.assign(object.remote.agent, body.agent);
      if (body.metadata) object.remote.metadata = object.metadata = body.metadata;
      if ('spend_control' in body) object.remote.spend_control = body.spend_control;
    }
    return reply(res, 200, object.remote);
  }
  if (parts[4] === 'items') return reply(res, 200, page(object.items));
  if (parts[4] === 'turns') return reply(res, 200, page(object.turns));
  if (parts[4] === 'events' && req.method === 'GET') {
    res.writeHead(200, {'Content-Type': 'text/event-stream'}); res.flushHeaders();
    if (!eventStreams.has(object.id)) eventStreams.set(object.id,new Set());
    eventStreams.get(object.id).add(res);
    const runtime=runtimes.get(object.remote.environment.id);
    res.write('data: '+JSON.stringify(runtime && runtime.type==='self_hosted' ? environmentEvent(runtime,object.id) :
      {type:'agent.session.completed',session_id:object.id})+'\n\n');
    const timer=setInterval(()=>res.write(': fixture stream keepalive\n\n'),1000);
    req.on('close',()=>{clearInterval(timer);eventStreams.get(object.id)?.delete(res);});return;
  }
  if (parts[4] === 'events' && req.method === 'POST') {
    eventPosts++;
    const key = req.headers['idempotency-key'];
    const previous = object.keys.get(key);
    if (previous && previous !== JSON.stringify(body)) return reply(res, 409, {});
    if (!previous) {
      object.keys.set(key, JSON.stringify(body));
      for (const event of body.events || []) {
        if (event.type === 'agent.session.input.message') message(object, event.input[0].content[0].text);
        if (event.type === 'agent.session.input.tool_result') {
          const action=object.remote.required_actions.find(value=>value.call_id===event.call_id);
          const identity=object.id+':'+event.call_id;
          toolResultCommits.set(identity,{session_id:object.id,call_id:event.call_id,turn_id:event.turn_id,name:action?.name||'',accepted:(toolResultCommits.get(identity)?.accepted||0)+1});
          workflowOutput(object,event);
        }
      }
    }
    const toolEvent=(body.events||[]).find(event=>event.type==='agent.session.input.tool_result');
    const committed=toolEvent && toolResultCommits.get(object.id+':'+toolEvent.call_id);
    if(committed && ((!previous && control.holdToolResultName===committed.name) || control.heldToolIdentity===object.id+':'+toolEvent.call_id)) {
      control.holdToolResultName=null;control.heldToolIdentity=object.id+':'+toolEvent.call_id;
      if(!heldToolResponses.has(control.heldToolIdentity))heldToolResponses.set(control.heldToolIdentity,new Set());
      heldToolResponses.get(control.heldToolIdentity).add(res);return;
    }
    if (control.loseInput) { control.loseInput = false; req.socket.destroy(); return; }
    res.writeHead(204); res.end(); return;
  }
  reply(res, 404, {});
}).listen(Number(process.env.PORT || 8080), '0.0.0.0');
setInterval(()=>{
  for(const runtime of runtimes.values()) if(runtime.type==='self_hosted' && runtime.status==='connected' && Date.now()-runtime.connectedAt>5000) {
    runtime.status='disconnected';
    for(const session of sessions.values())if(session.remote.environment.id===runtime.id)emit(session.id,environmentEvent(runtime,session.id));
  }
},500);
