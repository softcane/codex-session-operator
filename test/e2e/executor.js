#!/usr/bin/env node
// Deterministic executor fixture: actual Pod commands/files, no model or OpenAI calls.
'use strict';
const {spawn} = require('node:child_process');
const os = require('node:os');
const {randomUUID} = require('node:crypto');
const args = process.argv.slice(2);
if (args.includes('--help') || args.includes('--version')) {
  console.log('simulated exec-server fixture; no production execution proof');
  process.exit(0);
}
if (args[0] !== 'exec-server' || process.env.CODEX_API_KEY !== 'fixture-executor-key-not-real') {
  console.error('Fixture executor requires its isolated test credential');
  process.exit(1);
}
const remote = new URL(args[args.indexOf('--remote') + 1]);
if (remote.protocol !== 'https:' || !remote.hostname.startsWith('provider-fixture.')) {
  console.error('Fixture executor requires the owned provider service');
  process.exit(1);
}
remote.protocol = 'http:'; // The isolated fixture API intentionally uses local HTTP.
const environment = args[args.indexOf('--environment-id') + 1];
const instance = randomUUID();
let stopped = false;
let heartbeat;
const stop=()=>{stopped=true;clearInterval(heartbeat);};
process.on('SIGTERM', stop);
process.on('SIGINT', stop);
const delay = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));
async function post(path, body) {
  const response = await fetch(new URL(path, remote), {method:'POST',
    headers:{'Content-Type':'application/json'},body:JSON.stringify(body),signal:AbortSignal.timeout(3000)});
  if (!response.ok) throw new Error('fixture transport unavailable');
  return response.json();
}
async function run(job) {
  await post('/executor/started',{id:job.id,environment_id:environment,instance});
  // Command recipes originate only from the maintained isolated provider fixture.
  const allowed=['PATH','HOME','GOCACHE','GOMODCACHE','GOPATH','GOTOOLCHAIN','CGO_ENABLED'];
  const childEnv=Object.fromEntries(allowed.filter(name=>process.env[name]!==undefined).map(name=>[name,process.env[name]]));
  const child = spawn(job.argv[0], job.argv.slice(1), {cwd:process.cwd(), env:childEnv});
  let output = '';
  for (const stream of [child.stdout, child.stderr]) stream.on('data', bytes => {output += bytes.toString();});
  const timer = setTimeout(() => child.kill('SIGKILL'), Math.max(1000,Math.min(job.timeout_ms||15000,30000)));
  const exitCode = await new Promise(resolve => {
    child.on('error', () => resolve(127)); child.on('close', code => resolve(code === null ? 137 : code));
  });
  clearTimeout(timer);
  await post('/executor/result', {environment_id:environment,id:job.id,exit_code:exitCode,output,cwd:process.cwd()});
}
(async () => {
  heartbeat=setInterval(()=>post('/executor/connect',{environment_id:environment,hostname:os.hostname(),instance}).catch(()=>{}),300);
  while (!stopped) {
    try {
      await post('/executor/connect', {environment_id:environment,hostname:os.hostname(),instance});
      const response = await fetch(new URL('/executor/commands?environment_id='+encodeURIComponent(environment),remote),
        {signal:AbortSignal.timeout(3000)});
      const jobs = await response.json();
      for (const job of jobs) await run(job);
    } catch { console.error('Fixture transport retry'); }
    await delay(300);
  }
  await post('/executor/disconnect',{environment_id:environment}).catch(() => {});
})().catch(() => {console.error('Fixture executor stopped');process.exitCode=1;});
