// Controlled Agents API contract fixture. No inference, external writes or real keys.
const http = require('http');
const agents = new Map(), vaults = new Map(), sessions = new Map();
const templates = new Map(), runtimes = new Map(), credentials = new Map();
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
function message(session, text) {
  const turn = 'turn_' + (session.turns.length + 1);
  session.items.push({id: 'user_' + turn, type: 'message', role: 'user', turn_id: turn,
    content: [{type: 'input_text', text}]});
  session.items.push({id: 'assistant_' + turn, type: 'message', role: 'assistant', turn_id: turn,
    content: [{type: 'output_text', text: 'Fixture evidence observed; no external write performed'}]});
  session.turns.push({id: turn, status: 'completed'});
  messageCommits++;
}
http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://fixture');
  let body = {};
  if (req.method === 'POST') {
    let raw = '';
    for await (const chunk of req) raw += chunk;
    try { body = JSON.parse(raw || '{}'); } catch { return reply(res, 400, {}); }
  }
  if (url.pathname === '/control') {
    if (body.reset) control = {};
    control = {...control, ...body};
    if (body.mutateSession) {
      const session = sessions.get(body.mutateSession.id);
      if (session) Object.assign(session.remote, clone(body.mutateSession.fields));
    }
    return reply(res, 200, {ok: true});
  }
  if (url.pathname === '/counts') return reply(res, 200, {creates, deletes, eventPosts, messageCommits,
    agentCreates, vaultCreates, templateCreates, credentialCreates, credentialWrites,
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
    if (environment.type === 'openai_hosted') {
      environment.id = 'env_fixture_' + creates;
      runtimes.set(environment.id, {id: environment.id, status: 'connected', object: 'agent.environment'});
    }
    const remote = {id, object: 'agent.session', status: 'idle', agent,
      environment, vault_ids: body.vault_ids || [], metadata: body.metadata, required_actions: []};
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
    const timer = setInterval(() => res.write('data: '+JSON.stringify({type: 'agent.session.completed', session_id: object.id})+'\n\n'), 1000);
    req.on('close', () => clearInterval(timer)); return;
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
      }
    }
    if (control.loseInput) { control.loseInput = false; req.socket.destroy(); return; }
    res.writeHead(204); res.end(); return;
  }
  reply(res, 404, {});
}).listen(Number(process.env.PORT || 8080), '0.0.0.0');
