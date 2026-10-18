// HTTPS smart-Git fixture with public/private repositories and no external access.
'use strict';
const https = require('node:https');
const fs = require('node:fs');
const {spawn,execFileSync} = require('node:child_process');
const root='/tmp/git';
fs.mkdirSync(root,{recursive:true});
const git=(args,cwd)=>execFileSync('git',args,{cwd,env:{...process.env,
  GIT_AUTHOR_DATE:'2020-01-01T00:00:00Z',GIT_COMMITTER_DATE:'2020-01-01T00:00:00Z'},encoding:'utf8'}).trim();
const manifest={};
for (const name of ['public','private']) {
  const work='/tmp/'+name;
  fs.mkdirSync(work,{recursive:true}); git(['init','-q'],work);
  fs.writeFileSync(work+'/checkout.js',"'use strict';\nexports.toCents = amount => Number.parseInt(amount, 10) * 100;\n");
  fs.writeFileSync(work+'/checkout.test.js',"const {test}=require('node:test');\nconst assert=require('node:assert/strict');\nconst {toCents}=require('./checkout');\ntest('fractional checkout',()=>assert.equal(toCents('10.99'),1099));\n");
  fs.writeFileSync(work+'/README.md','Controlled HTTPS Git preparation fixture.\n');
  git(['add','.'],work);git(['-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-qm','Seed immutable fixture'],work);
  manifest[name]=git(['rev-parse','HEAD'],work);git(['clone','--bare','-q',work,root+'/'+name+'.git']);
}
let held=false;
https.createServer({cert:fs.readFileSync('/tls/tls.crt'),key:fs.readFileSync('/tls/tls.key')},async(req,res)=>{
  const url=new URL(req.url,'https://fixture');
  if(url.pathname==='/manifest') {res.writeHead(200,{'Content-Type':'application/json'});res.end(JSON.stringify(manifest));return;}
  if(url.pathname==='/control' && req.method==='POST') {
    let raw='';for await(const chunk of req)raw+=chunk;
    held=Boolean(JSON.parse(raw).held);res.writeHead(200,{'Content-Type':'application/json'});res.end('{"ok":true}');return;
  }
  if(!/^\/(public|private)\.git(?:\/|$)/.test(url.pathname)) {res.writeHead(404);res.end();return;}
  if(held){res.writeHead(503);res.end('fixture preparation held');return;}
  if(url.pathname.startsWith('/private.git')) {
    const expected='Basic '+Buffer.from('x-access-token:'+process.env.REPOSITORY_TOKEN).toString('base64');
    if(req.headers.authorization!==expected){res.writeHead(401,{'WWW-Authenticate':'Basic realm="fixture"'});res.end();return;}
  }
  const backend=spawn('git',['http-backend'],{env:{...process.env,GIT_PROJECT_ROOT:root,GIT_HTTP_EXPORT_ALL:'1',
    PATH_INFO:url.pathname,QUERY_STRING:url.search.slice(1),REQUEST_METHOD:req.method,
    CONTENT_TYPE:req.headers['content-type']||'',GIT_PROTOCOL:req.headers['git-protocol']||'',REMOTE_USER:'fixture'}});
  req.pipe(backend.stdin);
  const chunks=[];backend.stdout.on('data',bytes=>chunks.push(bytes));
  backend.stderr.resume();backend.on('error',()=>{res.writeHead(503);res.end();});
  backend.on('close',()=>{
    const output=Buffer.concat(chunks),split=output.indexOf('\r\n\r\n');
    if(split<0){res.writeHead(503);res.end();return;}
    let status=200;const headers={};
    for(const line of output.subarray(0,split).toString().split('\r\n')) {
      const separator=line.indexOf(':');const name=line.slice(0,separator),value=line.slice(separator+1).trim();
      if(name.toLowerCase()==='status')status=Number(value.split(' ')[0]);else headers[name]=value;
    }
    res.writeHead(status,headers);res.end(output.subarray(split+4));
  });
}).listen(8443,'0.0.0.0');
