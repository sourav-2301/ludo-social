// Ludo Social room server: static single-file client + dependency-free WebSocket sync.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const ROOT = __dirname, rooms = new Map();
const COLORS = ['red', 'green', 'yellow', 'blue'];
const START = { red: 0, green: 13, yellow: 26, blue: 39 };
const SAFE = new Set([0, 8, 13, 21, 26, 34, 39, 47]);
const freshTokens = () => Object.fromEntries(COLORS.map(c => [c, [-1, -1, -1, -1]]));
const roomCode = () => crypto.randomBytes(3).toString('hex').toUpperCase();
const snapshot = r => ({ code:r.code, host:r.host, started:r.started, turn:r.turn, dice:r.dice, winner:r.winner, players:r.players.map(({id,name,color})=>({id,name,color})), tokens:r.tokens });
function send(ws, obj) { if (!ws.destroyed && ws.writable) ws.write(frame(JSON.stringify(obj))); }
function broadcast(r) { const message={type:'state',room:snapshot(r)};for(const ws of r.clients)send(ws,message); }
function fail(ws,message) { send(ws,{type:'error',message}); }
function frame(text) { const data=Buffer.from(text),len=data.length;let head;if(len<126){head=Buffer.from([0x81,len])}else if(len<65536){head=Buffer.alloc(4);head[0]=0x81;head[1]=126;head.writeUInt16BE(len,2)}else{head=Buffer.alloc(10);head[0]=0x81;head[1]=127;head.writeBigUInt64BE(BigInt(len),2)}return Buffer.concat([head,data]); }
function possible(r,p,i){const n=r.tokens[p.color][i];return n!==57&&(n<0?r.dice===6:n+r.dice<=57)}
function moveTurn(r,extra=false){r.dice=null;if(!extra)r.turn=(r.turn+1)%r.players.length}
function action(r,p,a){if(!r.started||r.winner)throw Error('This game is not active.');if(r.players[r.turn]?.id!==p.id)throw Error('It is not your turn.');
  if(a.type==='roll'){if(r.dice!==null)throw Error('Move a token first.');r.dice=crypto.randomInt(1,7);if(!r.tokens[p.color].some((_,i)=>possible(r,p,i)))moveTurn(r);return}
  if(a.type!=='move')throw Error('Unknown move.');if(r.dice===null)throw Error('Roll the dice first.');const i=Number(a.token);if(!Number.isInteger(i)||i<0||i>3||!possible(r,p,i))throw Error('That token cannot move.');const old=r.tokens[p.color][i],next=old<0?0:old+r.dice;r.tokens[p.color][i]=next;
  if(next<52){const landing=(START[p.color]+next)%52;if(!SAFE.has(landing))for(const rival of r.players)if(rival.color!==p.color)r.tokens[rival.color]=r.tokens[rival.color].map(n=>n>=0&&n<52&&(START[rival.color]+n)%52===landing?-1:n)}
  const six=r.dice===6;if(r.tokens[p.color].every(n=>n===57))r.winner=p.id;moveTurn(r,six&&!r.winner);
}

const server=http.createServer((req,res)=>{
  const pathname=new URL(req.url,'http://localhost').pathname;
  const file=pathname==='/'||/^\/[A-Z0-9]{6}$/i.test(pathname)?'index.html':decodeURIComponent(pathname.slice(1));
  const full=path.resolve(ROOT,file);if(!full.startsWith(ROOT+path.sep)){res.writeHead(403);return res.end('Forbidden')}
  fs.readFile(full,(err,data)=>{if(err){res.writeHead(404,{'Content-Type':'text/plain'});return res.end('Not found')}const type=path.extname(full)==='.html'?'text/html; charset=utf-8':path.extname(full)==='.css'?'text/css':path.extname(full)==='.js'?'text/javascript':'application/octet-stream';res.writeHead(200,{'Content-Type':type,'Cache-Control':'no-cache','X-Content-Type-Options':'nosniff'});res.end(data)});
});

server.on('upgrade',(req,socket)=>{
  const key=req.headers['sec-websocket-key'];if(!key||req.headers.upgrade?.toLowerCase()!=='websocket'){socket.destroy();return}
  const accept=crypto.createHash('sha1').update(key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n');
  socket.clientId=null;socket.roomCode=null;socket._buffer=Buffer.alloc(0);
  socket.on('data',chunk=>{socket._buffer=Buffer.concat([socket._buffer,chunk]);while(socket._buffer.length>=2){const b0=socket._buffer[0],b1=socket._buffer[1];let len=b1&127,offset=2;if(len===126){if(socket._buffer.length<4)return;len=socket._buffer.readUInt16BE(2);offset=4}else if(len===127){if(socket._buffer.length<10)return;const big=socket._buffer.readBigUInt64BE(2);if(big>1_000_000n){socket.destroy();return}len=Number(big);offset=10}const masked=!!(b1&128);if(masked)offset+=4;if(socket._buffer.length<offset+len)return;let data=socket._buffer.subarray(offset,offset+len);if(masked){const mask=socket._buffer.subarray(offset-4,offset);data=Buffer.from(data);for(let i=0;i<data.length;i++)data[i]^=mask[i%4]}socket._buffer=socket._buffer.subarray(offset+len);const opcode=b0&15;if(opcode===8){socket.end(Buffer.from([0x88,0]));return}if(opcode===9){socket.write(Buffer.from([0x8a,0]));continue}if(opcode!==1)continue;try{handle(socket,JSON.parse(data.toString('utf8')))}catch(e){fail(socket,'Could not read that request.')}}});
  socket.on('close',()=>detach(socket));socket.on('end',()=>detach(socket));socket.on('error',()=>detach(socket));
});
function attach(ws,r,p){if(ws.roomCode&&ws.roomCode!==r.code)detach(ws);ws.roomCode=r.code;ws.clientId=p.id;r.clients.add(ws)}
function detach(ws){if(ws.roomCode){rooms.get(ws.roomCode)?.clients.delete(ws);ws.roomCode=null}}
function handle(ws,m){if(!m||typeof m.type!=='string')return;
  if(m.type==='create'){
    if(typeof m.playerId!=='string'||m.playerId.length>80)return fail(ws,'Refresh the page to create a room.');let code=roomCode();while(rooms.has(code))code=roomCode();const p={id:m.playerId,name:String(m.name||'Player').trim().slice(0,18)||'Player',color:COLORS[0]};const r={code,host:p.id,players:[p],started:false,turn:0,dice:null,winner:null,tokens:freshTokens(),clients:new Set()};rooms.set(code,r);attach(ws,r,p);return broadcast(r)
  }
  if(m.type==='join'||m.type==='resume'){
    const code=String(m.code||'').toUpperCase(),r=rooms.get(code);if(!r)return fail(ws,'Room not found. Check the room code and try again.');let p=r.players.find(x=>x.id===m.playerId);
    if(!p){if(m.type==='resume')return fail(ws,'Your seat is no longer available. Rejoin with a new name.');if(r.started)return fail(ws,'This game has already started.');if(r.players.length>=4)return fail(ws,'This room is full.');if(typeof m.playerId!=='string'||m.playerId.length>80)return fail(ws,'Refresh the page and try again.');p={id:m.playerId,name:String(m.name||'Player').trim().slice(0,18)||'Player',color:COLORS[r.players.length]};r.players.push(p)}
    attach(ws,r,p);return broadcast(r)
  }
  const r=rooms.get(String(m.code||'').toUpperCase());if(!r)return fail(ws,'Room not found.');const p=r.players.find(x=>x.id===m.playerId);if(!p)return fail(ws,'Join this room before playing.');if(ws.roomCode!==r.code||ws.clientId!==p.id)return fail(ws,'Reconnect to this room before playing.');
  if(m.type==='start'){if(p.id!==r.host)return fail(ws,'Only the host can start the game.');if(r.players.length<2)return fail(ws,'Invite at least one friend to start.');if(!r.started){r.started=true;r.turn=0}return broadcast(r)}
  if(m.type==='action'){try{action(r,p,m.action||{});return broadcast(r)}catch(e){return fail(ws,e.message)}}
}
const port=Number(process.env.PORT)||3000;server.listen(port,'0.0.0.0',()=>console.log(`Ludo Social listening on http://localhost:${port}`));
