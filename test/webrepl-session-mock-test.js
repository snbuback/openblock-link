/* eslint-disable no-console */
const {Server} = require('ws');
const WebSocket = require('ws');
const OpenBlockLink = require('../src/index');

const WEBREPL_PORT = 18267;
const LINK_PORT = 20113;

const readHeader = buffer => ({
    op: buffer.readUInt8(2),
    size: buffer.readUInt32LE(12),
    name: buffer.slice(18, 18 + buffer.readUInt16LE(16)).toString('utf8')
});

const webreplServer = new Server({port: WEBREPL_PORT});
let uploaded = null;
let booted = false;

webreplServer.on('connection', socket => {
    let loggedIn = false;
    let headerPart = null;
    let putInfo = null;
    let putPayload = null;

    socket.send('Password: ');
    socket.on('message', data => {
        if (!loggedIn) {
            loggedIn = true;
            socket.send('\r\nWebREPL connected\r\n>>> ');
            return;
        }
        if (typeof data === 'string') {
            if (data === '\x04') {
                booted = true;
                socket.send('BOOT OUTPUT\r\n');
                return;
            }
            if (data.indexOf('\x01') !== -1) {
                socket.send('\r\nraw REPL; CTRL-B to exit\r\n>');
                return;
            }
            if (data.endsWith('\x04')) {
                const command = data.slice(0, -1);
                const output = command.indexOf('print') === 0 ? 'hello\r\n' : '';
                socket.send(`OK${output}\x04\x04>`);
            }
            return;
        }
        if (putPayload) {
            putPayload = Buffer.concat([putPayload, data]);
            if (putPayload.length >= putInfo.size) {
                uploaded = {name: putInfo.name, content: putPayload.toString('utf8')};
                socket.send(Buffer.from([0x57, 0x42, 0, 0]));
                putInfo = null;
                putPayload = null;
            }
            return;
        }
        if (headerPart) {
            const header = Buffer.concat([headerPart, data]);
            headerPart = null;
            const info = readHeader(header);
            if (info.op === 3) {
                socket.send(Buffer.from([1, 19, 1]));
                return;
            }
            putInfo = info;
            putPayload = Buffer.alloc(0);
            socket.send(Buffer.from([0x57, 0x42, 0, 0]));
            return;
        }
        if (data.length === 10) {
            headerPart = data;
        } else if (data.length === 82 && readHeader(data).op === 3) {
            socket.send(Buffer.from([1, 19, 1]));
        }
    });
});

const link = new OpenBlockLink('/tmp/openblock-link-test-data', __dirname);
link.listen(LINK_PORT, '127.0.0.1');

const rpc = (socket, method, params, id) => new Promise((resolve, reject) => {
    const request = {jsonrpc: '2.0', id: id, method: method, params: params};
    const handler = data => {
        const json = JSON.parse(data);
        if (json.id === id) {
            socket.removeListener('message', handler);
            if (json.error) {
                reject(json.error);
            } else {
                resolve(json.result);
            }
        }
    };
    socket.on('message', handler);
    socket.send(JSON.stringify(request));
});

const run = async () => {
    const socket = new WebSocket(`ws://127.0.0.1:${LINK_PORT}/openblock/webrepl`);
    await new Promise((resolve, reject) => {
        socket.on('open', resolve);
        socket.on('error', reject);
    });
    await rpc(socket, 'discover', {filters: {pnpid: []}}, 1);
    await rpc(socket, 'connect', {
        peripheralId: `127.0.0.1:${WEBREPL_PORT}`,
        peripheralConfig: {password: 'secret'}
    }, 2);
    await rpc(socket, 'read', null, 3);
    const code = Buffer.from('print("from session")\n').toString('base64');
    await rpc(socket, 'upload', {message: code, config: {type: 'microPython'}, encoding: 'base64'}, 4);
    const output = await rpc(socket, 'execLive', {command: 'print(1)'}, 5);
    console.log('uploaded', uploaded && uploaded.name, JSON.stringify(uploaded && uploaded.content));
    console.log('booted', booted);
    console.log('live output', JSON.stringify(output));
    socket.close();
    link._httpServer.close();
    webreplServer.close();
};

run().catch(err => {
    console.error(err);
    process.exit(1);
});
