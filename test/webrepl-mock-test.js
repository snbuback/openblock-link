/* eslint-disable no-console */
const {Server} = require('ws');
const WebRepl = require('../src/lib/webrepl');

const readHeader = buffer => ({
    op: buffer.readUInt8(2),
    size: buffer.readUInt32LE(12),
    name: buffer.slice(18, 18 + buffer.readUInt16LE(16)).toString('utf8')
});

const server = new Server({port: 18266});

server.on('connection', socket => {
    let loggedIn = false;
    let headerPart = null;
    let putInfo = null;
    let putPayload = null;
    let pasteBuffer = null;

    socket.send('Password: ');

    socket.on('message', data => {
        if (!loggedIn) {
            loggedIn = true;
            socket.send('\r\nWebREPL connected\r\n>>> ');
            return;
        }

        if (typeof data === 'string') {
            if (pasteBuffer !== null) {
                if (data === '\x04') {
                    const command = pasteBuffer;
                    pasteBuffer = null;
                    socket.send('\x04');
                    socket.send(`RESULT:${command.length}\r\n\x04\x04>`);
                    return;
                }
                pasteBuffer += data;
                socket.send('\x01');
                return;
            }
            if (data.indexOf('\x05A\x01') !== -1) {
                pasteBuffer = '';
                socket.send('R\x01');
                socket.send(String.fromCharCode(128, 0));
                return;
            }
            if (data.indexOf('\x01') !== -1) {
                socket.send('\r\nraw REPL; CTRL-B to exit\r\n>');
                return;
            }
            if (data === '\x04') {
                socket.send('BOOT OUTPUT\r\n');
                return;
            }
            if (data.endsWith('\x04')) {
                const command = data.slice(0, -1);
                const output = command.indexOf('print') === 0 ? 'hello\r\n' : '';
                socket.send(`OK${output}\x04\x04>`);
            }
            return;
        }

        // Binary protocol frames.
        if (putPayload) {
            putPayload = Buffer.concat([putPayload, data]);
            if (putPayload.length >= putInfo.size) {
                console.log(`mock wrote ${putInfo.name} (${putPayload.length} bytes)`);
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
            return;
        }
        if (data.length === 82) {
            const info = readHeader(data);
            if (info.op === 3) {
                socket.send(Buffer.from([1, 19, 1]));
            }
        }
    });
});

const run = async () => {
    const client = new WebRepl({host: '127.0.0.1', port: 18266, password: 'secret'});
    client.on('text', data => process.stdout.write(data));
    await client.connect();
    const version = await client.getVersion();
    console.log('version', version);
    await client.putFile('main.py', Buffer.from('print("hi")\n'));
    console.log('put ok');
    const output = await client.execRaw('print(123)');
    console.log('exec', JSON.stringify(output));
    const longCommand = `print('${'x'.repeat(300)}')`;
    const longOutput = await client.execRaw(longCommand);
    console.log('long exec length', longOutput.length);
    await client.softReset();
    client.close();
    server.close();
};

run().catch(err => {
    console.error(err);
    process.exit(1);
});
