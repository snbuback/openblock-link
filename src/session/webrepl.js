const fs = require('fs');
const path = require('path');
const ansi = require('ansi-string');

const Session = require('./session');
const WebRepl = require('../lib/webrepl');

const RECONNECT_ATTEMPTS = 5;
const RECONNECT_DELAY = 1000;
const POST_RESET_DELAY = 1500;

const wait = ms => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Session handling a MicroPython WebREPL connection.
 *
 * The JSON-RPC surface mirrors SerialportSession so the renderer can drive
 * this transport with the same device/IO code. SerialportSession and the
 * Arduino/micro:bit uploaders are left untouched.
 */
class WebReplSession extends Session {
    constructor (socket, userDataPath, toolsPath) {
        super(socket);

        this.userDataPath = userDataPath;
        this.toolsPath = toolsPath;

        this._type = 'webrepl';
        this._client = null;
        this._connected = false;
        this._isRead = false;
        this._isInDisconnect = false;
        this._peripheralId = null;
        this._peripheralParams = null;

        this._uploading = false;
        this._abort = false;
        this._expectedDisconnect = false;
        this._unplugReported = false;
        this._execQueue = Promise.resolve();
    }

    async didReceiveCall (method, params, completion) {
        switch (method) {
        case 'discover':
            // WebREPL has no discovery protocol; the user enters the address.
            completion(null, null);
            break;
        case 'connect':
            await this.connect(params);
            completion(null, null);
            break;
        case 'disconnect':
            await this.disconnect();
            completion(null, null);
            break;
        case 'updateBaudrate':
            // There is no serial line to reconfigure.
            completion(null, null);
            break;
        case 'write':
            completion(await this.write(params), null);
            break;
        case 'read':
            this._isRead = true;
            completion(null, null);
            break;
        case 'upload':
            completion(await this.upload(params), null);
            break;
        case 'uploadFirmware':
            this.sendRemoteRequest('uploadError', {
                message: `${ansi.red}Firmware update over WebREPL is not supported. Please connect over USB.\n`
            });
            completion(null, null);
            break;
        case 'abortUpload':
            this.abortUpload();
            completion(null, null);
            break;
        case 'execLive':
            completion(await this.execLive(params), null);
            break;
        case 'pingMe':
            completion('willPing', null);
            this.sendRemoteRequest('ping', null, result => {
                console.log(`Got result from ping: ${result}`);
            });
            break;
        default:
            throw new Error('Method not found');
        }
    }

    /**
     * Connect to a WebREPL endpoint.
     * @param {object} params - peripheralId ("host:port") and peripheralConfig.
     * @returns {Promise} - resolved once logged in and version-checked.
     */
    async connect (params) {
        if (this._client && this._connected) {
            throw new Error('already connected to peripheral');
        }

        const {peripheralId, peripheralConfig} = params;
        const {host, port} = WebRepl.parseHostPort(peripheralId);
        const password = this._readPassword(peripheralConfig);

        const client = new WebRepl({host, port, password});
        this._bindClient(client);
        this._client = client;
        this._peripheralId = peripheralId;
        this._peripheralParams = params;
        this._expectedDisconnect = false;
        this._unplugReported = false;

        try {
            await client.connect();
            const version = await client.getVersion();
            this._connected = true;
            this.sendstd(`${ansi.clear}Connected to ${host}:${port} ` +
                `(WebREPL ${version.join('.')})\n`);
        } catch (err) {
            this._connected = false;
            this._client = null;
            try {
                client.close();
            } catch (closeErr) {
                // The socket is already gone.
            }
            this.sendRemoteRequest('connectError', {message: err.message});
            // Throw the plain message so the JSON-RPC error response carries
            // the useful text instead of an empty serialized Error object.
            throw err.message;
        }
    }

    /**
     * Extract the password from either the flat or the nested config shape.
     * @param {object} peripheralConfig - connection config.
     * @returns {string} - the password.
     * @private
     */
    _readPassword (peripheralConfig) {
        if (!peripheralConfig) return '';
        if (typeof peripheralConfig.password === 'string') {
            return peripheralConfig.password;
        }
        if (peripheralConfig.config && typeof peripheralConfig.config.password === 'string') {
            return peripheralConfig.config.password;
        }
        return '';
    }

    /**
     * Wire the WebREPL client events to the JSON-RPC session.
     * @param {WebRepl} client - the client.
     * @private
     */
    _bindClient (client) {
        client.on('text', data => {
            if (this._isRead) {
                this.sendRemoteRequest('onMessage', {
                    encoding: 'base64',
                    message: data.toString('base64')
                });
            }
        });
        client.on('close', expected => {
            this._connected = false;
            if (expected || this._expectedDisconnect) return;
            this._reportUnplug();
        });
        client.on('error', () => {
            if (this._expectedDisconnect) return;
            if (this._connected) {
                this._connected = false;
                this._reportUnplug();
            }
        });
    }

    _reportUnplug () {
        if (this._unplugReported) return;
        this._unplugReported = true;
        this.sendRemoteRequest('peripheralUnplug', null);
    }

    /**
     * Write text to the friendly REPL.
     * @param {object} params - message and encoding.
     * @returns {number} - bytes written.
     */
    async write (params) {
        if (!this._client || !this._connected) {
            return 0;
        }
        const {message, encoding} = params;
        const buffer = Buffer.from(message, encoding || 'utf8');
        // WebREPL terminal frames carry UTF-8 text. Console input is always
        // text, so decoding here also handles multi-byte characters.
        await this._client.writeRepl(buffer.toString('utf8'));
        return buffer.length;
    }

    /**
     * Run a Python statement in the board's raw REPL (realtime mode).
     * @param {object} params - command, timeout and options.
     * @returns {string} - command stdout.
     */
    execLive (params) {
        if (this._uploading) {
            return Promise.reject(new Error('An upload is already running.'));
        }
        const job = async () => {
            if (!this._client || !this._connected) {
                throw new Error('No peripheral is connected.');
            }
            return this._client.execRaw(
                params.command,
                params.timeout,
                params.options || {}
            );
        };
        const result = this._execQueue.then(job, job);
        // Keep the chain alive after a failed command.
        this._execQueue = result.catch(() => null);
        return result;
    }

    /**
     * Upload MicroPython source as main.py.
     * @param {object} params - message, config and encoding.
     * @returns {Promise} - resolved when the upload finished.
     */
    async upload (params) {
        const {message, config, encoding} = params;
        const code = Buffer.from(message, encoding || 'base64').toString('utf8');

        if (!this._client || !this._connected) {
            this.sendRemoteRequest('uploadError', {
                message: `${ansi.red}No peripheral is connected.\n`
            });
            return;
        }

        this._uploading = true;
        this._abort = false;
        this.sendRemoteRequest('setUploadAbortEnabled', true);

        try {
            // Let an in-flight realtime command finish before taking over the
            // REPL channel.
            await this._execQueue;
            // Leave raw REPL (if realtime mode used it) and stop the board
            // program before replacing main.py.
            await this._client.exitRawRepl();
            await this._client.interrupt();

            const files = this._collectFiles(config);
            for (const file of files) {
                if (this._abort) throw new Error('Aborted');
                await this._putFile(file.name, file.data);
            }

            if (this._abort) throw new Error('Aborted');
            await this._putFile('main.py', Buffer.from(code, 'utf8'));

            this.sendstd(`${ansi.clear}Reset device\n`);
            this._expectedDisconnect = true;
            await this._client.softReset();
            // The file is on the board; report success immediately so the
            // progress dialog closes while the WiFi link restarts.
            this.sendstd(`${ansi.green_dark}Success\n`);
            this.sendRemoteRequest('uploadSuccess', {aborted: false});
            await this._reconnectAfterReset();
        } catch (err) {
            if (err.message === 'Aborted' || this._abort) {
                this.sendstd(`${ansi.yellow_dark}Upload aborted.\n`);
                await this._recoverAfterAbort();
                this.sendRemoteRequest('uploadSuccess', {aborted: true});
            } else {
                this.sendRemoteRequest('uploadError', {
                    message: ansi.red + err.message
                });
            }
        } finally {
            this._uploading = false;
            this._abort = false;
        }
    }

    /**
     * Build the list of files to upload. Device extension libraries are
     * copied next to main.py, matching the micro:bit behaviour.
     * @param {object} config - upload config.
     * @returns {Array.<object>} - [{name, data}].
     * @private
     */
    _collectFiles (config) {
        const result = [];
        const libraries = (config && config.library) || [];
        libraries.forEach(libraryPath => {
            if (!libraryPath || !fs.existsSync(libraryPath)) return;
            const stats = fs.statSync(libraryPath);
            if (stats.isFile()) {
                result.push({
                    name: path.basename(libraryPath),
                    data: fs.readFileSync(libraryPath)
                });
                return;
            }
            fs.readdirSync(libraryPath).forEach(fileName => {
                const filePath = path.join(libraryPath, fileName);
                if (!fs.statSync(filePath).isFile()) return;
                if (fileName === 'main.py') return;
                result.push({name: fileName, data: fs.readFileSync(filePath)});
            });
        });
        return result;
    }

    /**
     * Put one file through the WebREPL binary protocol with progress.
     * @param {string} fileName - remote name.
     * @param {Buffer} data - content.
     * @returns {Promise} - resolved when written.
     * @private
     */
    async _putFile (fileName, data) {
        this.sendstd(`Writing ${fileName}...\n`);
        let lastPercent = -1;
        await this._client.putFile(fileName, data, (sent, total) => {
            const percent = total === 0 ? 100 : Math.round((sent / total) * 100);
            if (percent !== lastPercent) {
                lastPercent = percent;
                this.sendstd(`Writing ${fileName} ${percent}%\n`);
            }
        }, () => this._abort);
    }

    /**
     * Reopen the WebREPL connection after a soft reset so boot output keeps
     * flowing to the console.
     * @returns {Promise} - resolved when reconnected or after giving up.
     * @private
     */
    async _reconnectAfterReset () {
        await wait(POST_RESET_DELAY);

        if (!this._client) return;
        if (this._client.isOpen()) {
            this._client.clearExpectedClose();
            this._expectedDisconnect = false;
            this._unplugReported = false;
            return;
        }

        for (let attempt = 0; attempt < RECONNECT_ATTEMPTS; attempt++) {
            if (!this._client) return;
            try {
                // The board just rebooted; keep the per-attempt timeout short
                // so a missing WebREPL server cannot stall the session.
                await this._client.reconnect(3000);
                this._expectedDisconnect = false;
                this._unplugReported = false;
                this._connected = true;
                this.sendstd(`${ansi.clear}Reconnected, boot output follows.\n`);
                return;
            } catch (err) {
                await wait(RECONNECT_DELAY);
            }
        }

        this._expectedDisconnect = false;
        this._connected = false;
        this._reportUnplug();
    }

    /**
     * Close the stuck file protocol after an aborted transfer and reopen.
     * @returns {Promise} - resolved when the connection is usable again.
     * @private
     */
    async _recoverAfterAbort () {
        if (!this._client) return;
        this._expectedDisconnect = true;
        try {
            this._client.close();
        } catch (err) {
            // Already closed.
        }
        await wait(500);
        if (!this._client) return;
        try {
            await this._client.reconnect(5000);
            this._expectedDisconnect = false;
            this._unplugReported = false;
            this._connected = true;
            await this._client.exitRawRepl();
        } catch (err) {
            this._expectedDisconnect = false;
            this._connected = false;
            this._reportUnplug();
        }
    }

    abortUpload () {
        this._abort = true;
        if (this._client) {
            this._client.abort();
        }
    }

    disconnect () {
        this._isInDisconnect = true;
        this._connected = false;
        this._isRead = false;
        if (this._client) {
            this._client.close();
            this._client = null;
        }
        return Promise.resolve();
    }

    sendstd (message) {
        if (this._socket) {
            this.sendRemoteRequest('uploadStdout', {
                message: message
            });
        }
    }

    dispose () {
        this.disconnect();
        super.dispose();
        this._client = null;
        this._peripheralParams = null;
        this._execQueue = Promise.resolve();
    }
}

module.exports = WebReplSession;
