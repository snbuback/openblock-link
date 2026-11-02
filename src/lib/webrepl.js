const EventEmitter = require('events');
const WebSocket = require('ws');

const WEBREPL_DEFAULT_PORT = 8266;
const WEBREPL_CONNECT_TIMEOUT = 10000;
const WEBREPL_LOGIN_TIMEOUT = 12000;
const WEBREPL_RESPONSE_TIMEOUT = 10000;
const WEBREPL_REPL_TIMEOUT = 10000;
const WEBREPL_CHUNK_SIZE = 1024;

const WEBREPL_REQ_S_SIZE = 82;
const WEBREPL_PUT_FILE = 1;
const WEBREPL_GET_VER = 3;

const WEBREPL_REPL_RAW_BANNER = 'raw REPL; CTRL-B to exit';
const WEBREPL_RAW_PASTE_MAX_COMMAND = 200;

/**
 * Build an 82-byte WebREPL request header.
 * struct format: <2sBBQLH64s
 * @param {number} op - request operation.
 * @param {number} size - payload size for file operations.
 * @param {string} fileName - remote file name.
 * @returns {Buffer} - the request header.
 */
const makeRequest = (op, size = 0, fileName = '') => {
    const header = Buffer.alloc(WEBREPL_REQ_S_SIZE);
    header.write('WA', 0, 2, 'ascii');
    header.writeUInt8(op, 2);
    header.writeUInt8(0, 3);
    // Q at offset 4 is reserved (offset into the file), always zero here.
    header.writeUInt32LE(size, 12);
    const name = Buffer.from(fileName, 'utf8');
    header.writeUInt16LE(Math.min(name.length, 64), 16);
    name.copy(header, 18, 0, Math.min(name.length, 64));
    return header;
};

/**
 * Parse a "WB"+uint16 response.
 * @param {Buffer} data - four response bytes.
 * @returns {number} - response code, -1 when the signature is invalid.
 */
const parseResponse = data => {
    if (data.length < 4 || data[0] !== 0x57 || data[1] !== 0x42) {
        return -1;
    }
    return data.readUInt16LE(2);
};

/**
 * Split a host:port peripheral id. Supports IPv6 in brackets.
 * @param {string} peripheralId - host:port string.
 * @returns {object} - {host, port}.
 */
const parseHostPort = peripheralId => {
    if (!peripheralId || typeof peripheralId !== 'string') {
        throw new Error('Invalid WebREPL address');
    }
    let host = peripheralId;
    let port = WEBREPL_DEFAULT_PORT;
    if (peripheralId.startsWith('[')) {
        const end = peripheralId.indexOf(']');
        if (end === -1) throw new Error('Invalid WebREPL address');
        host = peripheralId.slice(1, end);
        const rest = peripheralId.slice(end + 1);
        if (rest.startsWith(':')) {
            port = parseInt(rest.slice(1), 10);
        }
    } else if (peripheralId.indexOf(':') !== -1) {
        const split = peripheralId.lastIndexOf(':');
        host = peripheralId.slice(0, split);
        port = parseInt(peripheralId.slice(split + 1), 10);
    }
    if (!host) {
        throw new Error('Invalid WebREPL address');
    }
    if (!Number.isInteger(port) || port < 1 || port > 65535) {
        throw new Error('Invalid WebREPL port');
    }
    return {host, port};
};

const formatSocketError = err => {
    const code = err && err.code;
    const message = err && err.message ? err.message : String(err);
    if (code === 'ECONNREFUSED') {
        return 'Connection refused. Is WebREPL enabled on the device?';
    }
    if (code === 'ENOTFOUND' || code === 'EAI_AGAIN') {
        return 'Cannot resolve the device host name.';
    }
    if (code === 'ETIMEDOUT' || /timed? ?out/i.test(message)) {
        return 'Connection timed out. Check that the device is on the same network.';
    }
    if (/ECONNRESET|EPIPE|socket hang up/i.test(message)) {
        return 'The device closed the connection.';
    }
    return message;
};

/**
 * A WebREPL client speaking the MicroPython WebREPL protocol over ws.
 * Text frames are REPL output, binary frames are protocol responses.
 */
class WebRepl extends EventEmitter {
    /**
     * @param {object} options - connection options.
     * @param {string} options.host - device host name or IP.
     * @param {number} options.port - WebREPL port.
     * @param {string} options.password - WebREPL password.
     */
    constructor (options) {
        super();
        this._host = options.host;
        this._port = options.port || WEBREPL_DEFAULT_PORT;
        this._password = options.password || '';
        this._connectTimeout = options.connectTimeout || WEBREPL_CONNECT_TIMEOUT;
        this._loginTimeout = options.loginTimeout || WEBREPL_LOGIN_TIMEOUT;

        this._ws = null;
        this._closedByUser = false;
        this._expectedClose = false;
        this._authFailed = false;

        this._loginPhase = true;
        this._loginSent = false;
        this._loginBuffer = '';
        this._loginResolve = null;
        this._loginReject = null;
        this._loginTimer = null;

        this._binaryQueue = Buffer.alloc(0);
        this._binaryWaiter = null;

        this._captureText = false;
        this._captureBuffer = '';
        this._captureWaiter = null;

        this._rawRepl = false;
        this._abort = false;
    }

    get host () {
        return this._host;
    }

    get port () {
        return this._port;
    }

    /**
     * Connect and wait for the password prompt.
     * @returns {Promise} - resolved once the password has been sent.
     */
    connect () {
        this._closedByUser = false;
        this._expectedClose = false;
        this._authFailed = false;
        this._loginPhase = true;
        this._loginSent = false;
        this._loginBuffer = '';
        this._binaryQueue = Buffer.alloc(0);
        this._captureBuffer = '';
        this._captureText = false;
        this._rawRepl = false;
        this._abort = false;

        return new Promise((resolve, reject) => {
            this._loginResolve = resolve;
            this._loginReject = reject;
            this._loginTimer = setTimeout(() => {
                this._failLogin(new Error('Timed out waiting for the WebREPL password prompt.'));
            }, this._loginTimeout);
            this._openSocket();
        });
    }

    /**
     * Reconnect after a soft reset or a dropped link.
     * @param {number} timeout - optional shorter connect/login timeout.
     * @returns {Promise} - resolved once logged in again.
     */
    reconnect (timeout) {
        if (timeout) {
            this._connectTimeout = timeout;
            this._loginTimeout = timeout;
        }
        if (this._ws) {
            this._ws.removeAllListeners();
            try {
                this._ws.terminate();
            } catch (e) {
                // The old socket is already gone.
            }
            this._ws = null;
        }
        return this.connect();
    }

    /**
     * Open the underlying websocket.
     * @private
     */
    _openSocket () {
        const url = `ws://${this._host}:${this._port}/`;
        const ws = new WebSocket(url, {
            handshakeTimeout: this._connectTimeout,
            perMessageDeflate: false
        });
        this._ws = ws;

        const connectTimer = setTimeout(() => {
            if (ws.readyState !== WebSocket.OPEN) {
                this._handleSocketError(new Error('Connection timed out.'));
                try {
                    ws.terminate();
                } catch (e) {
                    // Already closed.
                }
            }
        }, this._connectTimeout);

        ws.on('open', () => {
            clearTimeout(connectTimer);
        });
        ws.on('message', data => {
            if (typeof data === 'string') {
                this._handleText(data);
            } else {
                this._handleBinary(Buffer.from(data));
            }
        });
        ws.on('error', err => {
            clearTimeout(connectTimer);
            this._handleSocketError(err);
        });
        ws.on('close', () => {
            clearTimeout(connectTimer);
            this._handleSocketClose();
        });
    }

    _handleSocketError (err) {
        const message = formatSocketError(err);
        if (this._loginPhase) {
            this._failLogin(new Error(message));
            return;
        }
        this._failBinary(new Error(message));
        this._failCapture(new Error(message));
        if (!this._expectedClose && !this._closedByUser) {
            this.emit('error', new Error(message));
        }
    }

    _handleSocketClose () {
        const expected = this._expectedClose || this._closedByUser;
        this._ws = null;
        if (this._loginPhase) {
            this._failLogin(new Error('The device closed the connection during login.'));
        }
        this._failBinary(new Error('WebREPL connection closed.'));
        this._failCapture(new Error('WebREPL connection closed.'));
        this.emit('close', expected);
    }

    _failLogin (error) {
        if (!this._loginPhase) return;
        this._loginPhase = false;
        if (this._loginTimer) {
            clearTimeout(this._loginTimer);
            this._loginTimer = null;
        }
        const reject = this._loginReject;
        this._loginResolve = null;
        this._loginReject = null;
        if (reject) reject(error);
    }

    _completeLogin () {
        this._loginPhase = false;
        if (this._loginTimer) {
            clearTimeout(this._loginTimer);
            this._loginTimer = null;
        }
        const resolve = this._loginResolve;
        this._loginResolve = null;
        this._loginReject = null;
        if (resolve) resolve();
    }

    _handleText (text) {
        // A repeated password prompt means the password was rejected. The
        // WebREPL server loops on the prompt instead of closing the socket.
        if (!this._loginPhase && this._loginSent && text.indexOf('Password: ') !== -1) {
            if (this._authFailed) return;
            this._authFailed = true;
            const error = new Error('Authentication failed. Check the WebREPL password.');
            this._failBinary(error);
            this._failCapture(error);
            this.emit('error', error);
            return;
        }

        if (this._loginPhase) {
            this._loginBuffer += text;
            const prompt = 'Password: ';
            const index = this._loginBuffer.indexOf(prompt);
            if (index === -1) return;

            this._loginSent = true;
            const afterPrompt = this._loginBuffer.slice(index + prompt.length);
            this._loginBuffer = '';
            // Upstream sends the password as a binary frame by default.
            this._writeBinary(Buffer.from(`${this._password}\r`, 'utf8'), () => {});
            this._completeLogin();
            if (afterPrompt) {
                this._forwardText(afterPrompt);
            }
            return;
        }

        this._forwardText(text);
    }

    _forwardText (text) {
        if (this._captureText) {
            this._captureBuffer += text;
            this._notifyCapture();
            return;
        }
        this.emit('text', Buffer.from(text, 'utf8'));
    }

    _handleBinary (data) {
        this._binaryQueue = Buffer.concat([this._binaryQueue, data]);
        this._notifyBinary();
    }

    _notifyBinary () {
        if (!this._binaryWaiter) return;
        const waiter = this._binaryWaiter;
        if (this._binaryQueue.length < waiter.size) return;
        const result = this._binaryQueue.slice(0, waiter.size);
        this._binaryQueue = this._binaryQueue.slice(waiter.size);
        this._binaryWaiter = null;
        clearTimeout(waiter.timer);
        waiter.resolve(result);
    }

    _readBinary (size, timeout = WEBREPL_RESPONSE_TIMEOUT) {
        return new Promise((resolve, reject) => {
            if (this._binaryWaiter) {
                return reject(new Error('Another WebREPL request is already waiting.'));
            }
            const timer = setTimeout(() => {
                if (this._binaryWaiter && this._binaryWaiter.size === size) {
                    this._binaryWaiter = null;
                }
                if (this._authFailed) {
                    return reject(new Error('Authentication failed. Check the WebREPL password.'));
                }
                return reject(new Error('Timed out waiting for a WebREPL response.'));
            }, timeout);
            this._binaryWaiter = {size, resolve, reject, timer};
            this._notifyBinary();
        });
    }

    _failBinary (error) {
        if (!this._binaryWaiter) return;
        const waiter = this._binaryWaiter;
        this._binaryWaiter = null;
        clearTimeout(waiter.timer);
        waiter.reject(error);
    }

    _notifyCapture () {
        if (!this._captureWaiter) return;
        const waiter = this._captureWaiter;
        const result = waiter.tryTake();
        if (result === null) return;
        this._captureWaiter = null;
        clearTimeout(waiter.timer);
        waiter.resolve(result);
    }

    _waitForCapture (tryTake, timeout, what) {
        return new Promise((resolve, reject) => {
            if (this._captureWaiter) {
                return reject(new Error('Another WebREPL REPL request is already waiting.'));
            }
            const timer = setTimeout(() => {
                if (this._captureWaiter && this._captureWaiter.tryTake === tryTake) {
                    this._captureWaiter = null;
                }
                if (this._authFailed) {
                    return reject(new Error('Authentication failed. Check the WebREPL password.'));
                }
                return reject(new Error(`Timed out waiting for ${what} from the board.`));
            }, timeout);
            this._captureWaiter = {tryTake, resolve, reject, timer};
            this._notifyCapture();
        });
    }

    _failCapture (error) {
        if (!this._captureWaiter) return;
        const waiter = this._captureWaiter;
        this._captureWaiter = null;
        clearTimeout(waiter.timer);
        waiter.reject(error);
    }

    _waitForCaptureText (marker, timeout = WEBREPL_REPL_TIMEOUT) {
        return this._waitForCapture(() => {
            const index = this._captureBuffer.indexOf(marker);
            if (index === -1) return null;
            const result = this._captureBuffer.slice(0, index);
            this._captureBuffer = this._captureBuffer.slice(index + marker.length);
            return result;
        }, timeout, `"${marker}"`);
    }

    _waitForCaptureCount (count, timeout = WEBREPL_REPL_TIMEOUT) {
        return this._waitForCapture(() => {
            if (this._captureBuffer.length < count) return null;
            const result = this._captureBuffer.slice(0, count);
            this._captureBuffer = this._captureBuffer.slice(count);
            return result;
        }, timeout, `${count} byte(s)`);
    }

    _writeBinary (buffer, callback) {
        if (!this._ws || this._ws.readyState !== WebSocket.OPEN) {
            const error = new Error('WebREPL connection is not open.');
            if (callback) {
                callback(error);
                return;
            }
            return Promise.reject(error);
        }
        if (callback) {
            this._ws.send(buffer, {binary: true}, callback);
            return;
        }
        return new Promise((resolve, reject) => {
            this._ws.send(buffer, {binary: true}, err => {
                if (err) reject(err);
                else resolve();
            });
        });
    }

    _writeText (text) {
        if (!this._ws || this._ws.readyState !== WebSocket.OPEN) {
            return Promise.reject(new Error('WebREPL connection is not open.'));
        }
        return new Promise((resolve, reject) => {
            this._ws.send(text, {binary: false}, err => {
                if (err) reject(err);
                else resolve();
            });
        });
    }

    /**
     * Send user input to the friendly REPL.
     * @param {string} text - text to send.
     * @returns {Promise} - resolved when the frame is queued.
     */
    writeRepl (text) {
        return this._writeText(text);
    }

    /**
     * Read the remote WebREPL protocol version.
     * @returns {Promise<Array.<number>>} - [major, minor, patch].
     */
    async getVersion () {
        const request = makeRequest(WEBREPL_GET_VER);
        await this._writeBinary(request);
        const response = await this._readBinary(3);
        return [response[0], response[1], response[2]];
    }

    /**
     * Upload a file through the WebREPL binary file protocol.
     * @param {string} fileName - name on the device.
     * @param {Buffer} data - file content.
     * @param {Function} onProgress - called with (sent, total).
     * @param {Function} isAborted - returns true when the upload was aborted.
     * @returns {Promise} - resolved when the device acknowledged the file.
     */
    async putFile (fileName, data, onProgress = null, isAborted = null) {
        const size = data.length;
        const request = makeRequest(WEBREPL_PUT_FILE, size, fileName);
        // The WebREPL server reads the 82-byte header as two frames.
        await this._writeBinary(request.slice(0, 10));
        await this._writeBinary(request.slice(10));

        let response = parseResponse(await this._readBinary(4));
        if (response !== 0) {
            throw new Error(`WebREPL refused ${fileName} (code ${response}).`);
        }

        let sent = 0;
        while (sent < size) {
            if (isAborted && isAborted()) {
                throw new Error('Aborted');
            }
            const chunk = data.slice(sent, sent + WEBREPL_CHUNK_SIZE);
            await this._writeBinary(chunk);
            sent += chunk.length;
            if (onProgress) onProgress(sent, size);
        }

        response = parseResponse(await this._readBinary(4));
        if (response !== 0) {
            throw new Error(`WebREPL failed to write ${fileName} (code ${response}).`);
        }
    }

    /**
     * Enter the raw REPL and run one Python command.
     * @param {string} command - Python source.
     * @param {number} timeout - response timeout in ms.
     * @param {object} options - execution options.
     * @param {boolean} options.echo - forward stdout to the console.
     * @returns {Promise<string>} - the command stdout.
     */
    async execRaw (command, timeout = WEBREPL_REPL_TIMEOUT, options = {}) {
        this._abort = false;
        this._captureText = true;
        this._captureBuffer = '';
        try {
            await this._ensureRawRepl(timeout);
            this._captureBuffer = '';

            if (command.length > WEBREPL_RAW_PASTE_MAX_COMMAND) {
                return await this._execRawPaste(command, timeout, options);
            }

            await this._writeText(`${command}\x04`);
            // Raw REPL acknowledges the end of input with "OK" before it
            // starts executing the command.
            await this._waitForCaptureText('OK', timeout);
            const output = await this._waitForCaptureText('\x04', timeout);
            const error = await this._waitForCaptureText('\x04', timeout);
            await this._waitForCaptureText('>', timeout);
            if (error.length > 0) {
                throw new Error(`Board error: ${error.trim()}`);
            }
            if (options.echo && output) {
                this.emit('text', Buffer.from(output, 'utf8'));
            }
            return output;
        } finally {
            this._captureText = false;
            this._captureBuffer = '';
        }
    }

    /**
     * Run a command through the flow-controlled raw-paste mode.
     * @param {string} command - Python source.
     * @param {number} timeout - response timeout in ms.
     * @param {object} options - execution options.
     * @returns {Promise<string>} - the command stdout.
     * @private
     */
    async _execRawPaste (command, timeout, options) {
        await this._writeText('\x05A\x01');
        const answer = await this._waitForCaptureCount(2, timeout);
        if (answer !== 'R\x01') {
            // Raw-paste is not supported by this firmware. Fall back to the
            // plain raw REPL, which is fine for the command sizes we use.
            this._captureBuffer = '';
            await this._writeText(`${command}\x04`);
            await this._waitForCaptureText('OK', timeout);
            const output = await this._waitForCaptureText('\x04', timeout);
            const error = await this._waitForCaptureText('\x04', timeout);
            await this._waitForCaptureText('>', timeout);
            if (error.length > 0) throw new Error(`Board error: ${error.trim()}`);
            if (options.echo && output) this.emit('text', Buffer.from(output, 'utf8'));
            return output;
        }

        const header = await this._waitForCaptureCount(2, timeout);
        const windowSize = header.charCodeAt(0) | (header.charCodeAt(1) << 8);
        let windowRemain = windowSize;
        let offset = 0;
        while (offset < command.length) {
            while (windowRemain === 0 || this._captureBuffer.length > 0) {
                const flow = await this._waitForCaptureCount(1, timeout);
                if (flow === '\x01') {
                    windowRemain += windowSize;
                } else if (flow === '\x04') {
                    await this._writeText('\x04');
                    // The board ended reception early; read the error reply.
                    await this._waitForCaptureText('\x04', timeout);
                    const error = await this._waitForCaptureText('\x04', timeout);
                    await this._waitForCaptureText('>', timeout);
                    throw new Error(`Board error: ${error.trim() || 'raw-paste failed'}`);
                }
            }
            const take = Math.min(windowRemain, command.length - offset);
            await this._writeText(command.slice(offset, offset + take));
            offset += take;
            windowRemain -= take;
        }
        await this._writeText('\x04');
        await this._waitForCaptureText('\x04', timeout);
        const output = await this._waitForCaptureText('\x04', timeout);
        const error = await this._waitForCaptureText('\x04', timeout);
        await this._waitForCaptureText('>', timeout);
        if (error.length > 0) throw new Error(`Board error: ${error.trim()}`);
        if (options.echo && output) this.emit('text', Buffer.from(output, 'utf8'));
        return output;
    }

    /**
     * Make sure the board REPL is in raw mode.
     * @param {number} timeout - banner timeout in ms.
     * @returns {Promise} - resolved at the raw REPL prompt.
     * @private
     */
    async _ensureRawRepl (timeout) {
        if (this._rawRepl) return;
        // Interrupt a running program, then request the raw REPL.
        await this._writeText('\x03');
        await new Promise(resolve => setTimeout(resolve, 100));
        await this._writeText('\r\x01');
        await this._waitForCaptureText(WEBREPL_REPL_RAW_BANNER, timeout);
        await this._waitForCaptureText('>', timeout);
        this._rawRepl = true;
    }

    /**
     * Leave the raw REPL and return to the friendly prompt.
     * @returns {Promise} - resolved when the frame is queued.
     */
    async exitRawRepl () {
        this._captureText = false;
        this._captureBuffer = '';
        if (!this._rawRepl) return;
        this._rawRepl = false;
        await this._writeText('\x02');
    }

    /**
     * Interrupt a running program.
     * @returns {Promise} - resolved when the interrupt is sent.
     */
    async interrupt () {
        await this._writeText('\x03');
    }

    /**
     * Soft reset the board. The connection usually drops and is reopened by
     * the session after the board restarts.
     * @returns {Promise} - resolved when the reset byte is queued.
     */
    async softReset () {
        this._rawRepl = false;
        this._expectedClose = true;
        await this._writeText('\x04');
    }

    /**
     * Stop treating the next socket close as an expected post-reset close.
     */
    clearExpectedClose () {
        this._expectedClose = false;
    }

    /**
     * @returns {boolean} - whether the underlying socket is open.
     */
    isOpen () {
        return !!this._ws && this._ws.readyState === WebSocket.OPEN;
    }

    /**
     * Close the connection.
     */
    close () {
        this._closedByUser = true;
        if (this._ws) {
            try {
                this._ws.close();
            } catch (e) {
                // Already closed.
            }
        }
        this._ws = null;
    }

    /**
     * Abort a running upload.
     */
    abort () {
        this._abort = true;
    }

    isAborted () {
        return this._abort;
    }
}

WebRepl.DEFAULT_PORT = WEBREPL_DEFAULT_PORT;
WebRepl.parseHostPort = parseHostPort;
WebRepl.formatSocketError = formatSocketError;

module.exports = WebRepl;
