"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
const http = __importStar(require("node:http"));
const https = __importStar(require("node:https"));
const Database_1 = require("./Database");
/**
 * InfluxDB 3 (Core / Enterprise).
 *
 * InfluxDB 3 still understands InfluxQL for reading (the 1.x compatible `/query` endpoint), so the whole
 * InfluxQL read path of the adapter (getHistory, query, raw entries, statistics) is shared with 1.x.
 * This client talks HTTP directly instead of using the 1.x driver, because that driver sends parameters
 * (empty `epoch`, `rp`, `params`) that InfluxDB 3 rejects, and it does not authenticate `/ping`.
 *
 * - write: `/api/v3/write_lp` (line protocol, millisecond precision, partial writes accepted)
 * - read: `/query` (InfluxQL, several statements separated by `;` allowed)
 * - databases, retention, dropping a measurement: `/api/v3/configure/*`
 *
 * InfluxDB 3 cannot delete single values or time ranges; only a whole measurement (table) can be dropped.
 * There are no tags/fields variants for the metadata either: q, ack and from are always fields.
 */
class DatabaseInfluxDB3x extends Database_1.Database {
    token;
    validateSSL;
    agent = null;
    constructor(options, db3xOptions) {
        super(options);
        this.token = db3xOptions.token;
        this.validateSSL = db3xOptions.validateSSL !== undefined ? db3xOptions.validateSSL : true;
        this.connect();
    }
    connect() {
        this.log.debug(`Connect InfluxDB3: ${this.protocol}://${this.host}:${this.port} [${this.database}]`);
        this.agent?.destroy();
        this.agent =
            this.protocol === 'https'
                ? new https.Agent({ keepAlive: true, rejectUnauthorized: this.validateSSL })
                : new http.Agent({ keepAlive: true });
    }
    /**
     * Send a request to InfluxDB 3.
     *
     * @param method HTTP method
     * @param path path incl. leading slash, without query string
     * @param query query string parameters
     * @param body request body: an object is sent as JSON, a string as is
     * @param contentType content type for a string body
     * @returns status code and response text
     */
    async request(method, path, query, body, contentType) {
        try {
            return await this.requestOnce(method, path, query, body, contentType);
        }
        catch (error) {
            // A keep-alive socket can be closed by the server just when it is reused for the next
            // request. Node then reports "socket hang up" (ECONNRESET) although the server is fine.
            // That request never reached the server, so it is safe to send it once more on a new socket.
            if (error.reusedSocket && DatabaseInfluxDB3x.isReset(error)) {
                this.log.debug(`${method} ${path}: kept-alive connection was closed by the server, retrying`);
                return await this.requestOnce(method, path, query, body, contentType);
            }
            throw error;
        }
    }
    static isReset(error) {
        const err = error;
        return err?.code === 'ECONNRESET' || /socket hang up/i.test(err?.message || '');
    }
    requestOnce(method, path, query, body, contentType) {
        const qs = query ? `?${new URLSearchParams(query).toString()}` : '';
        let payload;
        let type = contentType;
        if (typeof body === 'string') {
            payload = Buffer.from(body, 'utf8');
            type ||= 'text/plain; charset=utf-8';
        }
        else if (body !== undefined) {
            payload = Buffer.from(JSON.stringify(body), 'utf8');
            type = 'application/json';
        }
        const transport = this.protocol === 'https' ? https : http;
        return new Promise((resolve, reject) => {
            const req = transport.request({
                method,
                host: this.host,
                port: parseInt(this.port, 10) || 8181,
                path: `${path}${qs}`,
                agent: this.agent || undefined,
                timeout: this.requestTimeout || 30000,
                headers: {
                    Authorization: `Bearer ${this.token}`,
                    Accept: 'application/json',
                    ...(payload ? { 'Content-Type': type, 'Content-Length': payload.length } : {}),
                },
            }, res => {
                const chunks = [];
                res.on('data', (chunk) => chunks.push(chunk));
                res.on('end', () => {
                    const status = res.statusCode || 0;
                    const text = Buffer.concat(chunks).toString('utf8');
                    if (status >= 200 && status < 300) {
                        resolve({ status, text });
                        return;
                    }
                    let message = text.trim() || res.statusMessage || '';
                    try {
                        const parsed = JSON.parse(text);
                        message = parsed?.error || parsed?.message || message;
                        // a rejected write lists the reason per line
                        if (Array.isArray(parsed?.data)) {
                            const reasons = parsed.data
                                .map((line) => line?.error_message)
                                .filter((reason) => !!reason);
                            if (reasons.length) {
                                message += `: ${[...new Set(reasons)].slice(0, 5).join('; ')}`;
                            }
                        }
                    }
                    catch {
                        // plain text answer
                    }
                    const error = new Error(`InfluxDB 3 answered ${status} on ${method} ${path}: ${message}`);
                    error.statusCode = status;
                    reject(error);
                });
                res.on('error', reject);
            });
            req.on('timeout', () => req.destroy(new Error('Request timed out')));
            req.on('error', (error) => {
                // keep code/errno so the error is still recognized as connection error, but say which call failed
                error.message = `${method} ${path}: ${error.message}`;
                error.reusedSocket = req.reusedSocket;
                reject(error);
            });
            if (payload) {
                req.write(payload);
            }
            req.end();
        });
    }
    async ping() {
        try {
            await this.request('GET', '/ping');
            this.markHostAvailable();
            return [{ online: true }];
        }
        catch (error) {
            const status = error.statusCode;
            if (status && status < 500) {
                // The server answered - it is reachable, even if it did not like the request (e.g. 401
                // for a wrong token). That shows up on the next write with a meaningful message.
                this.markHostAvailable();
                return [{ online: true }];
            }
            this.markHostUnavailable();
            return [{ online: false }];
        }
    }
    async getDatabaseNames() {
        const { text } = await this.trackConnection(() => this.request('GET', '/api/v3/configure/database', { format: 'json' }));
        let rows;
        try {
            rows = text ? JSON.parse(text) : [];
        }
        catch {
            throw new Error(`Unexpected answer when listing databases: ${text}`);
        }
        return (Array.isArray(rows) ? rows : [])
            .map(row => (row['iox::database'] ?? row.name ?? row.db))
            .filter(name => !!name);
    }
    async createDatabase(dbname) {
        await this.trackConnection(() => this.request('POST', '/api/v3/configure/database', undefined, { db: dbname }));
    }
    async dropDatabase(dbname) {
        await this.trackConnection(() => this.request('DELETE', '/api/v3/configure/database', { db: dbname, hard_delete_at: 'now' }));
    }
    getMetaDataStorageType() {
        return Promise.resolve('fields');
    }
    async getRetentionPolicyForDB(dbname) {
        // InfluxDB 3 has no retention policies, only one retention period per database.
        // 1. `system.databases` (SQL) knows it directly. `SHOW RETENTION POLICIES` is not used first:
        //    some InfluxDB 3 versions drop the connection on it instead of answering.
        try {
            const { text } = await this.trackConnection(() => this.request('POST', '/api/v3/query_sql', undefined, {
                db: dbname,
                q: `SELECT retention_period_ns FROM system.databases WHERE database_name = '${dbname.replace(/'/g, "''")}'`,
                format: 'json',
            }));
            const rows = text ? JSON.parse(text) : [];
            if (Array.isArray(rows) && rows.length) {
                const ns = rows[0]?.retention_period_ns;
                // null = infinite
                const time = ns === null || ns === undefined ? 0 : Math.round(Number(ns) / 1_000_000_000);
                return { name: 'autogen', time };
            }
        }
        catch (error) {
            this.log.debug(`Cannot read retention from system.databases: ${error.message}`);
        }
        // 2. older servers without that column: InfluxQL, formatted like 1.x ("24h0m0s", "0s" = infinite)
        try {
            const rows = await this.query(`SHOW RETENTION POLICIES ON "${(0, Database_1.escapeInfluxQLIdentifier)(dbname)}"`);
            const row = rows?.[0];
            if (row?.duration) {
                const match = row.duration.match(/^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$/);
                const time = match
                    ? (parseInt(match[1], 10) || 0) * 3600 +
                        (parseInt(match[2], 10) || 0) * 60 +
                        (parseInt(match[3], 10) || 0)
                    : undefined;
                return { name: row.name || 'autogen', time };
            }
        }
        catch (error) {
            this.log.debug(`Cannot read retention via SHOW RETENTION POLICIES: ${error.message}`);
        }
        // unknown - the caller then simply (re)applies the configured period
        return { name: null, time: undefined };
    }
    async applyRetentionPolicyToDB(dbname, retention) {
        const retentionSeconds = parseInt(retention, 10) || 0;
        const old = await this.getRetentionPolicyForDB(dbname);
        if (old && old.time !== undefined && old.time === retentionSeconds) {
            this.log.debug(`Retention period for ${dbname} remains unchanged.`);
            return;
        }
        this.log.info(`Applying retention period for ${dbname}: ${retentionSeconds ? `${retentionSeconds} seconds` : 'infinity'}`);
        try {
            // Current servers: one endpoint sets the period, or clears it when none is given
            await this.request('PUT', '/api/v3/configure/database', undefined, retentionSeconds ? { db: dbname, retention_period: `${retentionSeconds}s` } : { db: dbname });
        }
        catch (error) {
            const status = error.statusCode;
            if (status !== 404 && status !== 405) {
                throw error;
            }
            // Older 3.x servers only know the dedicated retention endpoint
            if (retentionSeconds) {
                await this.request('POST', '/api/v3/configure/database/retention_period', {
                    db: dbname,
                    duration: `${retentionSeconds}s`,
                });
            }
            else {
                await this.request('DELETE', '/api/v3/configure/database/retention_period', { db: dbname });
            }
        }
    }
    deleteData() {
        return Promise.reject(new Error('InfluxDB 3 cannot delete single values or time ranges. Only all values of a datapoint can be removed.'));
    }
    async dropMeasurement(measurement) {
        await this.trackConnection(() => this.request('DELETE', '/api/v3/configure/table', {
            db: this.database,
            table: measurement,
            hard_delete_at: 'now',
        }));
    }
    // ------------------------------------------------------------------ write
    /** Escape a measurement name for line protocol */
    static escapeMeasurement(name) {
        return name.replace(/\\/g, '\\\\').replace(/,/g, '\\,').replace(/ /g, '\\ ').replace(/\n/g, '\\n');
    }
    /** Escape a tag key, tag value or field key for line protocol */
    static escapeKey(name) {
        return name
            .replace(/\\/g, '\\\\')
            .replace(/,/g, '\\,')
            .replace(/=/g, '\\=')
            .replace(/ /g, '\\ ')
            .replace(/\n/g, '\\n');
    }
    /** Format a field value for line protocol. Numbers are always written as float, like the 1.x driver does */
    static formatField(value) {
        if (typeof value === 'boolean') {
            return value ? 'true' : 'false';
        }
        if (typeof value === 'number') {
            return String(value);
        }
        return `"${String(value).replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n')}"`;
    }
    /** Convert one value into a line of line protocol. Everything except time and tags is a field */
    static toLine(seriesId, point) {
        let line = DatabaseInfluxDB3x.escapeMeasurement(seriesId);
        if (point.tags) {
            for (const name of Object.keys(point.tags).sort()) {
                const value = point.tags[name];
                if (value !== undefined && value !== null && value !== '') {
                    line += `,${DatabaseInfluxDB3x.escapeKey(name)}=${DatabaseInfluxDB3x.escapeKey(String(value))}`;
                }
            }
        }
        const fields = [];
        for (const key of Object.keys(point)) {
            if (key === 'time' || key === 'tags') {
                continue;
            }
            const value = point[key];
            if (value === undefined || value === null || (key === 'from' && !value)) {
                continue;
            }
            fields.push(`${DatabaseInfluxDB3x.escapeKey(key)}=${DatabaseInfluxDB3x.formatField(value)}`);
        }
        return `${line} ${fields.join(',')} ${Math.round(point.time)}`;
    }
    async writeLines(lines) {
        if (!lines.length) {
            return;
        }
        await this.trackConnection(() => this.request('POST', '/api/v3/write_lp', { db: this.database, precision: 'millisecond', accept_partial: 'true' }, lines.join('\n')));
    }
    /**
     * InfluxDB 3 words a type conflict differently than 1.x/2.x ("invalid column type for column 'value',
     * expected iox::column_type::field::float, got iox::column_type::field::boolean"). The adapter detects
     * and repairs conflicts by the 1.x wording, so the message is translated.
     *
     * @param seriesId the measurement that was written
     * @param error the error of the request
     */
    static translateError(seriesId, error) {
        const text = error instanceof Error ? error.message : String(error);
        const match = text.match(/column '?([^',]+)'?,? expected (?:iox::column_type::field::)?(\w+), got (?:iox::column_type::field::)?(\w+)/i);
        if (!match) {
            return error;
        }
        const typeName = (type) => {
            switch (type.toLowerCase()) {
                case 'boolean':
                case 'bool':
                    return 'bool';
                case 'float':
                case 'f64':
                    return 'float';
                case 'integer':
                case 'i64':
                    return 'integer';
                case 'uinteger':
                case 'u64':
                    return 'unsigned';
                default:
                    return type.toLowerCase();
            }
        };
        const translated = new Error(`field type conflict: input field "${match[1]}" on measurement "${seriesId}" is type ${typeName(match[3])}, already exists as type ${typeName(match[2])} (${text})`);
        translated.cause = error;
        return translated;
    }
    async writeSeries(series) {
        const lines = [];
        for (const seriesId of Object.keys(series)) {
            for (const point of series[seriesId]) {
                lines.push(DatabaseInfluxDB3x.toLine(seriesId, point));
            }
        }
        try {
            await this.writeLines(lines);
        }
        catch (error) {
            const ids = Object.keys(series);
            throw DatabaseInfluxDB3x.translateError(ids.length === 1 ? ids[0] : '*', error);
        }
    }
    async writePoints(seriesId, pointsToSend) {
        try {
            await this.writeLines(pointsToSend.map(point => DatabaseInfluxDB3x.toLine(seriesId, point)));
        }
        catch (error) {
            throw DatabaseInfluxDB3x.translateError(seriesId, error);
        }
    }
    async writePoint(seriesId, pointToSend) {
        await this.writePoints(seriesId, [pointToSend]);
    }
    // ------------------------------------------------------------------ read
    /** Turn the series of one statement into rows, the way the 1.x driver does */
    static toRows(series = []) {
        const rows = [];
        const groups = [];
        for (const one of series) {
            const columns = one.columns || [];
            const groupRows = [];
            for (const values of one.values || []) {
                const row = {};
                for (let i = 0; i < columns.length; i++) {
                    row[columns[i]] =
                        columns[i] === 'time' && values[i] !== null && values[i] !== undefined
                            ? new Date(values[i])
                            : values[i];
                }
                if (one.tags) {
                    Object.assign(row, one.tags);
                }
                rows.push(row);
                groupRows.push(row);
            }
            groups.push({ name: one.name || '', tags: one.tags || {}, rows: groupRows });
        }
        rows.groups = () => groups;
        return rows;
    }
    /**
     * Run InfluxQL against the 1.x compatible endpoint.
     *
     * @param query one or more statements, separated by `;`
     * @returns one entry per statement
     */
    /**
     * Split InfluxQL into single statements at `;`, ignoring semicolons inside quoted identifiers,
     * string literals and regular expressions (e.g. a state ID like `"my;id"`).
     *
     * @param query one or more statements
     * @returns the non-empty statements, trimmed
     */
    static splitStatements(query) {
        const statements = [];
        let current = '';
        let quote = null;
        for (let i = 0; i < query.length; i++) {
            const char = query[i];
            if (quote) {
                current += char;
                if (char === '\\' && i + 1 < query.length) {
                    current += query[++i];
                }
                else if (char === quote) {
                    quote = null;
                }
            }
            else if (char === '"' || char === "'") {
                quote = char;
                current += char;
            }
            else if (char === '/' && /(?:\bFROM|=~|!~|,)\s*$/i.test(current)) {
                // a regular expression follows FROM, a regex match operator or a comma in the FROM
                // list (`FROM /.*\/`, `=~ /x/`) - anywhere else `/` is a division
                quote = '/';
                current += char;
            }
            else if (char === ';') {
                statements.push(current);
                current = '';
            }
            else {
                current += char;
            }
        }
        statements.push(current);
        return statements.map(statement => statement.trim()).filter(statement => !!statement);
    }
    /**
     * Run one InfluxQL statement against the 1.x compatible endpoint.
     *
     * @param statement a single statement
     * @returns the rows of the statement
     */
    async runStatement(statement) {
        this.log.debug(`Query to execute: ${statement}`);
        // POST with a form body: no URL length limit for long queries; epoch=ms gives numeric timestamps
        const body = new URLSearchParams({ db: this.database, q: statement, epoch: 'ms' }).toString();
        const { text } = await this.trackConnection(() => this.request('POST', '/query', undefined, body, 'application/x-www-form-urlencoded'));
        let response;
        try {
            response = JSON.parse(text);
        }
        catch {
            throw new Error(`Unexpected answer from InfluxDB 3: ${text.slice(0, 200)}`);
        }
        if (response.error) {
            throw new Error(`Error from InfluxDB: ${response.error}`);
        }
        const result = response.results?.[0];
        if (result?.error) {
            throw new Error(`Error from InfluxDB: ${result.error}`);
        }
        return DatabaseInfluxDB3x.toRows(result?.series);
    }
    /**
     * Run InfluxQL. InfluxDB 3 accepts only one statement per request (depending on the version), so
     * several statements separated by `;` are sent one after the other.
     *
     * @param query one or more statements, separated by `;`
     * @returns one entry per statement
     */
    async runInfluxQL(query) {
        const statements = DatabaseInfluxDB3x.splitStatements(query);
        const results = [];
        for (const statement of statements) {
            results.push(await this.runStatement(statement));
        }
        return results;
    }
    /**
     * Same result shape as the 1.x driver: the rows for a single statement, an array of row lists for
     * several statements (`SELECT ...;SELECT ...`).
     *
     * @param query InfluxQL
     */
    async query(query) {
        const results = await this.runInfluxQL(query);
        if (results.length === 1) {
            return results[0];
        }
        return results;
    }
    /** Rows of one statement grouped by measurement - `SELECT ... FROM /.*\/` returns one series per measurement */
    async queryGrouped(query) {
        const results = await this.runInfluxQL(query);
        return (results[0]?.groups() || []);
    }
    async getStatistics(start, stop) {
        const where = ` WHERE time >= '${new Date(start).toISOString()}' AND time <= '${new Date(stop).toISOString()}'`;
        const statistics = {};
        const entryOf = (name) => (statistics[name] ||= { count: 0, firstTs: null, lastTs: null, cardinality: null });
        for (const group of await this.queryGrouped(`SELECT count("value") FROM /.*/${where}`)) {
            entryOf(group.name).count = Number(group.rows[0]?.count) || 0;
        }
        for (const group of await this.queryGrouped(`SELECT first("value") FROM /.*/${where}`)) {
            const ts = group.rows[0]?.time;
            entryOf(group.name).firstTs = ts ? new Date(ts).getTime() : null;
        }
        for (const group of await this.queryGrouped(`SELECT last("value") FROM /.*/${where}`)) {
            const ts = group.rows[0]?.time;
            entryOf(group.name).lastTs = ts ? new Date(ts).getTime() : null;
        }
        // InfluxDB 3 has no series index and no `SHOW SERIES`, so there is no cheap cardinality.
        // It stays empty instead of scanning every table.
        return statistics;
    }
}
exports.default = DatabaseInfluxDB3x;
