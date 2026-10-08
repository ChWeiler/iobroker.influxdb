import * as http from 'node:http';
import * as https from 'node:https';
import { Database, escapeInfluxQLIdentifier, type ValuesForInflux } from './Database';
import type { MeasurementStatistics } from './statistics';

/** A row of an InfluxQL answer; `time` is converted to a Date like the 1.x driver does */
type Row = Record<string, any> & { time: Date };

/** Rows of one statement, with the per-measurement grouping the 1.x driver offers via `groups()` */
type StatementRows = Row[] & { groups: () => Array<{ name: string; tags: Record<string, string>; rows: Row[] }> };

interface V1Series {
    name?: string;
    columns?: string[];
    tags?: Record<string, string>;
    values?: unknown[][];
}

interface V1Response {
    results?: Array<{ statement_id?: number; series?: V1Series[]; error?: string }>;
    error?: string;
}

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
export default class DatabaseInfluxDB3x extends Database {
    private readonly token: string;
    private readonly validateSSL: boolean;
    private agent: http.Agent | https.Agent | null = null;

    constructor(
        options: {
            log: ioBroker.Logger;
            host: string;
            port: number | string;
            protocol: 'http' | 'https';
            database: string;
            requestTimeout: number;
        },
        db3xOptions: {
            token: string;
            validateSSL?: boolean;
        },
    ) {
        super(options);
        this.token = db3xOptions.token;
        this.validateSSL = db3xOptions.validateSSL !== undefined ? db3xOptions.validateSSL : true;
        this.connect();
    }

    connect(): void {
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
    private async request(
        method: 'GET' | 'POST' | 'PUT' | 'DELETE',
        path: string,
        query?: Record<string, string>,
        body?: unknown,
        contentType?: string,
    ): Promise<{ status: number; text: string }> {
        try {
            return await this.requestOnce(method, path, query, body, contentType);
        } catch (error) {
            // A keep-alive socket can be closed by the server just when it is reused for the next
            // request. Node then reports "socket hang up" (ECONNRESET) although the server is fine.
            // That request never reached the server, so it is safe to send it once more on a new socket.
            if ((error as { reusedSocket?: boolean }).reusedSocket && DatabaseInfluxDB3x.isReset(error)) {
                this.log.debug(`${method} ${path}: kept-alive connection was closed by the server, retrying`);
                return await this.requestOnce(method, path, query, body, contentType);
            }
            throw error;
        }
    }

    private static isReset(error: unknown): boolean {
        const err = error as { code?: string; message?: string };
        return err?.code === 'ECONNRESET' || /socket hang up/i.test(err?.message || '');
    }

    private requestOnce(
        method: 'GET' | 'POST' | 'PUT' | 'DELETE',
        path: string,
        query?: Record<string, string>,
        body?: unknown,
        contentType?: string,
    ): Promise<{ status: number; text: string }> {
        const qs = query ? `?${new URLSearchParams(query).toString()}` : '';
        let payload: Buffer | undefined;
        let type = contentType;
        if (typeof body === 'string') {
            payload = Buffer.from(body, 'utf8');
            type ||= 'text/plain; charset=utf-8';
        } else if (body !== undefined) {
            payload = Buffer.from(JSON.stringify(body), 'utf8');
            type = 'application/json';
        }
        const transport = this.protocol === 'https' ? https : http;

        return new Promise((resolve, reject) => {
            const req = transport.request(
                {
                    method,
                    host: this.host,
                    port: parseInt(this.port as string, 10) || 8181,
                    path: `${path}${qs}`,
                    agent: this.agent || undefined,
                    timeout: this.requestTimeout || 30000,
                    headers: {
                        Authorization: `Bearer ${this.token}`,
                        Accept: 'application/json',
                        ...(payload ? { 'Content-Type': type!, 'Content-Length': payload.length } : {}),
                    },
                },
                res => {
                    const chunks: Buffer[] = [];
                    res.on('data', (chunk: Buffer) => chunks.push(chunk));
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
                                    .map((line: { error_message?: string }) => line?.error_message)
                                    .filter((reason: string | undefined) => !!reason);
                                if (reasons.length) {
                                    message += `: ${[...new Set(reasons)].slice(0, 5).join('; ')}`;
                                }
                            }
                        } catch {
                            // plain text answer
                        }
                        const error = new Error(`InfluxDB 3 answered ${status} on ${method} ${path}: ${message}`);
                        (error as Error & { statusCode: number }).statusCode = status;
                        reject(error);
                    });
                    res.on('error', reject);
                },
            );
            req.on('timeout', () => req.destroy(new Error('Request timed out')));
            req.on('error', (error: Error & { reusedSocket?: boolean }) => {
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

    async ping(): Promise<{ online: boolean }[]> {
        try {
            await this.request('GET', '/ping');
            this.markHostAvailable();
            return [{ online: true }];
        } catch (error) {
            const status = (error as { statusCode?: number }).statusCode;
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

    async getDatabaseNames(): Promise<string[]> {
        const { text } = await this.trackConnection(() =>
            this.request('GET', '/api/v3/configure/database', { format: 'json' }),
        );
        let rows: unknown;
        try {
            rows = text ? JSON.parse(text) : [];
        } catch {
            throw new Error(`Unexpected answer when listing databases: ${text}`);
        }
        return (Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [])
            .map(row => (row['iox::database'] ?? row.name ?? row.db) as string)
            .filter(name => !!name);
    }

    async createDatabase(dbname: string): Promise<void> {
        await this.trackConnection(() => this.request('POST', '/api/v3/configure/database', undefined, { db: dbname }));
    }

    async dropDatabase(dbname: string): Promise<void> {
        await this.trackConnection(() =>
            this.request('DELETE', '/api/v3/configure/database', { db: dbname, hard_delete_at: 'now' }),
        );
    }

    getMetaDataStorageType(): Promise<'tags' | 'fields'> {
        return Promise.resolve('fields');
    }

    async getRetentionPolicyForDB(dbname: string): Promise<{ name: string | null; time: number | undefined } | null> {
        // InfluxDB 3 has no retention policies, only one retention period per database.
        // 1. `system.databases` (SQL) knows it directly. `SHOW RETENTION POLICIES` is not used first:
        //    some InfluxDB 3 versions drop the connection on it instead of answering.
        try {
            const { text } = await this.trackConnection(() =>
                this.request('POST', '/api/v3/query_sql', undefined, {
                    db: dbname,
                    q: `SELECT retention_period_ns FROM system.databases WHERE database_name = '${dbname.replace(/'/g, "''")}'`,
                    format: 'json',
                }),
            );
            const rows = text ? JSON.parse(text) : [];
            if (Array.isArray(rows) && rows.length) {
                const ns = rows[0]?.retention_period_ns;
                // null = infinite
                const time = ns === null || ns === undefined ? 0 : Math.round(Number(ns) / 1_000_000_000);
                return { name: 'autogen', time };
            }
        } catch (error) {
            this.log.debug(`Cannot read retention from system.databases: ${(error as Error).message}`);
        }

        // 2. older servers without that column: InfluxQL, formatted like 1.x ("24h0m0s", "0s" = infinite)
        try {
            const rows = await this.query<{ name?: string; duration?: string }>(
                `SHOW RETENTION POLICIES ON "${escapeInfluxQLIdentifier(dbname)}"`,
            );
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
        } catch (error) {
            this.log.debug(`Cannot read retention via SHOW RETENTION POLICIES: ${(error as Error).message}`);
        }

        // unknown - the caller then simply (re)applies the configured period
        return { name: null, time: undefined };
    }

    async applyRetentionPolicyToDB(dbname: string, retention: string | number): Promise<void> {
        const retentionSeconds = parseInt(retention as string, 10) || 0;
        const old = await this.getRetentionPolicyForDB(dbname);
        if (old && old.time !== undefined && old.time === retentionSeconds) {
            this.log.debug(`Retention period for ${dbname} remains unchanged.`);
            return;
        }

        this.log.info(
            `Applying retention period for ${dbname}: ${retentionSeconds ? `${retentionSeconds} seconds` : 'infinity'}`,
        );

        try {
            // Current servers: one endpoint sets the period, or clears it when none is given
            await this.request(
                'PUT',
                '/api/v3/configure/database',
                undefined,
                retentionSeconds ? { db: dbname, retention_period: `${retentionSeconds}s` } : { db: dbname },
            );
        } catch (error) {
            const status = (error as { statusCode?: number }).statusCode;
            if (status !== 404 && status !== 405) {
                throw error;
            }
            // Older 3.x servers only know the dedicated retention endpoint
            if (retentionSeconds) {
                await this.request('POST', '/api/v3/configure/database/retention_period', {
                    db: dbname,
                    duration: `${retentionSeconds}s`,
                });
            } else {
                await this.request('DELETE', '/api/v3/configure/database/retention_period', { db: dbname });
            }
        }
    }

    deleteData(): Promise<void> {
        return Promise.reject(
            new Error(
                'InfluxDB 3 cannot delete single values or time ranges. Only all values of a datapoint can be removed.',
            ),
        );
    }

    async dropMeasurement(measurement: string): Promise<void> {
        await this.trackConnection(() =>
            this.request('DELETE', '/api/v3/configure/table', {
                db: this.database,
                table: measurement,
                hard_delete_at: 'now',
            }),
        );
    }

    // ------------------------------------------------------------------ write

    /** Escape a measurement name for line protocol */
    private static escapeMeasurement(name: string): string {
        return name.replace(/\\/g, '\\\\').replace(/,/g, '\\,').replace(/ /g, '\\ ').replace(/\n/g, '\\n');
    }

    /** Escape a tag key, tag value or field key for line protocol */
    private static escapeKey(name: string): string {
        return name
            .replace(/\\/g, '\\\\')
            .replace(/,/g, '\\,')
            .replace(/=/g, '\\=')
            .replace(/ /g, '\\ ')
            .replace(/\n/g, '\\n');
    }

    /** Format a field value for line protocol. Numbers are always written as float, like the 1.x driver does */
    private static formatField(value: unknown): string {
        if (typeof value === 'boolean') {
            return value ? 'true' : 'false';
        }
        if (typeof value === 'number') {
            return String(value);
        }
        return `"${String(value).replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n')}"`;
    }

    /** Convert one value into a line of line protocol. Everything except time and tags is a field */
    static toLine(seriesId: string, point: ValuesForInflux): string {
        let line = DatabaseInfluxDB3x.escapeMeasurement(seriesId);
        if (point.tags) {
            for (const name of Object.keys(point.tags).sort()) {
                const value = point.tags[name];
                if (value !== undefined && value !== null && value !== '') {
                    line += `,${DatabaseInfluxDB3x.escapeKey(name)}=${DatabaseInfluxDB3x.escapeKey(String(value))}`;
                }
            }
        }
        const fields: string[] = [];
        for (const key of Object.keys(point) as (keyof ValuesForInflux)[]) {
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

    private async writeLines(lines: string[]): Promise<void> {
        if (!lines.length) {
            return;
        }
        await this.trackConnection(() =>
            this.request(
                'POST',
                '/api/v3/write_lp',
                { db: this.database, precision: 'millisecond', accept_partial: 'true' },
                lines.join('\n'),
            ),
        );
    }

    /**
     * InfluxDB 3 words a type conflict differently than 1.x/2.x ("invalid column type for column 'value',
     * expected iox::column_type::field::float, got iox::column_type::field::boolean"). The adapter detects
     * and repairs conflicts by the 1.x wording, so the message is translated.
     *
     * @param seriesId the measurement that was written
     * @param error the error of the request
     */
    static translateError(seriesId: string, error: unknown): unknown {
        const text = error instanceof Error ? error.message : String(error);
        const match = text.match(
            /column '?([^',]+)'?,? expected (?:iox::column_type::field::)?(\w+), got (?:iox::column_type::field::)?(\w+)/i,
        );
        if (!match) {
            return error;
        }
        const typeName = (type: string): string => {
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
        const translated = new Error(
            `field type conflict: input field "${match[1]}" on measurement "${seriesId}" is type ${typeName(match[3])}, already exists as type ${typeName(match[2])} (${text})`,
        );
        (translated as Error & { cause?: unknown }).cause = error;
        return translated;
    }

    async writeSeries(series: { [id: string]: ValuesForInflux[] }): Promise<void> {
        const lines: string[] = [];
        for (const seriesId of Object.keys(series)) {
            for (const point of series[seriesId]) {
                lines.push(DatabaseInfluxDB3x.toLine(seriesId, point));
            }
        }
        try {
            await this.writeLines(lines);
        } catch (error) {
            const ids = Object.keys(series);
            throw DatabaseInfluxDB3x.translateError(ids.length === 1 ? ids[0] : '*', error);
        }
    }

    async writePoints(seriesId: string, pointsToSend: ValuesForInflux[]): Promise<void> {
        try {
            await this.writeLines(pointsToSend.map(point => DatabaseInfluxDB3x.toLine(seriesId, point)));
        } catch (error) {
            throw DatabaseInfluxDB3x.translateError(seriesId, error);
        }
    }

    async writePoint(seriesId: string, pointToSend: ValuesForInflux): Promise<void> {
        await this.writePoints(seriesId, [pointToSend]);
    }

    // ------------------------------------------------------------------ read

    /** Turn the series of one statement into rows, the way the 1.x driver does */
    private static toRows(series: V1Series[] = []): StatementRows {
        const rows = [] as unknown as StatementRows;
        const groups: Array<{ name: string; tags: Record<string, string>; rows: Row[] }> = [];
        for (const one of series) {
            const columns = one.columns || [];
            const groupRows: Row[] = [];
            for (const values of one.values || []) {
                const row: Record<string, any> = {};
                for (let i = 0; i < columns.length; i++) {
                    row[columns[i]] =
                        columns[i] === 'time' && values[i] !== null && values[i] !== undefined
                            ? new Date(values[i] as number | string)
                            : values[i];
                }
                if (one.tags) {
                    Object.assign(row, one.tags);
                }
                rows.push(row as Row);
                groupRows.push(row as Row);
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
    static splitStatements(query: string): string[] {
        const statements: string[] = [];
        let current = '';
        let quote: string | null = null;
        for (let i = 0; i < query.length; i++) {
            const char = query[i];
            if (quote) {
                current += char;
                if (char === '\\' && i + 1 < query.length) {
                    current += query[++i];
                } else if (char === quote) {
                    quote = null;
                }
            } else if (char === '"' || char === "'") {
                quote = char;
                current += char;
            } else if (char === '/' && /(?:\bFROM|=~|!~|,)\s*$/i.test(current)) {
                // a regular expression follows FROM, a regex match operator or a comma in the FROM
                // list (`FROM /.*\/`, `=~ /x/`) - anywhere else `/` is a division
                quote = '/';
                current += char;
            } else if (char === ';') {
                statements.push(current);
                current = '';
            } else {
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
    private async runStatement(statement: string): Promise<StatementRows> {
        this.log.debug(`Query to execute: ${statement}`);
        // POST with a form body: no URL length limit for long queries; epoch=ms gives numeric timestamps
        const body = new URLSearchParams({ db: this.database, q: statement, epoch: 'ms' }).toString();
        const { text } = await this.trackConnection(() =>
            this.request('POST', '/query', undefined, body, 'application/x-www-form-urlencoded'),
        );
        let response: V1Response;
        try {
            response = JSON.parse(text);
        } catch {
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
    private async runInfluxQL(query: string): Promise<StatementRows[]> {
        const statements = DatabaseInfluxDB3x.splitStatements(query);
        const results: StatementRows[] = [];
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
    async query<T>(query: string): Promise<Array<T & { time: Date }>> {
        const results = await this.runInfluxQL(query);
        if (results.length === 1) {
            return results[0] as unknown as Array<T & { time: Date }>;
        }
        return results as unknown as Array<T & { time: Date }>;
    }

    /** Rows of one statement grouped by measurement - `SELECT ... FROM /.*\/` returns one series per measurement */
    private async queryGrouped<T>(query: string): Promise<Array<{ name: string; rows: T[] }>> {
        const results = await this.runInfluxQL(query);
        return (results[0]?.groups() || []) as unknown as Array<{ name: string; rows: T[] }>;
    }

    async getStatistics(start: number, stop: number): Promise<MeasurementStatistics> {
        const where = ` WHERE time >= '${new Date(start).toISOString()}' AND time <= '${new Date(stop).toISOString()}'`;
        const statistics: MeasurementStatistics = {};
        const entryOf = (name: string): MeasurementStatistics[string] =>
            (statistics[name] ||= { count: 0, firstTs: null, lastTs: null, cardinality: null });

        for (const group of await this.queryGrouped<{ count: number }>(`SELECT count("value") FROM /.*/${where}`)) {
            entryOf(group.name).count = Number(group.rows[0]?.count) || 0;
        }
        for (const group of await this.queryGrouped<{ time: Date }>(`SELECT first("value") FROM /.*/${where}`)) {
            const ts = group.rows[0]?.time;
            entryOf(group.name).firstTs = ts ? new Date(ts).getTime() : null;
        }
        for (const group of await this.queryGrouped<{ time: Date }>(`SELECT last("value") FROM /.*/${where}`)) {
            const ts = group.rows[0]?.time;
            entryOf(group.name).lastTs = ts ? new Date(ts).getTime() : null;
        }
        // InfluxDB 3 has no series index and no `SHOW SERIES`, so there is no cheap cardinality.
        // It stays empty instead of scanning every table.
        return statistics;
    }
}
