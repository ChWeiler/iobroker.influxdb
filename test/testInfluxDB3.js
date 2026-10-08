const assert = require('node:assert');
const http = require('node:http');
const DatabaseInfluxDB3x = require('../build/lib/DatabaseInfluxDB3x').default;

/**
 * Offline tests of the InfluxDB 3 client against a small mock of the InfluxDB 3 HTTP API.
 * The answers mimic the shape of InfluxDB 3 Core (v1 compatible /query, /api/v3/* endpoints).
 */
describe('Test InfluxDB 3 client', function () {
    let server;
    let port;
    let calls = [];
    let putSupported = true;

    const log = { silly() {}, debug() {}, info() {}, warn() {}, error() {} };
    const createClient = () =>
        new DatabaseInfluxDB3x(
            { log, host: '127.0.0.1', port, protocol: 'http', database: 'iobroker', requestTimeout: 2000 },
            { token: 'apiv3_test' },
        );

    before(function (done) {
        server = http.createServer((req, res) => {
            let body = '';
            req.on('data', chunk => (body += chunk));
            req.on('end', () => {
                const url = new URL(req.url, 'http://localhost');
                calls.push({ method: req.method, path: url.pathname, query: url.searchParams, body, auth: req.headers.authorization });
                const json = (code, obj) => {
                    res.writeHead(code, { 'content-type': 'application/json' });
                    res.end(JSON.stringify(obj));
                };
                if (req.headers.authorization !== 'Bearer apiv3_test') {
                    return json(401, { error: 'unauthenticated' });
                }
                if (url.pathname === '/ping') {
                    return json(200, { version: '3.x' });
                }
                if (url.pathname === '/api/v3/configure/database') {
                    if (req.method === 'GET') {
                        return json(200, [{ 'iox::database': '_internal' }, { 'iox::database': 'iobroker' }]);
                    }
                    if (req.method === 'PUT' && !putSupported) {
                        return json(405, { error: 'method not allowed' });
                    }
                    res.writeHead(200);
                    return res.end();
                }
                if (url.pathname.startsWith('/api/v3/configure/')) {
                    res.writeHead(204);
                    return res.end();
                }
                if (url.pathname === '/query') {
                    const q = new URLSearchParams(body).get('q');
                    if (q.startsWith('SHOW RETENTION POLICIES')) {
                        return json(200, {
                            results: [
                                {
                                    statement_id: 0,
                                    series: [
                                        {
                                            name: 'retention_policies',
                                            columns: ['name', 'duration'],
                                            values: [['autogen', '24h0m0s']],
                                        },
                                    ],
                                },
                            ],
                        });
                    }
                    if (q.startsWith('SELECT count')) {
                        return json(200, {
                            results: [
                                {
                                    statement_id: 0,
                                    series: [
                                        { name: 'a.0.x', columns: ['time', 'count'], values: [[0, 5]] },
                                        { name: 'b.0.y', columns: ['time', 'count'], values: [[0, 7]] },
                                    ],
                                },
                            ],
                        });
                    }
                    if (q.includes(';')) {
                        return json(500, { error: 'must provide only one InfluxQl statement per query' });
                    }
                    if (q.startsWith('SELECT mean')) {
                        return json(200, {
                            results: [
                                {
                                    statement_id: 0,
                                    series: [{ name: 'a.0.x', columns: ['time', 'val'], values: [[1700000000000, 1.5]] }],
                                },
                            ],
                        });
                    }
                    if (q.startsWith('SELECT value')) {
                        return json(200, {
                            results: [
                                {
                                    statement_id: 0,
                                    series: [{ name: 'a.0.x', columns: ['time', 'value'], values: [[1699999990000, 1]] }],
                                },
                            ],
                        });
                    }
                    if (q.startsWith('BAD')) {
                        return json(200, { results: [{ statement_id: 0, error: 'error parsing query' }] });
                    }
                    return json(200, { results: [{ statement_id: 0 }] });
                }
                if (url.pathname === '/api/v3/write_lp') {
                    if (body.includes('value=true')) {
                        return json(400, {
                            error: 'partial write of line protocol occurred',
                            data: [
                                {
                                    original_line: body,
                                    line_number: 1,
                                    error_message:
                                        "invalid column type for column 'value', expected iox::column_type::field::float, got iox::column_type::field::boolean",
                                },
                            ],
                        });
                    }
                    res.writeHead(204);
                    return res.end();
                }
                res.writeHead(404);
                res.end();
            });
        });
        server.listen(0, '127.0.0.1', () => {
            port = server.address().port;
            done();
        });
    });

    beforeEach(function () {
        calls = [];
        putSupported = true;
    });

    after(function (done) {
        server.close(() => done());
    });

    it('retries once when the server closed a kept-alive connection', async function () {
        // every second request on the same socket is answered by closing the socket, like a server
        // that drops an idle keep-alive connection right when the client reuses it
        const perSocket = new WeakMap();
        const resetServer = http.createServer((req, res) => {
            const count = (perSocket.get(req.socket) || 0) + 1;
            perSocket.set(req.socket, count);
            req.resume();
            req.on('end', () => {
                if (count > 1) {
                    req.socket.destroy();
                    return;
                }
                res.writeHead(200, { 'content-type': 'application/json', connection: 'keep-alive' });
                res.end(JSON.stringify([{ 'iox::database': 'iobroker' }]));
            });
        });
        await new Promise(resolve => resetServer.listen(0, '127.0.0.1', resolve));
        try {
            const client = new DatabaseInfluxDB3x(
                {
                    log,
                    host: '127.0.0.1',
                    port: resetServer.address().port,
                    protocol: 'http',
                    database: 'iobroker',
                    requestTimeout: 2000,
                },
                { token: 'apiv3_test' },
            );
            for (let i = 0; i < 3; i++) {
                assert.deepStrictEqual(await client.getDatabaseNames(), ['iobroker']);
            }
        } finally {
            resetServer.closeAllConnections();
            await new Promise(resolve => resetServer.close(resolve));
        }
    });

    it('pings with the token', async function () {
        const hosts = await createClient().ping();
        assert.deepStrictEqual(hosts, [{ online: true }]);
        assert.strictEqual(calls[0].auth, 'Bearer apiv3_test');
    });

    it('reports an unreachable server as offline', async function () {
        const client = new DatabaseInfluxDB3x(
            { log, host: '127.0.0.1', port: 1, protocol: 'http', database: 'iobroker', requestTimeout: 1000 },
            { token: 'apiv3_test' },
        );
        assert.deepStrictEqual(await client.ping(), [{ online: false }]);
        assert.strictEqual(client.getHostsAvailable(), 0);
    });

    it('lists and creates databases via /api/v3/configure/database', async function () {
        const client = createClient();
        assert.deepStrictEqual(await client.getDatabaseNames(), ['_internal', 'iobroker']);
        await client.createDatabase('other');
        const create = calls.find(call => call.method === 'POST');
        assert.strictEqual(create.path, '/api/v3/configure/database');
        assert.deepStrictEqual(JSON.parse(create.body), { db: 'other' });
    });

    it('reads the retention period', async function () {
        const rp = await createClient().getRetentionPolicyForDB('iobroker');
        assert.deepStrictEqual(rp, { name: 'autogen', time: 86400 });
    });

    it('sets the retention period, with fallback for older servers', async function () {
        const client = createClient();
        await client.applyRetentionPolicyToDB('iobroker', 86400);
        assert.ok(!calls.some(call => call.method === 'PUT'), 'unchanged period must not be written');

        await client.applyRetentionPolicyToDB('iobroker', 3600);
        const put = calls.find(call => call.method === 'PUT');
        assert.deepStrictEqual(JSON.parse(put.body), { db: 'iobroker', retention_period: '3600s' });

        calls = [];
        putSupported = false;
        await client.applyRetentionPolicyToDB('iobroker', 3600);
        const fallback = calls.find(call => call.path === '/api/v3/configure/database/retention_period');
        assert.strictEqual(fallback.method, 'POST');
        assert.strictEqual(fallback.query.get('duration'), '3600s');
    });

    it('writes escaped line protocol with millisecond precision', async function () {
        await createClient().writeSeries({
            'a b,c': [
                {
                    value: 'say "hi"',
                    time: 1700000000123,
                    from: 'system.adapter.x',
                    q: 0,
                    ack: false,
                    tags: { room: 'Living room' },
                },
            ],
            'n.0.v': [{ value: 21.5, time: 1700000000124, from: '', q: 0, ack: true }],
        });
        const write = calls.find(call => call.path === '/api/v3/write_lp');
        assert.strictEqual(write.query.get('db'), 'iobroker');
        assert.strictEqual(write.query.get('precision'), 'millisecond');
        assert.strictEqual(
            write.body,
            'a\\ b\\,c,room=Living\\ room value="say \\"hi\\"",from="system.adapter.x",q=0,ack=false 1700000000123\n' +
                'n.0.v value=21.5,q=0,ack=true 1700000000124',
        );
    });

    it('translates a type conflict into the 1.x wording the adapter understands', async function () {
        await assert.rejects(
            createClient().writePoint('javascript.0.test', { value: true, time: 1, from: '', q: 0, ack: true }),
            error => {
                assert.ok(error.message.includes('field type conflict'), error.message);
                assert.ok(error.message.includes('is type bool, already exists as type float'), error.message);
                return true;
            },
        );
    });

    it('returns the rows of one statement, and one list per statement for several', async function () {
        const client = createClient();
        const multi = await client.query('SELECT mean(value) AS val FROM "a.0.x";SELECT value FROM "a.0.x" LIMIT 1');
        assert.strictEqual(calls.filter(c => c.path === '/query').length, 2, 'one request per statement');
        assert.strictEqual(multi.length, 2);
        assert.strictEqual(multi[0][0].val, 1.5);
        assert.ok(multi[0][0].time instanceof Date);
        assert.strictEqual(multi[0][0].time.getTime(), 1700000000000);
        assert.strictEqual(multi[1][0].value, 1);

        const single = await client.query('SHOW RETENTION POLICIES ON "iobroker"');
        assert.strictEqual(single[0].duration, '24h0m0s');

        const call = calls.find(c => c.path === '/query');
        assert.strictEqual(new URLSearchParams(call.body).get('epoch'), 'ms');
        assert.strictEqual(new URLSearchParams(call.body).get('db'), 'iobroker');
    });

    it('splits statements only at real separators', function () {
        const split = DatabaseInfluxDB3x.splitStatements;
        assert.deepStrictEqual(split('SELECT 1 FROM "a";SELECT 2 FROM "b"'), ['SELECT 1 FROM "a"', 'SELECT 2 FROM "b"']);
        // leading/trailing separators as the history query builds them
        assert.deepStrictEqual(split(';SELECT value from "x" LIMIT 1;'), ['SELECT value from "x" LIMIT 1']);
        // semicolons inside identifiers, strings and regular expressions are no separators
        assert.deepStrictEqual(split('SELECT * FROM "my;id" WHERE "from" = \'a;b\''), [
            'SELECT * FROM "my;id" WHERE "from" = \'a;b\'',
        ]);
        assert.deepStrictEqual(split('SELECT count(value) FROM /a;b/;SHOW MEASUREMENTS'), [
            'SELECT count(value) FROM /a;b/',
            'SHOW MEASUREMENTS',
        ]);
        assert.deepStrictEqual(split('SELECT "x\\";y" FROM a'), ['SELECT "x\\";y" FROM a']);
        // a division is no regular expression
        assert.deepStrictEqual(split('SELECT value / 10 FROM "a";SELECT 1 FROM "b"'), [
            'SELECT value / 10 FROM "a"',
            'SELECT 1 FROM "b"',
        ]);
    });

    it('throws on a query error', async function () {
        await assert.rejects(createClient().query('BAD'), /error parsing query/);
    });

    it('collects statistics per measurement', async function () {
        const statistics = await createClient().getStatistics(0, Date.now());
        assert.strictEqual(statistics['a.0.x'].count, 5);
        assert.strictEqual(statistics['b.0.y'].count, 7);
        assert.strictEqual(statistics['a.0.x'].cardinality, null);
    });

    it('drops a measurement as table, but refuses to delete ranges', async function () {
        const client = createClient();
        await client.dropMeasurement('hm-rpc.0.ABC.1.TEMPERATURE');
        const drop = calls.find(call => call.method === 'DELETE');
        assert.strictEqual(drop.path, '/api/v3/configure/table');
        assert.strictEqual(drop.query.get('table'), 'hm-rpc.0.ABC.1.TEMPERATURE');
        assert.strictEqual(drop.query.get('hard_delete_at'), 'now');

        await assert.rejects(client.deleteData(0, 1, '', 'iobroker', ''), /cannot delete single values/);
    });
});
