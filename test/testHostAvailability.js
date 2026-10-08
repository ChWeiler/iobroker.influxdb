const assert = require('node:assert');
const { Database } = require('../build/lib/Database');

const log = { debug: () => {}, info: () => {}, warn: () => {}, error: () => {}, silly: () => {} };

/** The abstract members are never called here, only the connection tracking of the base class */
class TestDatabase extends Database {
    constructor() {
        super({ log, host: 'localhost', port: 8086, protocol: 'http', database: 'iobroker', requestTimeout: 1000 });
    }

    run(action) {
        return this.trackConnection(action);
    }
}

function connectionError() {
    // what Node hands over when InfluxDB is switched off and the host is "localhost"
    return Object.assign(new AggregateError([new Error('connect ECONNREFUSED 127.0.0.1:8086')]), {
        code: 'ECONNREFUSED',
    });
}

describe('Test host availability', function () {
    it('starts with an available host', function () {
        assert.strictEqual(new TestDatabase().getHostsAvailable(), 1);
    });

    it('takes the host out of rotation after a connection error', async function () {
        const db = new TestDatabase();
        await assert.rejects(() =>
            db.run(() => {
                throw connectionError();
            }),
        );
        assert.strictEqual(db.getHostsAvailable(), 0, 'the adapter must buffer instead of writing point by point');
    });

    it('keeps the host after a rejected point', async function () {
        const db = new TestDatabase();
        await assert.rejects(() =>
            db.run(() => {
                throw new Error('partial write: field type conflict: input field "value" ...');
            }),
        );
        // a bad point says nothing about the reachability of the server, the write path has to go on
        assert.strictEqual(db.getHostsAvailable(), 1);
    });

    it('brings the host back after a successful request', async function () {
        const db = new TestDatabase();
        await assert.rejects(() =>
            db.run(() => {
                throw connectionError();
            }),
        );
        assert.strictEqual(db.getHostsAvailable(), 0);

        // markHostAvailable() is what ping() calls when the host answers again
        db.markHostAvailable();
        assert.strictEqual(db.getHostsAvailable(), 1);
        assert.strictEqual(await db.run(async () => 'ok'), 'ok');
        assert.strictEqual(db.getHostsAvailable(), 1);
    });

    it('retries the host after the backoff', async function () {
        const db = new TestDatabase();
        await assert.rejects(() =>
            db.run(() => {
                throw connectionError();
            }),
        );
        assert.strictEqual(db.getHostsAvailable(), 0);

        // without an expiry the adapter would never write again: only a request could clear the state,
        // and no request is sent while the host counts as unavailable
        db.hostUnavailableSince = Date.now() - 60000;
        assert.strictEqual(db.getHostsAvailable(), 1);
    });
});
