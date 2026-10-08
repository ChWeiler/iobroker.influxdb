const assert = require('node:assert');
const { sendResponse } = require('@iobroker/aggregate');

// The response path of getHistory: everything the adapter does after the database has answered.
// Regression guard for ioBroker/ioBroker.influxdb#261 ("count: 1 together with removeBorderValues
// returns an empty result") - the part of the pipeline that could drop the values is here, the query
// itself is covered by the adapter test suites.

const START = 1652998792726; // 2022-05-19T22:19:52.726Z
const END = 1653037312726; // 2022-05-20T09:01:52.726Z

/** Collects what the adapter would send back to the caller */
function runSendResponse(options, data) {
    let answer = null;
    const adapter = {
        log: { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} },
        sendTo: (from, command, message) => {
            answer = message;
        },
    };
    const msg = { from: 'system.adapter.javascript.0', command: 'getHistory', callback: {} };

    sendResponse(adapter, msg, 'influxdb.0.rain', { ...options }, data, Date.now());
    return answer;
}

/** The options the adapter builds for `aggregate: 'none'` (see getHistoryV2 in src/main.ts) */
function rawOptions(overrides) {
    return {
        start: START,
        end: END,
        count: 1,
        aggregate: 'none',
        limit: 2000,
        addId: false,
        ignoreNull: true,
        returnNewestEntries: true,
        removeBorderValues: true,
        ...overrides,
    };
}

describe('Test getHistory response for count + removeBorderValues (#261)', function () {
    it('returns the value if a single datapoint lies inside the range', function () {
        const answer = runSendResponse(rawOptions(), [{ ts: 1653031493307, val: 1.5, ack: true }]);
        assert.strictEqual(answer.result.length, 1, JSON.stringify(answer));
        assert.strictEqual(answer.result[0].val, 1.5);
        assert.strictEqual(answer.result[0].ts, 1653031493307);
    });

    it('returns the newest value if more values than requested lie inside the range', function () {
        const answer = runSendResponse(rawOptions(), [
            { ts: START + 1000, val: 1 },
            { ts: START + 2000, val: 2 },
            { ts: END - 1000, val: 3 },
        ]);
        assert.strictEqual(answer.result.length, 1, JSON.stringify(answer));
        assert.strictEqual(answer.result[0].val, 3, 'returnNewestEntries must keep the newest value');
    });

    it('keeps values exactly on the borders of the range', function () {
        const answer = runSendResponse(rawOptions({ count: 5 }), [
            { ts: START, val: 1 },
            { ts: END, val: 2 },
        ]);
        assert.strictEqual(answer.result.length, 2, JSON.stringify(answer));
    });

    it('does not return values outside the range when removeBorderValues is set', function () {
        // This is what removeBorderValues means, and what ioBroker/ioBroker.influxdb#438 would have
        // changed: it restored the "closest" of these values instead of answering with an empty result.
        const answer = runSendResponse(rawOptions(), [
            { ts: START - 60000, val: 41 },
            { ts: END + 60000, val: 42 },
        ]);
        assert.deepStrictEqual(answer.result, [], JSON.stringify(answer));
    });

    it('answers with an empty result if the database has no data at all', function () {
        const answer = runSendResponse(rawOptions(), []);
        assert.deepStrictEqual(answer.result, []);
    });
});
