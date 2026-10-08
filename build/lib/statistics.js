"use strict";
/**
 * Pure helpers behind the `getDpStatistics` and `cleanupOrphaned` messages.
 *
 * Kept out of `main.ts` so they can be unit tested: importing `main.ts` pulls in
 * `@iobroker/adapter-core`, which needs a js-controller installation that this repository does
 * not depend on. Shared in shape with the same file of ioBroker.sql, so the two statistics tabs
 * stay comparable - only the storage specific columns differ.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.classifyDatapoint = classifyDatapoint;
exports.summarize = summarize;
exports.selectForCleanup = selectForCleanup;
exports.cardinalityFromSeriesKeys = cardinalityFromSeriesKeys;
/**
 * Decide how a datapoint should be classified.
 *
 * @param objectExists whether the ioBroker object for this ID still exists
 * @param loggingEnabled whether this instance currently logs the ID
 */
function classifyDatapoint(objectExists, loggingEnabled) {
    if (!objectExists) {
        return 'objectMissing';
    }
    return loggingEnabled ? 'active' : 'loggingDisabled';
}
/**
 * Totals for the statistics table.
 *
 * `cardinality` is null as soon as one datapoint has none: a partial sum presented as a total
 * would understate the real footprint.
 *
 * @param stats the per-datapoint statistics
 */
function summarize(stats) {
    const byStatus = {
        active: { datapoints: 0, values: 0 },
        loggingDisabled: { datapoints: 0, values: 0 },
        objectMissing: { datapoints: 0, values: 0 },
    };
    let values = 0;
    let cardinality = 0;
    let cardinalityKnown = true;
    for (const stat of stats) {
        values += stat.count;
        byStatus[stat.status].datapoints++;
        byStatus[stat.status].values += stat.count;
        if (stat.cardinality === null) {
            cardinalityKnown = false;
        }
        else {
            cardinality += stat.cardinality;
        }
    }
    return {
        datapoints: stats.length,
        values,
        cardinality: cardinalityKnown ? cardinality : null,
        byStatus,
    };
}
/**
 * Pick the datapoints a cleanup run with this scope would remove.
 *
 * An empty scope selects `objectMissing` only. Defaulting to the safe half means a caller that
 * forgets to pass a scope deletes the data nobody can reach any more, not data someone may be
 * keeping on purpose. `active` datapoints are never selectable.
 *
 * @param stats the per-datapoint statistics
 * @param scope which statuses to include
 */
function selectForCleanup(stats, scope) {
    const includeMissing = scope?.objectMissing !== false;
    const includeDisabled = scope?.loggingDisabled === true;
    return stats.filter(stat => (stat.status === 'objectMissing' && includeMissing) ||
        (stat.status === 'loggingDisabled' && includeDisabled));
}
/**
 * Count the series of a measurement from the series keys InfluxDB 1.x reports.
 *
 * `SHOW SERIES` answers with one key per series, in the line protocol shape
 * `measurement,tag=value,tag=value`. The measurement itself may contain escaped commas, so the
 * key is split at the first comma that is not escaped - splitting naively would attribute the
 * series of `a\,b` to the measurement `a\`.
 *
 * @param seriesKeys the `key` column of `SHOW SERIES`
 */
function cardinalityFromSeriesKeys(seriesKeys) {
    const result = {};
    for (const key of seriesKeys) {
        if (!key) {
            continue;
        }
        let measurement = key;
        for (let i = 0; i < key.length; i++) {
            if (key[i] === '\\') {
                i++; // the next character is escaped, so it can not end the measurement
            }
            else if (key[i] === ',') {
                measurement = key.substring(0, i);
                break;
            }
        }
        measurement = measurement.replace(/\\(.)/g, '$1');
        result[measurement] = (result[measurement] || 0) + 1;
    }
    return result;
}
