"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Database = exports.MAX_INFLUX_TIME = exports.MIN_INFLUX_TIME = void 0;
exports.escapeInfluxQLIdentifier = escapeInfluxQLIdentifier;
exports.escapeFluxString = escapeFluxString;
const errors_1 = require("./errors");
/**
 * How long a host counts as unusable after a connection error.
 *
 * The 1.x driver pool takes a host out of rotation the same way, and without an expiry the adapter
 * would keep buffering forever, because only a successful request could ever bring the host back -
 * and no request is sent while the host counts as unavailable. Roughly the reconnect interval.
 */
const HOST_UNAVAILABLE_TIME = 10_000;
/** Oldest timestamp the adapter looks at. It never writes a point before the epoch */
exports.MIN_INFLUX_TIME = 0;
/** Newest timestamp a Flux range may stop at. InfluxDB cannot store anything after it */
exports.MAX_INFLUX_TIME = Date.UTC(2262, 3, 11);
/**
 * Escape an InfluxQL identifier (e.g. a measurement or database name) that is placed inside
 * double quotes in a query, to prevent InfluxQL injection via the ioBroker state id.
 */
function escapeInfluxQLIdentifier(id) {
    return String(id).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}
/**
 * Escape a value that is placed inside a Flux double-quoted string literal, to prevent Flux
 * injection (incl. Flux string interpolation via ${...}) via the ioBroker state id or db name.
 */
function escapeFluxString(value) {
    return String(value).replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\$\{/g, '\\${');
}
class Database {
    log;
    host;
    port;
    protocol;
    database;
    requestTimeout;
    /** 0 if the host is usable, otherwise the time of the connection error that took it out of rotation */
    hostUnavailableSince = 0;
    constructor(options) {
        this.log = options.log;
        this.host = options.host;
        this.port = options.port;
        this.protocol = options.protocol;
        this.database = options.database;
        this.requestTimeout = options.requestTimeout;
    }
    /**
     * Is the InfluxDB host currently usable?
     *
     * The adapter uses this to decide whether it may write at all or has to buffer: writing point by
     * point against a server that is not reachable produces one error (and one log line) per point.
     * A host that failed with a connection error is taken out of rotation for a short while and is
     * then tried again - exactly what the connection pool of the 1.x driver does internally.
     *
     * @returns 1 while the host may be used, 0 while it is known to be unreachable
     */
    getHostsAvailable() {
        if (this.hostUnavailableSince && Date.now() - this.hostUnavailableSince < HOST_UNAVAILABLE_TIME) {
            return 0;
        }
        // the backoff is over: give the host another try
        this.hostUnavailableSince = 0;
        return 1;
    }
    /** Report that the host answered, so it counts as usable again */
    markHostAvailable() {
        this.hostUnavailableSince = 0;
    }
    /** Report that the host is not reachable, so the adapter buffers instead of writing point by point */
    markHostUnavailable() {
        this.hostUnavailableSince = Date.now();
    }
    /**
     * Run a request and remember whether the host answered.
     *
     * Only connection errors change the state: a rejected point ("field type conflict", "unauthorized")
     * says nothing about the reachability of the server and must not stop the adapter from writing.
     *
     * @param action the request to execute
     * @returns whatever the request returned
     */
    async trackConnection(action) {
        try {
            const result = await action();
            this.markHostAvailable();
            return result;
        }
        catch (error) {
            if ((0, errors_1.isConnectionError)(error)) {
                this.markHostUnavailable();
            }
            throw error;
        }
    }
    async queries(queries) {
        const collectedRows = [];
        let success = false;
        const errors = [];
        for (const query of queries) {
            try {
                const rows = await this.query(query);
                success = true;
                collectedRows.push(rows);
            }
            catch (error) {
                this.log.warn(`Error in query "${query}": ${(0, errors_1.formatError)(error)}`);
                errors.push(error);
                collectedRows.push([]);
            }
        }
        if (errors.length) {
            throw new Error(`${errors.length} Error happened while processing ${queries.length} queries`);
        }
        return success ? collectedRows : null;
    }
    calculateShardGroupDuration(retentionTime) {
        // in seconds
        // Shard Group Duration according to official Influx recommendations
        if (!retentionTime) {
            // infinite
            return 604800; // 7 days
        }
        if (retentionTime < 172800) {
            // < 2 days
            return 3600; // 1 hour
        }
        if (retentionTime >= 172800 && retentionTime <= 15811200) {
            // >= 2 days, <= 6 months (~182 days)
            return 86400; // 1 day
        }
        // > 6 months
        return 604800; // 7 days
    }
}
exports.Database = Database;
