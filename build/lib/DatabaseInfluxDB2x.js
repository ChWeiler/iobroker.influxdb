"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Database_1 = require("./Database");
const influxdb_client_1 = require("@influxdata/influxdb-client");
const influxdb_client_apis_1 = require("@influxdata/influxdb-client-apis");
// Influx 2.x auth requires token, not user/pw
class DatabaseInfluxDB2x extends Database_1.Database {
    connection;
    path;
    token;
    organization;
    useTags;
    validateSSL;
    queryApi;
    writeApi;
    bucketsApi;
    orgsApi;
    healthApi;
    deleteApi;
    organizationId = '';
    bucketIds = {};
    constructor(options, db2xOptions) {
        super(options);
        this.path = db2xOptions.path;
        this.token = db2xOptions.token;
        this.organization = db2xOptions.organization;
        this.useTags = db2xOptions.useTags || false;
        this.validateSSL = db2xOptions.validateSSL;
        this.connect();
    }
    connect() {
        const url = `${this.protocol}://${this.host}:${this.port}/${this.path || ''}`;
        this.log.debug(`Connect InfluxDB2: ${url} [${this.database}]`);
        this.connection = new influxdb_client_1.InfluxDB({
            url,
            token: this.token,
            timeout: this.requestTimeout,
            transportOptions: { rejectUnauthorized: this.validateSSL },
        });
        this.queryApi = this.connection.getQueryApi(this.organization);
        this.writeApi = this.connection.getWriteApi(this.organization, this.database, 'ms');
        this.bucketsApi = new influxdb_client_apis_1.BucketsAPI(this.connection);
        this.orgsApi = new influxdb_client_apis_1.OrgsAPI(this.connection);
        this.healthApi = new influxdb_client_apis_1.HealthAPI(this.connection);
        this.deleteApi = new influxdb_client_apis_1.DeleteAPI(this.connection);
    }
    async deleteData(start, stop, org, dbName, query) {
        await this.deleteApi.postDelete({
            org,
            bucket: dbName,
            body: {
                start: new Date(start).toISOString(),
                stop: new Date(stop).toISOString(),
                predicate: query,
            },
        });
    }
    async getDatabaseNames() {
        this.log.debug(`Organization being checked: ${this.organization}`);
        const organizations = await this.trackConnection(() => this.orgsApi.getOrgs({ org: this.organization }));
        this.log.debug(`Organizations: ${JSON.stringify(organizations)}`);
        if (!organizations?.orgs?.length) {
            throw new Error('No organizations exists or the token do not have proper permissions. Please check the token (see Readme for Tipps)!');
        }
        this.organizationId = organizations.orgs[0].id || '';
        if (!this.organizationId) {
            throw new Error(`Could not find organization ID for organization "${this.organization}"`);
        }
        const buckets = await this.trackConnection(() => this.bucketsApi.getBuckets({ orgID: this.organizationId }));
        this.log.debug(`Buckets: ${JSON.stringify(buckets)}`);
        const foundDatabases = [];
        buckets.buckets?.forEach(bucket => {
            if (bucket.name && bucket.id) {
                foundDatabases.push(bucket.name);
                this.bucketIds[bucket.name] = bucket.id;
            }
        });
        return foundDatabases;
    }
    async getRetentionPolicyForDB(dbName) {
        this.log.debug(`Getting retention policy for ${dbName}`);
        try {
            const bucketData = await this.bucketsApi.getBucketsID({ bucketID: this.bucketIds[dbName] });
            // A bucket without any retention rule keeps the data forever, which is "0" for us
            const everySeconds = bucketData.retentionRules?.[0]?.everySeconds ?? 0;
            this.log.debug(`Found retention policy: ${everySeconds} seconds`);
            return { time: everySeconds, name: dbName };
        }
        catch (error) {
            this.log.error(`Cannot read the retention policy for ${dbName}: ${error}`);
            return null;
        }
    }
    async applyRetentionPolicyToDB(dbName, retention) {
        // Skip the PATCH request if the retention is already as desired (avoids an API call on every connect)
        const currentRetention = await this.getRetentionPolicyForDB(dbName);
        if (currentRetention && currentRetention.time === retention) {
            this.log.debug(`Retention policy for ${dbName} remains unchanged.`);
            return;
        }
        const shardGroupDuration = this.calculateShardGroupDuration(retention);
        this.log.info(`Applying retention policy for ${dbName} to ${!retention ? 'infinity' : `${retention} seconds`}. Shard Group Duration (calculated): ${shardGroupDuration} seconds`);
        await this.bucketsApi.patchBucketsID({
            bucketID: this.bucketIds[dbName],
            body: {
                retentionRules: [
                    {
                        type: 'expire',
                        everySeconds: retention,
                        shardGroupDurationSeconds: shardGroupDuration,
                    },
                ],
            },
        });
    }
    async createDatabase(dbname) {
        this.log.info(`Creating database ${dbname} for orgId ${this.organizationId}`);
        const newBucket = await this.bucketsApi.postBuckets({
            body: {
                orgID: this.organizationId,
                name: dbname,
            },
        });
        this.bucketIds[dbname] = newBucket.id || '';
    }
    async dropDatabase(dbname) {
        this.log.info(`Dropping database ${dbname} for orgId "${this.organizationId}"`);
        await this.bucketsApi.deleteBucketsID({ bucketID: this.bucketIds[dbname] });
    }
    async writeSeries(series) {
        this.log.debug(`Write series: ${JSON.stringify(series)}`);
        const points = [];
        for (const [pointId, valueSets] of Object.entries(series)) {
            valueSets.forEach(value => {
                points.push(this.stateValueToPoint(pointId, value));
            });
        }
        this.writeApi.writePoints(points);
        await this.trackConnection(() => this.writeApi.flush());
        this.log.debug(`Points written to ${this.database}`);
    }
    async writePoints(seriesId, pointsToSend) {
        this.log.debug(`Write Points: ${seriesId} pointsToSend:${JSON.stringify(pointsToSend)}`);
        const points = [];
        pointsToSend.forEach(value => {
            points.push(this.stateValueToPoint(seriesId, value));
        });
        this.writeApi.writePoints(points);
        await this.trackConnection(() => this.writeApi.flush());
        this.log.debug(`Points written to ${this.database}`);
    }
    async writePoint(seriesId, value) {
        this.log.debug(`Write Point: ${seriesId} values:${JSON.stringify(value)}`);
        this.writeApi.writePoint(this.stateValueToPoint(seriesId, value));
        await this.trackConnection(() => this.writeApi.flush());
        this.log.debug(`Point written to ${this.database}`);
    }
    stateValueToPoint(pointName, stateValue) {
        let point = null;
        if (this.useTags) {
            point = new influxdb_client_1.Point(pointName)
                .timestamp(stateValue.time)
                .tag('q', String(stateValue.q))
                .tag('ack', String(stateValue.ack))
                .tag('from', stateValue.from);
        }
        else {
            point = new influxdb_client_1.Point(pointName)
                .timestamp(stateValue.time)
                .floatField('q', stateValue.q)
                .booleanField('ack', stateValue.ack)
                .stringField('from', stateValue.from);
        }
        // custom tags are independent of `useTags`: they never collide with q/ack/from (reserved names)
        if (stateValue.tags) {
            for (const [name, value] of Object.entries(stateValue.tags)) {
                point.tag(name, value);
            }
        }
        switch (typeof stateValue.value) {
            case 'boolean':
                point.booleanField('value', stateValue.value);
                break;
            case 'number':
                point.floatField('value', parseFloat(stateValue.value));
                break;
            case 'string':
            default:
                point.stringField('value', stateValue.value);
                break;
        }
        return point;
    }
    query(query) {
        this.log.debug(`Query to execute: ${query}`);
        return this.trackConnection(() => new Promise((resolve, reject) => {
            const rows = [];
            this.queryApi.queryRows(query, {
                next(row, tableMeta) {
                    const fields = tableMeta.toObject(row);
                    // Columns "_time" and "_value" are mapped to "time" and "value" for backwards compatibility
                    if (fields._time !== null) {
                        fields.time = fields._time;
                    }
                    rows.push(fields);
                },
                error(error) {
                    // Ignore errors that are related to an empty range. The handling of this currently seems inconsistent for flux.
                    // See also https://github.com/influxdata/flux/issues/3543
                    if (error.message.match('.*cannot query an empty range.*')) {
                        resolve(rows);
                    }
                    else {
                        reject(error);
                    }
                },
                complete() {
                    resolve(rows);
                },
            });
        }));
    }
    async getStatistics(start, stop) {
        // One script with three yields, so the whole table needs a single request. `first()` and
        // `last()` keep the natural grouping of `from()`, which means one row per series: their
        // row count per measurement is its series cardinality, and the smallest/largest `_time`
        // among them is the oldest/newest value. That saves a fourth pass for the cardinality and
        // avoids sorting, which would have to hold a whole measurement in memory.
        const fluxQuery = `base = from(bucket: "${(0, Database_1.escapeFluxString)(this.database)}")
    |> range(start: ${new Date(start).toISOString()}, stop: ${new Date(stop).toISOString()})
    |> filter(fn: (r) => r["_field"] == "value")

base |> group(columns: ["_measurement"]) |> count(column: "_value") |> yield(name: "count")
base |> first() |> yield(name: "first")
base |> last() |> yield(name: "last")`;
        const rows = await this.query(fluxQuery);
        const statistics = {};
        const entryOf = (name) => (statistics[name] ||= { count: 0, firstTs: null, lastTs: null, cardinality: null });
        for (const row of rows || []) {
            const name = row._measurement;
            if (!name) {
                continue;
            }
            const entry = entryOf(name);
            if (row.result === 'count') {
                entry.count = Number(row._value) || 0;
                continue;
            }
            const ts = row._time ? new Date(row._time).getTime() : NaN;
            if (!isFinite(ts)) {
                continue;
            }
            if (row.result === 'first') {
                // one row per series: count them for the cardinality, keep the oldest timestamp
                entry.cardinality = (entry.cardinality || 0) + 1;
                entry.firstTs = entry.firstTs === null ? ts : Math.min(entry.firstTs, ts);
            }
            else if (row.result === 'last') {
                entry.lastTs = entry.lastTs === null ? ts : Math.max(entry.lastTs, ts);
            }
        }
        return statistics;
    }
    async dropMeasurement(measurement) {
        // 2.x has no "drop measurement": deleting every point of it over the whole storable range
        // is the equivalent, the series disappear from the index with their last point
        await this.deleteData(Database_1.MIN_INFLUX_TIME, Database_1.MAX_INFLUX_TIME, this.organization, this.database, `_measurement="${(0, Database_1.escapeFluxString)(measurement)}"`);
    }
    async getMetaDataStorageType() {
        const bucket = (0, Database_1.escapeFluxString)(this.database);
        const queries = [
            `import "influxdata/influxdb/schema" schema.tagKeys(bucket: "${bucket}")`,
            `import "influxdata/influxdb/schema" schema.fieldKeys(bucket: "${bucket}")`,
        ];
        const result = await this.queries(queries);
        if (!result) {
            throw new Error('Could not determine metadata storage type');
        }
        let storageType = 'none';
        this.log.debug(`Result of metadata storage type check: ${JSON.stringify(result)}`);
        for (let i = 0; i <= 1; i++) {
            result[i].forEach(row => {
                switch (row._value) {
                    case 'q':
                    case 'ack':
                    case 'from':
                        storageType = !i ? 'tags' : 'fields';
                        return;
                }
            });
        }
        return storageType;
    }
    async ping() {
        // can't do much with interval, so ignoring it for compatibility reasons
        const result = await this.trackConnection(() => this.healthApi.getHealth());
        if (result.status === 'pass') {
            this.markHostAvailable();
            return [{ online: true }];
        }
        // reachable, but not healthy - treat it like a dead host, the adapter reconnects
        this.markHostUnavailable();
        return [{ online: false }];
    }
}
exports.default = DatabaseInfluxDB2x;
