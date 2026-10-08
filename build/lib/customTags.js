"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getInvalidTagNameReason = getInvalidTagNameReason;
exports.normalizeCustomTags = normalizeCustomTags;
/**
 * Tag names that must not be used as custom tags.
 *
 * `value`, `q`, `ack` and `from` are written by the adapter itself (as fields or as tags, see `usetags`),
 * a tag with the same name would collide with them. `time` is reserved by InfluxQL, and every name that
 * starts with `_` is reserved by InfluxDB 2.x (`_measurement`, `_field`, `_value`, `_time`, ...).
 */
const RESERVED_TAG_NAMES = ['value', 'q', 'ack', 'from', 'time'];
/**
 * Why a tag name can not be used, or `null` if it is fine.
 *
 * @param name the tag name, already trimmed
 */
function getInvalidTagNameReason(name) {
    if (!name) {
        return 'empty name';
    }
    if (RESERVED_TAG_NAMES.includes(name.toLowerCase())) {
        return 'reserved name';
    }
    if (name.startsWith('_')) {
        return 'names starting with "_" are reserved by InfluxDB';
    }
    return null;
}
/** Admin delivers texts, but a number typed into a script via `enableHistory` is fine as well */
function toText(value) {
    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
        return String(value).trim();
    }
    return '';
}
/**
 * Convert the custom tags as stored in the object (an array of `{name, value}` rows from the admin table)
 * into the tags to write.
 *
 * Rows that can not be written are skipped and reported in `invalid`: InfluxDB drops tags with an empty
 * value, so empty names or values (e.g. a row that was added in the admin but never filled) are ignored,
 * and so are reserved names. If a name occurs more than once, the last row wins.
 *
 * @param rows the `customTags` attribute of the custom config: either the array of rows from the admin
 *        table, or an already normalized `name -> value` map. Anything else means "no tags"
 */
function normalizeCustomTags(rows) {
    const tags = {};
    const invalid = [];
    let rowsToCheck;
    if (Array.isArray(rows)) {
        rowsToCheck = rows;
    }
    else if (rows && typeof rows === 'object') {
        // `getEnabledDPs` reports the normalized map, not the rows of the admin table. Accept it here
        // as well, so a configuration read from there and written back through `enableHistory` keeps
        // its tags instead of losing them silently
        rowsToCheck = Object.entries(rows).map(([name, value]) => ({ name, value }));
    }
    else {
        return { tags, invalid };
    }
    for (const row of rowsToCheck) {
        if (!row || typeof row !== 'object') {
            continue;
        }
        const name = toText(row.name);
        const value = toText(row.value);
        if (!name && !value) {
            // an empty row, as the admin adds it with "+"
            continue;
        }
        const reason = getInvalidTagNameReason(name);
        if (reason) {
            invalid.push(`"${name}" (${reason})`);
            continue;
        }
        if (!value) {
            invalid.push(`"${name}" (empty value)`);
            continue;
        }
        tags[name] = value;
    }
    return { tags, invalid };
}
