/**
 * A user-defined function (UDF) for Dataflow template to process Kafka JSON messages.
 * This UDF parses the input JSON, extracts top-level fields, and converts any
 * nested values (like objects or arrays) into JSON strings.
 *
 * @param {string} inJson The input JSON string.
 * @return {string} The modified output JSON string.
 */
function process(inJson) {
  var obj = parseJsonSafe(inJson);
  var result = {};

  for (var key in obj) {
    if (obj.hasOwnProperty(key)) {
      var value = obj[key];
      if (value !== null && typeof value === 'object') {
        // Serialize objects/arrays back to a JSON string, restoring large
        // integers as bare number tokens (not quoted strings).
        result[key] = serializeValue(value);
      } else {
        result[key] = value;
      }
    }
  }

  // Use serializeValue for the top-level result as well, so any large integer
  // that appears at the top level is also written as a bare number token.
  return serializeValue(result);
}

// ── Large-integer-safe JSON parsing ──────────────────────────────────────────
//
// JSON.parse maps every number to a JS IEEE 754 double.  Integers larger than
// Number.MAX_SAFE_INTEGER (9 007 199 254 740 991, which is 16 digits) are
// silently rounded.  For example 901 807 788 764 659 968 (18 digits) becomes
// 901 807 788 764 660 000 — the last five digits are wrong.
//
// The fix is a pre-pass over the raw JSON text that wraps any bare integer of
// 16 or more digits in a distinguishable marker string *before* JSON.parse
// runs.  {16,} is a lower-bound quantifier — it matches 16, 17, 18, … digits,
// so the fix is general for any integer length that can lose precision.
// 15-digit integers are always safe (max value 999 999 999 999 999, which is
// less than MAX_SAFE_INTEGER).
//
// The companion serializeValue function detects the marker and writes the
// value back as a bare number token, so the type inside nested JSON strings
// remains "number" — the same as in the original message.

var BIGINT_S = '__bigint_s__'; // start marker  (pure printable ASCII, safe for JSON.parse)
var BIGINT_E = '__bigint_e__'; // end marker

/**
 * Parses JSON while preserving large integer precision.
 * Integers with 16+ digits are pre-quoted with a unique marker before
 * JSON.parse; serializeValue later strips the marker and emits a bare number.
 *
 * @param {string} jsonStr Raw JSON string.
 * @return {*} Parsed object with large integers stored as marker strings.
 */
function parseJsonSafe(jsonStr) {
  // The alternation ensures we never touch digits that are already inside a
  // quoted string value (first branch skips any JSON string it encounters).
  var safeJson = jsonStr.replace(
    /"(?:\\.|[^"\\])*"|(-?\d{16,})/g,
    function(match, number) {
      return number !== undefined
        ? '"' + BIGINT_S + number + BIGINT_E + '"'
        : match;
    }
  );
  return JSON.parse(safeJson);
}

/**
 * Custom JSON serializer.
 * Mirrors parseJsonSafe: strings that carry the BIGINT markers are written
 * back as bare number tokens so the output type stays "number", not "string".
 * All other values are serialized identically to JSON.stringify.
 *
 * @param {*} value The value to serialize.
 * @return {string} JSON fragment.
 */
function serializeValue(value) {
  if (value === null) return 'null';
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  if (typeof value === 'number') return String(value);
  if (typeof value === 'string') {
    if (value.indexOf(BIGINT_S) === 0 && value.slice(-BIGINT_E.length) === BIGINT_E) {
      // Was a large integer in the source JSON — emit as a bare number token.
      return value.slice(BIGINT_S.length, -BIGINT_E.length);
    }
    return JSON.stringify(value); // handles escaping of quotes, backslashes, etc.
  }
  if (Array.isArray(value)) {
    var items = [];
    for (var i = 0; i < value.length; i++) {
      items.push(serializeValue(value[i]));
    }
    return '[' + items.join(',') + ']';
  }
  // Plain object
  var pairs = [];
  for (var k in value) {
    if (value.hasOwnProperty(k)) {
      pairs.push(JSON.stringify(k) + ':' + serializeValue(value[k]));
    }
  }
  return '{' + pairs.join(',') + '}';
}
