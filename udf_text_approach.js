/**
 * UDF — text approach.
 *
 * The Dataflow template already hands us the raw Kafka message as a plain
 * string, so we are in the same position as a browser caller that used
 * response.text() instead of response.json().  We pre-process the raw text
 * with a regex before JSON.parse ever runs, avoiding IEEE 754 precision loss
 * for large integers.
 *
 * Trade-off vs udf.js: large integers that appear as bare number tokens in
 * the source (e.g. ID: 901807788764659968) are converted to JSON strings
 * (e.g. "901807788764659968") inside the nested blob.  Their values are
 * exact; only the JSON type changes from number to string.
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
        result[key] = JSON.stringify(value);
      } else {
        result[key] = value;
      }
    }
  }

  return JSON.stringify(result);
}

/**
 * Parses JSON while preserving large integer precision.
 *
 * Works on the raw text (the "text approach"): a single regex pass quotes any
 * bare integer of 16 or more digits before JSON.parse runs.  Digits that are
 * already inside a quoted string are skipped by the first branch of the
 * alternation and are never modified.
 *
 * Result: large integers arrive as JS strings instead of imprecise JS numbers.
 * Their string values are exact.
 *
 * @param {string} jsonStr Raw JSON string.
 * @return {*} Parsed object.
 */
function parseJsonSafe(jsonStr) {
  return JSON.parse(
    jsonStr.replace(
      /"(?:\\.|[^"\\])*"|(-?\d{16,})/g,
      function(match, number) {
        return number !== undefined ? '"' + number + '"' : match;
      }
    )
  );
}
