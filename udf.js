/**
 * A user-defined function (UDF) for Dataflow template to process Kafka JSON messages.
 * This UDF parses the input JSON, extracts top-level fields, and converts any
 * nested values (like objects or arrays) into JSON strings.
 *
 * @param {string} inJson The input JSON string.
 * @return {string} The modified output JSON string.
 */
function process(inJson) {
  var obj = JSON.parse(inJson);
  var result = {};

  for (var key in obj) {
    if (obj.hasOwnProperty(key)) {
        var value = obj[key];
        // If the value is an object or array (and not null), stringify it
        if (value !== null && typeof value === 'object') {
            result[key] = JSON.stringify(value);
        } else {
            result[key] = value;
        }
    }
  }
  
  return JSON.stringify(result);
}
