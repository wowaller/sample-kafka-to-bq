/**
 * Local test for udf.js.
 * Run with:  node sample/test_udf.js
 */
'use strict';

var fs   = require('fs');
var path = require('path');

// ── load UDF ──────────────────────────────────────────────────────────────────
// new Function() runs in global scope so hoisted function declarations are
// reachable and can be returned as an object.
var udfCode = fs.readFileSync(path.join(__dirname, '..', 'udf.js'), 'utf8');
var udf = new Function(udfCode + '\nreturn { process: process, parseJsonSafe: parseJsonSafe, serializeValue: serializeValue };')();
var process       = udf.process;
var parseJsonSafe = udf.parseJsonSafe;
var serializeValue = udf.serializeValue;

// ── helpers ───────────────────────────────────────────────────────────────────
var PASS = '\x1b[32mPASS\x1b[0m';
var FAIL = '\x1b[31mFAIL\x1b[0m';
var failures = 0;

function assert(label, actual, expected) {
  if (actual === expected) {
    console.log(PASS + '  ' + label);
  } else {
    console.log(FAIL + '  ' + label);
    console.log('       expected: ' + JSON.stringify(expected));
    console.log('       actual:   ' + JSON.stringify(actual));
    failures++;
  }
}

// ── test 1: full pipeline on sample data ──────────────────────────────────────
// The key assertion: ID inside the "data" JSON string must be a bare number
// token (not a quoted string) AND must retain its full 18-digit precision.
(function testFullSampleData() {
  var rawJson = fs.readFileSync(path.join(__dirname, 'json.txt'), 'utf8');
  var output  = process(rawJson);
  var result  = JSON.parse(output);   // parse the UDF output envelope

  // "data" must be a JSON string (the array blob)
  assert('data field is a string', typeof result.data, 'string');

  // Check the raw text of result.data: ID must appear as a bare number, not quoted.
  // A quoted string would look like "ID":"901807788764659968"; bare looks like "ID":901807788764659968
  var hasQuotedId  = result.data.indexOf('"ID":"') !== -1;
  var hasBareId    = result.data.indexOf('"ID":901807788764659968') !== -1;
  assert('ID is a bare number (not quoted) in data blob', hasBareId,   true);
  assert('ID is NOT a quoted string in data blob',        hasQuotedId, false);

  // Round-trip through JSON.parse of the data blob to verify the number value
  // is exactly right.  JSON.parse of 901807788764659968 would round it, so we
  // use a regex to extract the raw token instead.
  var m = result.data.match(/"ID"\s*:\s*(\d+)/);
  assert('ID token in data blob', m && m[1], '901807788764659968');

  // Top-level primitive fields must survive unchanged.
  assert('table field correct',    result.table,    'CCM_OMS_ORDER');
  assert('database field correct', result.database, 'UAT_LITEMAIN');
  assert('type field correct',     result.type,     'INSERT');
  assert('isDdl field correct',    result.isDdl,    false);
  assert('ts is a number',         typeof result.ts, 'number');
  assert('ts value correct',       result.ts,        1773746472062);
})();

// ── test 2: small integers stay as numbers, not quoted ─────────────────────────
(function testSmallIntegersUnchanged() {
  var input  = '{"ts":1773746472062,"flag":false,"label":"ok"}';
  var output = process(input);
  var result = JSON.parse(output);

  assert('ts is a number (safe, 13 digits)',  typeof result.ts, 'number');
  assert('ts value',                          result.ts,        1773746472062);
  assert('flag is boolean',                   typeof result.flag, 'boolean');
  assert('flag value',                        result.flag,      false);
})();

// ── test 3: nested objects are still stringified ───────────────────────────────
(function testNestedObjectsStringified() {
  var input  = '{"table":"T","meta":{"key":"val","count":5}}';
  var output = process(input);
  var result = JSON.parse(output);

  assert('table is a string',   typeof result.table, 'string');
  assert('meta is a string',    typeof result.meta,  'string');
  var meta = JSON.parse(result.meta);
  assert('meta.key round-trips', meta.key,   'val');
  assert('meta.count round-trips as number', meta.count, 5);
})();

// ── test 4: large integer inside a nested object stays a bare number ───────────
(function testLargeIntInsideNestedObject() {
  var input  = '{"wrapper":{"ID":901807788764659968,"name":"test"}}';
  var output = process(input);
  var result = JSON.parse(output);

  assert('wrapper is a string',   typeof result.wrapper, 'string');

  // ID must appear as a bare number token, not a quoted string, inside wrapper.
  var hasBare   = result.wrapper.indexOf('"ID":901807788764659968') !== -1;
  var hasQuoted = result.wrapper.indexOf('"ID":"')                  !== -1;
  assert('ID is bare number inside wrapper string',     hasBare,   true);
  assert('ID is not a quoted string inside wrapper',    hasQuoted, false);

  var m = result.wrapper.match(/"ID"\s*:\s*(\d+)/);
  assert('ID token value in wrapper', m && m[1], '901807788764659968');
})();

// ── test 5: large integers at different digit lengths (generality check) ───────
(function testDifferentLengths() {
  // 16-digit number just above MAX_SAFE_INTEGER
  var n16 = '9007199254740993';     // 16 digits, > MAX_SAFE_INTEGER
  // 20-digit number
  var n20 = '12345678901234567890'; // 20 digits

  var input  = '{"a":' + n16 + ',"b":' + n20 + ',"c":12345}';
  var output = process(input);

  // a and b must appear as bare numbers in the output text
  var hasA = output.indexOf('"a":' + n16) !== -1;
  var hasB = output.indexOf('"b":' + n20) !== -1;
  assert('16-digit large int preserved as bare number', hasA, true);
  assert('20-digit large int preserved as bare number', hasB, true);

  // c (5 digits, safe) must remain a number
  var result = JSON.parse(output);
  assert('safe int c stays a number',  typeof result.c, 'number');
  assert('safe int c value correct',   result.c,        12345);
})();

// ── test 6: number already inside a quoted string is untouched ─────────────────
(function testNumbersInStringsUntouched() {
  // "code" is a genuine string value that happens to be all digits — must stay string.
  var input  = '{"code":"901807788764659968","id":901807788764659968}';
  var output = process(input);

  // "code" must remain quoted (it was a string field in the source)
  var codeQuoted = output.indexOf('"code":"901807788764659968"') !== -1;
  // "id" must be a bare number
  var idBare     = output.indexOf('"id":901807788764659968')     !== -1;
  assert('string field code stays quoted',         codeQuoted, true);
  assert('bare large int id stays an unquoted number', idBare, true);
})();

// ── test 7: null values pass through ──────────────────────────────────────────
(function testNullValues() {
  var input  = '{"old":null,"name":"Hans"}';
  var output = process(input);
  var result = JSON.parse(output);

  assert('null field stays null',   result.old,  null);
  assert('string field stays string', result.name, 'Hans');
})();

// ── test 8: parseJsonSafe unit test ───────────────────────────────────────────
(function testParseJsonSafeUnit() {
  // Large int must come back as a marked string (internal implementation detail)
  var parsed = parseJsonSafe('{"big":901807788764659968,"small":42}');
  assert('small int parses as JS number',       typeof parsed.small, 'number');
  assert('small int value correct',              parsed.small,        42);
  // The big int is a string internally (marked); just verify it is NOT 901807788764660000
  var bigAsString = String(parsed.big);
  assert('large int not rounded to 901807788764660000', bigAsString.indexOf('901807788764660000'), -1);
})();

// ── test 9: serializeValue restores bare number tokens ────────────────────────
(function testSerializeValueUnit() {
  // Round-trip a plain object containing a large int through parse+serialize
  var parsed = parseJsonSafe('[{"ID":901807788764659968}]');
  var text   = serializeValue(parsed);
  var hasBare = text.indexOf('"ID":901807788764659968') !== -1;
  assert('serializeValue emits bare number for large int', hasBare, true);
})();

// ── summary ───────────────────────────────────────────────────────────────────
console.log('');
if (failures === 0) {
  console.log('All tests passed.');
} else {
  console.log(failures + ' test(s) failed.');
  process.exit(1);
}
