/**
 * Tests for udf_text_approach.js.
 * Run with:  node sample/test_udf_text_approach.js
 *
 * These tests also document the known trade-off: large integers that are bare
 * number tokens in the source become JSON strings inside nested blobs.
 */
'use strict';

var fs   = require('fs');
var path = require('path');

var udfCode = fs.readFileSync(path.join(__dirname, '..', 'udf_text_approach.js'), 'utf8');
var udf = new Function(udfCode + '\nreturn { process: process, parseJsonSafe: parseJsonSafe };')();
var process       = udf.process;
var parseJsonSafe = udf.parseJsonSafe;

// ── helpers ───────────────────────────────────────────────────────────────────
var PASS = '\x1b[32mPASS\x1b[0m';
var FAIL = '\x1b[31mFAIL\x1b[0m';
var NOTE = '\x1b[33mNOTE\x1b[0m';
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

function note(label) {
  console.log(NOTE + '  ' + label);
}

// ── test 1: full pipeline on sample data ──────────────────────────────────────
(function testFullSampleData() {
  console.log('\n-- test 1: full sample data');
  var rawJson = fs.readFileSync(path.join(__dirname, 'json.txt'), 'utf8');
  var output  = process(rawJson);
  var result  = JSON.parse(output);

  assert('data field is a string (nested blob)', typeof result.data, 'string');

  // Precision: the 18-digit ID must appear with its exact digits in the blob.
  var m = result.data.match(/"ID"\s*:\s*"?([\d]+)"?/);
  assert('ID digits are exact (901807788764659968)', m && m[1], '901807788764659968');

  // Trade-off: ID is a quoted string inside the blob, not a bare number.
  var idIsString = result.data.indexOf('"ID":"901807788764659968"') !== -1;
  var idIsBare   = result.data.indexOf('"ID":901807788764659968')   !== -1;
  assert('ID is a quoted string inside data blob (known trade-off)', idIsString, true);
  note('ID is NOT a bare number inside data blob — type changed from number to string');
  assert('confirm: ID is not a bare number', idIsBare, false);

  // Top-level primitive fields must survive unchanged.
  assert('table correct',    result.table,    'CCM_OMS_ORDER');
  assert('database correct', result.database, 'UAT_LITEMAIN');
  assert('type correct',     result.type,     'INSERT');
  assert('isDdl correct',    result.isDdl,    false);
  assert('ts is a number',   typeof result.ts, 'number');
  assert('ts value correct', result.ts,        1773746472062);
})();

// ── test 2: string field whose value is all digits stays a string ──────────────
(function testStringFieldWithDigits() {
  console.log('\n-- test 2: string fields with numeric-looking values');

  // 12-digit phone — below the 16-digit threshold, untouched by the regex.
  var c1 = JSON.parse(process('{"data":[{"SENDER_TELPHONE":"491701234567"}]}'));
  var d1 = JSON.parse(c1.data);
  assert('12-digit phone string stays string', typeof d1[0].SENDER_TELPHONE, 'string');
  assert('12-digit phone value correct',       d1[0].SENDER_TELPHONE, '491701234567');

  // 18-digit string field — above threshold, but protected by the first regex branch.
  var c2 = JSON.parse(process('{"code":"901807788764659968","id":901807788764659968}'));
  assert('18-digit string field stays string', typeof c2.code, 'string');
  assert('18-digit string field value exact',  c2.code, '901807788764659968');
  note('code stays "string" because the first regex branch skips quoted values');
})();

// ── test 3: safe (small) integers are unaffected ───────────────────────────────
(function testSafeIntegers() {
  console.log('\n-- test 3: safe integers');
  var result = JSON.parse(process('{"ts":1773746472062,"count":5,"flag":false}'));
  assert('ts is a number',    typeof result.ts, 'number');
  assert('ts value correct',  result.ts,        1773746472062);
  assert('count is a number', typeof result.count, 'number');
  assert('flag is a boolean', typeof result.flag, 'boolean');
})();

// ── test 4: generality — different digit lengths above the threshold ───────────
(function testDifferentLengths() {
  console.log('\n-- test 4: large integers at different digit lengths');
  var n16 = '9007199254740993';     // 16 digits, just above MAX_SAFE_INTEGER
  var n20 = '12345678901234567890'; // 20 digits
  var result = JSON.parse(process('{"a":' + n16 + ',"b":' + n20 + ',"c":99}'));

  // a and b survive as strings with exact digits (top-level, not nested)
  assert('16-digit int exact value', result.a, n16);
  assert('20-digit int exact value', result.b, n20);
  assert('safe int stays number',    typeof result.c, 'number');
  note('top-level large ints arrive as strings — type changed from number to string');
})();

// ── test 5: null values pass through ──────────────────────────────────────────
(function testNullValues() {
  console.log('\n-- test 5: null values');
  var result = JSON.parse(process('{"old":null,"name":"Hans"}'));
  assert('null field stays null',     result.old,  null);
  assert('string field stays string', result.name, 'Hans');
})();

// ── side-by-side comparison note ─────────────────────────────────────────────
console.log('\n-- comparison with udf.js');
(function compareWithFullUdf() {
  var udfFullCode = fs.readFileSync(path.join(__dirname, '..', 'udf.js'), 'utf8');
  var udfFull = new Function(udfFullCode + '\nreturn { process: process };')();

  var input = '{"data":[{"ID":901807788764659968,"name":"test"}]}';

  var outText = process(input);
  var outFull = udfFull.process(input);

  var textData = JSON.parse(JSON.parse(outText).data);
  var fullData = JSON.parse(JSON.parse(outFull).data);

  console.log('  text approach — ID type inside data blob: ' + typeof textData[0].ID
    + '  value: ' + textData[0].ID);
  note('text approach: ID type is string, value is exact string "901807788764659968"');

  var fullRaw = JSON.parse(outFull).data;
  var fullIdBare = fullRaw.indexOf('"ID":901807788764659968') !== -1;
  console.log('  udf.js        — ID is bare number in blob: ' + fullIdBare
    + '  (precision-exact bare number token)');
  note('udf.js: ID type stays number, value is exact bare number 901807788764659968');
})();

// ── summary ───────────────────────────────────────────────────────────────────
console.log('');
if (failures === 0) {
  console.log('All tests passed.');
} else {
  console.log(failures + ' test(s) failed.');
  process.exit(1);
}
