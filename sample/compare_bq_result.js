/**
 * Compares original data[0] from json.txt with the BigQuery result in bg_result.json.
 * Run with:  node sample/compare_bq_result.js
 */
'use strict';

var fs   = require('fs');
var path = require('path');

// Simple safe parser: quotes large bare integers as strings so JSON.parse
// doesn't corrupt them, but without the BIGINT_MARK so values stay readable.
function parseSafe(jsonStr) {
  return JSON.parse(
    jsonStr.replace(
      /"(?:\\.|[^"\\])*"|(-?\d{16,})/g,
      function(match, number) {
        return number !== undefined ? '"' + number + '"' : match;
      }
    )
  );
}

var original = parseSafe(fs.readFileSync(path.join(__dirname, 'json.txt'),       'utf8')).data[0];
var bqResult = parseSafe(fs.readFileSync(path.join(__dirname, 'bg_result.json'), 'utf8'))[0];

var origKeys = Object.keys(original);
var bqKeys   = Object.keys(bqResult);

var diffs        = [];
var missingInBq  = origKeys.filter(function(k) { return !bqResult.hasOwnProperty(k); });
var extraInBq    = bqKeys.filter(function(k)   { return !original.hasOwnProperty(k); });

origKeys.forEach(function(key) {
  if (!bqResult.hasOwnProperty(key)) return;
  if (JSON.stringify(original[key]) !== JSON.stringify(bqResult[key])) {
    diffs.push({ key: key, original: original[key], bq: bqResult[key] });
  }
});

console.log('=== Field count ===');
console.log('Original :', origKeys.length);
console.log('BQ result:', bqKeys.length);

console.log('\n=== Missing / extra fields ===');
if (missingInBq.length) console.log('Missing in BQ:', missingInBq);
else                    console.log('No missing fields.');
if (extraInBq.length)   console.log('Extra in BQ  :', extraInBq);
else                    console.log('No extra fields.');

console.log('\n=== Value differences ===');
if (diffs.length === 0) {
  console.log('All ' + origKeys.length + ' fields match exactly.');
} else {
  diffs.forEach(function(d) {
    console.log('FIELD   :', d.key);
    console.log('Original:', JSON.stringify(d.original), ' type:', typeof d.original);
    console.log('BQ      :', JSON.stringify(d.bq),       ' type:', typeof d.bq);
    console.log('');
  });
}

console.log('\n=== ID precision check ===');
console.log('Original :', original.ID, ' (type: ' + typeof original.ID + ')');
console.log('BQ result:', bqResult.ID, ' (type: ' + typeof bqResult.ID + ')');
console.log('Match    :', original.ID === bqResult.ID);
