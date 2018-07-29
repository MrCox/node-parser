module.exports = {
  csv: csv,
  rows: rows,
  json: json,
  raw: raw,
  asv: asv,
  osv: rows
};

function csv(cb) {
  var delimiter = this.delimiter(),
      headers = this.headers(),
      t = this, quote = t.quote();
  if (!headers) { //using top row as headers
    var fs = require('fs'),
      f = fs.openSync(t.from(), 'r'),
      b = new Buffer(t._topBytes());
    fs.readSync(f, b, 0, t._topBytes(), 0);
    t.headers(
      headers = b.toString().split(t.newline())[0].split(delimiter).map(quotes)
    );
    fs.close(f);
    return function(row, ind, offset) { ind && read.apply(this, arguments); };
  };
  return quote ? read
    : function(row, ind, offset) {
        var recs = row.split(delimiter), n = {};
        headers.forEach(function(d, i) { n[d] = recs[i]; });
        cb.call(t, n, ind, row, offset);
      };
  function read(row, ind, offset) {
    var recs = not_in_quotes(delimiter, quote, row), n = {};
    headers.forEach(function(d, i) { n[d] = recs[i] && quotes(recs[i]); });
    cb.call(t, n, ind, row, offset);
  };
};

function raw(cb) {
  var t = this;
  return function(row, ind, offset) {
    cb.call(t, row, ind, row, offset);
  };
};

function rows(cb) {
  var t = this;
  return function(row, ind, offset) {
    var rec = JSON.parse(row);
    cb.call(t, rec, ind, row, offset);
  };
};

function json(cb) {
  var t = this, realInd = 0, stash, rec;
  return function(row, ind, offset) {
    if (row[0] != '[') {
      //middle chunks
      var d = stash + row,
        start = d.indexOf('{'),
        end = d.lastIndexOf('}') + 1,
        sub = '[' + d.substr(start, end - start) + ']';
      rec = JSON.parse(sub);
      stash = row.substr(end);
    } else {
      //first chunk
      var end = row.lastIndexOf('}') + 1,
        sub = row.substr(0, end) + ']';
      rec = JSON.parse(sub);
      stash = row.substr(end);
    };
    rec.forEach(function(r) {
      cb.call(t, r, realInd, row, offset);
      ++realInd;
    });
  };
};

function asv(cb) {
  var headers = this.headers(), t = this;
  if (!headers) { //using top row as headers
    var fs = require('fs'),
      f = fs.openSync(t.from(), 'r'),
      b = new Buffer(t._topBytes());
    fs.readSync(f, b, 0, t._topBytes(), 0);
    t.headers( headers = JSON.parse(b.toString().split(t.newline())[0]) );
    fs.close(f);
    return function(row, ind, offset) { ind && read.apply(this, arguments); };
  };
  return read;
  function read(row, ind, offset) {
    var recs = JSON.parse(row), n = {};
    headers.forEach(function(d, i) { n[d] = recs[i]; });
    cb.call(t, n, ind, row, offset);
  };
};

function not_in_quotes(comma, quote, line) {
  var count = 0, n = {};
  for (var i = line.length - 1; i > -1; i--) {
    if (line[i] == quote) ++count;
    n[i] = count;
  };

  if (!count || count == 1) return line.split(comma).map(quotes);

  var split = [], cur = '';
  for (var i = 0; i < line.length; i++) {
    var c = line[i];
    if (c != comma) cur += c;
    else if (c == comma && (n[i] % 2 == 0)) {
      split.push(cur);
      cur = '';
    };
  };
  return split;
};
function quotes(d) { return d.replace(/^"+|"+$/g, ''); };
