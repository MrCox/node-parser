var fs = require("fs");

module.exports = {
   json: csv_to_json,
   rows: csv_to_json_rows,
   csv: to_csv,
   raw: raw,
   osv: csv_to_json_rows,
   asv: to_asv
}

function to_csv(to, headers) {
  to = to || this.to();
  var t = this,
    fd = fs.openSync(to, 'w'),
    heads = headers || t.headers();

  this._pre().push(function() {
    // finds headers
    if (!heads) {
      var f = fs.openSync(t.from(), 'r'),
        b = new Buffer(t._topBytes());

      fs.readSync(f, b, 0, t._topBytes(), 0);

      require('./readers')[t.intype()]
        .call(t, function(row) {
          t.headers(heads = Object.keys(row));
        })
        .call(t, b.toString());

      fs.close(f);
    }

    var b = new Buffer(heads.map(add_quotes).join(',') + '\n');
    fs.writeSync(fd, b, 0, b.length);
  })

  this._post().push(function() { fs.close(fd); })
  var h;
  return function(row, ind, raw) {
    var n = heads.map(function(h) {
        return (h = check_val(row[h])) ? add_quotes(h) : h;
      }), b = new Buffer(n.join(',') + '\n');
    fs.writeSync(fd, b, 0, b.length);
  }
}

function raw(to) {
  to = to || this.to();
  var fd = fs.openSync(to, 'w');
  this._post().push(function() { fs.close(fd); })
  return function(row, ind, raw) {
    var b = new Buffer(raw + ',\n');
    fs.writeSync(fd, b, 0, b.length);
  }
}

function csv_to_json_rows(to) {
  var t = this,
    fd = fs.openSync(to || t.to(), 'w'); // opens file
  t._post().push(function() { fs.close(fd); }) // closes file
  return function(row) {
    var b = new Buffer(JSON.stringify(row) + '\n');
    fs.writeSync(fd, b, 0, b.length);
  }
}

function csv_to_json(to) {
  to = to || this.to();
  var fd = fs.openSync(to, 'w'), // opens file
    pos = 0;
  this._pre().push(function() {
    var b = new Buffer('[');
    pos += b.length;
    fs.writeSync(fd, b, 0, b.length);
  })
  this._post().push(function() {
    var b = new Buffer(']');
    fs.writeSync(fd, b, 0, b.length, pos - 2); // overwrites comma
    fs.close(fd); // closing file
  })
  return function(row) {
    var b = new Buffer(JSON.stringify(row) + ',\n');
    pos += fs.writeSync(fd, b, 0, b.length);
  }
}


function to_asv(to, headers) {
  to = to || this.to();
  var t = this,
    fd = fs.openSync(to, 'w'),
    heads = headers || t.headers();
  this._pre().push(function() {
    if (!heads) {
      var f = fs.openSync(t.from(), 'r'), b = new Buffer(t._topBytes());
      fs.readSync(f, b, 0, t._topBytes(), 0);
      require('./readers')[t.intype()].call(t, function(row) {
        t.headers(heads = Object.keys(row));
      }).call(t, b.toString().split(t.newline())[0], 1);
      fs.close(f);
    }
    var b = new Buffer(JSON.stringify(heads) + '\n');
    fs.writeSync(fd, b, 0, b.length);
  })
  this._post().push(function() { fs.close(fd); })
  return function(row, ind, raw) {
    var n = heads.map(function(h) { return check_val(row[h]); }),
      b = new Buffer(JSON.stringify(n) + '\n');
    fs.writeSync(fd, b, 0, b.length);
  }
}


function add_quotes(_) {
  return typeof _ == 'string'
    ? (_.match(',') ? '"' + _ + '"' : _)
    : _;
}

function check_val(d) {
  return d != undefined && d != null ? d : '';
}
