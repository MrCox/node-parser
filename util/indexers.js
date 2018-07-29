var fs = require('fs'),
  path = require('path');

module.exports = {
  bytes: { csv: csv, rows: rows, json: json, osv: rows },
  list: { csv: csv, rows: list_rows, json: list_json, osv: list_rows },
  route: { csv: route, rows: route, json: route, osv: route },
  recurse: { csv: csv, rows: r_rows, json: r_json, osv: r_rows },
  string: { rows: str_rows, csv: csv, json: str_json, osv: str_rows }
}

function asv(to) {
}

/* ____________________ JSON ___________________ */

function list_json(to) {
  var index = [],
    from = path.resolve(this.from()),
    err = errors(this),
    last = 0;
  to = to || this.to();
  this._post().push(function() {
    var n = { data: from, type: "list", index: index, format: 'json' }
    fs.writeFile(to, JSON.stringify(n), err);
  });
  return function(rec, i, row, offset) {
    var bytes = (new Buffer(row)).length;

    if (i) index.push([ last, last + bytes - 2 ]);
    else index.push([ last + 1, last + bytes - 2 ]);
    last = last + bytes + offset;
  }
}

function str_json(to, key) {
  var index = {},
    from = path.resolve(this.from()),
    err = errors(this),
    last = 0;
  to = to || this.to();
  this._post().push(function() {
    for (var i in index) { index[i] += ']'; }
    var n = { data: from, type: "bytes", index: index, format: 'json' }
    fs.writeFile(to, JSON.stringify(n), err);
  });
  return function(rec, i, row, offset) {
    var bytes = (new Buffer(row)).length,
      dat = key(rec, i), ind = index[dat];

    if (ind != undefined) index[dat] += ',' + ar_str(last, last + bytes - 2);
    else if (i) index[dat] = '[' + ar_str(last, last + bytes - 2);
    else index[dat] = '[' + ar_str(last + 1, last + bytes - 2);
    last = last + bytes + offset;
  }
}

function json(to, key) {
  var index = {},
    from = path.resolve(this.from()),
    err = errors(this),
    last = 0;
  to = to || this.to();
  this._post().push(function() {
    var n = { data: from, type: "bytes", index: index, format: 'json' }
    fs.writeFile(to, JSON.stringify(n), err);
  });
  return function(rec, i, row, offset) {
    var bytes = (new Buffer(row)).length,
      dat = key(rec, i), ind = index[dat];

    if (ind != undefined) ind.push([ last, last + bytes - 2]);
    else if (i) index[dat] = [[ last, last + bytes - 2 ]];
    else index[dat] = [[ last + 1, last + bytes - 2 ]];
    last = last + bytes + offset;
  }
}

function r_json(to, key) {
  throw new Error("recursive json indexing hasn't been implemented yet");
}

/* ______________________ ROWS _________________________*/

function list_rows(to) {
  var index = [],
    from = path.resolve(this.from()),
    err = errors(this),
    last = 0;
  to = to || this.to();
  this._post().push(function() {
    var n = { data: from, type: "list", index: index, format: 'rows' }
    fs.writeFile(to, JSON.stringify(n), err);
  });
  return function(rec, ind, row, offset) {
    var bytes = (new Buffer(row)).length;

    index.push([ last, last + bytes - 1 ]);
    last = last + bytes + offset;
  }
}

function rows(to, key) {
  var index = {},
    from = path.resolve(this.from()),
    err = errors(this),
    last = 0;

  to = to || this.to();
  this._post().push(function() {
    var n = { data: from, type: "bytes", index: index, format: 'rows' }
    fs.writeFile(to, JSON.stringify(n), err);
  });
  return function(rec, ind, row, offset) {
    var bytes = (new Buffer(row)).length,
      dat = key(rec, ind), ind = index[dat];

    if (ind != undefined) ind.push([ last, last + bytes - 1 ]);
    else index[dat] = [[ last, last + bytes - 1 ]];
    last = last + bytes + offset;
  }
}

function str_rows(to, key) {
  var index = {},
    from = path.resolve(this.from()),
    err = errors(this),
    last = 0;

  to = to || this.to();
  this._post().push(function() {
    for (var i in index) { index[i] += ']' }
    var n = { data: from, type: "bytes", index: index, format: 'rows' }
    fs.writeFile(to, JSON.stringify(n), err);
  });
  return function(rec, ind, row, offset) {
    var bytes = (new Buffer(row)).length,
      dat = key(rec, ind), ind = index[dat];

    if (ind != undefined) index[dat] += (',[' + last + ',' + (last + bytes - 1) + ']');
    else index[dat] = '[[' + last + ',' + (last + bytes - 1) + ']';
    last = last + bytes + offset;
  }
}

function r_rows(to, key) {
  var index = {},
    dir = path.resolve(path.dirname(to)),
    index_path = path.join(dir, '_index'),
    posix = require('posix'),
    limit = posix.getrlimit('nofile'),
    from = this.from(),
    last = 0;

  this._jobs(this._jobs() + 1);
  this._post().push(function() {
    var parser = require('../parser'),
      output = fs.openSync(
        path.join(path.dirname(to), '_' + path.basename(to)), 'w'),
      new_index = {}

    for (var i in index) { parse(i, 0); break; }
    function parse(i, start) {
      var b = new Buffer('{"' + i + '":['), l = b.length;
      fs.writeSync(output, b, 0, l);

      parser(path.join(index_path, i.replace(',', '/')))
        .intype('rows').each(function(row, ind, raw) {
          if (ind) {
            var buff = new Buffer(',' + raw);
            l += buff.length;
            fs.writeSync(output, buff, 0, buff.length);
          } else {
            var buff = new Buffer(raw);
            l += buff.length;
            fs.writeSync(output, buff, 0, buff.length);
          }
        }).execute(function() {
          var end = new Buffer(']}\n');
          fs.writeSync(output, end, 0, end.length);

          fs.closeSync(index[i]); // close file descriptor
          delete index[i]; // removing overhead?
          fs.unlinkSync(path.join(index_path, i.replace('/'))); // deleting file

          new_index[i] = [ start, start + l + end.length - 1 ];
          if (!isEmpty(index)) {
            for (var j in index) { parse(j, start + l + end.length); break; }
          } else {
            // resetting defaults
            var def = { soft: 1024, hard: 4096 }
            console.log("Resetting file desc. limit: " + JSON.stringify(def));
            posix.setrlimit('nofile', def);

            fs.rmdirSync(index_path); // deleting _index dir
            // saving index
            console.log("saving index...");
            var n = {
              data: path.resolve(from),
              type: "recurse",
              index: new_index,
              format: this.intype()
            }
            fs.writeFile(to, JSON.stringify(n), errors(this));
          }
        });
    }
  });
  fs.mkdirSync(index_path);
  return function(row, ind, raw, offset) {
    var routes = key(row, ind, raw).map(cleanRoute),
      route = routes.join(),
      ind = index[route],
      bytes = (new Buffer(row)).length,
      b = new Buffer(JSON.stringify([ last, last + bytes - 1 ]) + '\n');

    if (ind == undefined) {
      var p = index_path;
      for (var i = 0; i < routes.length - 1; i++) {
        // making path, one slash at a time
        p = path.join(p, '' + routes[i]);
        try { fs.mkdirSync(p); } catch(e) { /* path already exists */ }
      }
      // at last, the filename
      p = path.join(p, '' + routes[routes.length - 1]);
      try {
        ind = index[route] = fs.openSync(p, 'a');
      } catch(e) {
        // increasing file descriptor limit
        var lims = { soft: limit.soft + 10000, hard: limit.hard + 10000 }
        console.log("Increasing file descriptor limit: " + JSON.stringify(lims));
        limit = lims;
        posix.setrlimit('nofile', lims);
        ind = index[route] = fs.openSync(p, 'a');
      }
    }
    fs.writeSync(ind, b, 0, b.length);
    last = last + bytes + offset;
  }
}

/* ________________________ CSV __________________________ */

function csv(to, key) {
  throw new Error("CSV indexing is not supported at this time.");
}

/* ________________________ ROUTE INDEXES ____________________ */

function route(to, key) {
  var index = {},
    dir = path.resolve(path.dirname(to)),
    index_path = path.join(dir, '_index'),
    posix = require('posix'),
    limit = posix.getrlimit('nofile'),
    from = this.from();

  this._jobs(this._jobs() + 1);
  this._post().push(function() {
    // closing files
    console.log("closing open files...");
    for (var i in index) {
      fs.close(index[i]);
      index[i] = path.join(index_path, i.replace(',', '/'));
    }
    // resetting defaults
    var def = { soft: 1024, hard: 4096 }
    console.log("Resetting file desc. limit: " + JSON.stringify(def));
    posix.setrlimit('nofile', def);
    // saving index
    console.log("saving index...");
    var n = {
      data: path.resolve(from),
      type: "route",
      index: index,
      format: this.intype()
    }
    fs.writeFile(to, JSON.stringify(n), errors(this));
  });
  fs.mkdirSync(index_path);
  return function(row, ind, raw) {
    var routes = key(row, ind, raw).map(cleanRoute),
      route = routes.join(),
      ind = index[route],
      b = new Buffer(raw + '\n');

    if (ind == undefined) {
      var p = index_path;
      for (var i = 0; i < routes.length; i++) {
        // making path, one slash at a time
        p = path.join(p, '' + routes[i]);
        try { fs.mkdirSync(p); } catch(e) { /* path already exists */ }
      }
      // at last, the filename
      p = path.join(p, '' + routes[routes.length - 1]);
      try {
        ind = index[route] = fs.openSync(p, 'a');
      } catch(e) {
        // increasing file descriptor limit
        var lims = { soft: limit.soft + 10000, hard: limit.hard + 10000 }
        console.log("Increasing file descriptor limit: " + JSON.stringify(lims));
        limit = lims;
        posix.setrlimit('nofile', lims);
        ind = index[route] = fs.openSync(p, 'a');
      }
    }
    fs.writeSync(ind, b, 0, b.length);
  }
}

function errors(t) {
  return function(e) { if (e) throw e; t.emit("write_complete");}
}

function cleanRoute(_) {
  return _ == '' || _ == ' ' ? '_' : _;
}
function ar_str(a, b) {
  return '[' + a + ',' + b + ']';
}
function isEmpty(obj) {
  for (var i in obj) {
    if (obj.hasOwnProperty(i)) return false;
  }
  return true;
}
