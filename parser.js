var fs = require('fs'),
  u = require('./util/utils'),
  readers = require('./util/readers'),
  writers = require('./util/writers'),
  util = require('util'),
  path = require('path'),
  chain = u.chain;

module.exports = function(source, dest) { return parser(source, dest); }

var parser = chain(function(from, to) {
  var self = this, _queue = [];
  self.queue = function() { return _queue; }
  self.var('headerObj');
  self.var('index');
  self.var('_jobs', 0);
  self.var('_topBytes', 10000);
  self.var('_pre', []);
  self.var('_post', []);
  self.var('to', to);
  self.var('from', from);
  self.var('newline', /\r\n|\n/);
  self.var('intype', 'rows');
  self.var('outtype', 'json');
  self.var("quote");
  self.var('delimiter', ',');
  self.var('headers', null, function(_) {
    if (typeof _ === "number") {
      self._vars.headers = [], i = -1;
      while (++i < _) self._vars.headers.push(i);
    }
  });

  self.execute = function(cb) {
    self._pre().unshift(function() { self.emit("start"); }); // starts with start event

    if (self._jobs() > 0) { // if there are asyncronous write jobs
      var writes = 0;
      self.on("write_complete", function() {
        if (++writes == self._jobs()) {
          cb && cb.call(self);
          self.emit("finish"); // emits "finish" when write jobs are done
        }
      });
    } else {
      self.on("read_complete", function() {
        cb && cb.call(self);
        self.emit("finish"); // otherwise, emits "finish" when reading is done
      });
    }

    var r = readers[ intype(self.from()) ],
      q = _queue.map(function(fn) { return r.call(self, fn); }),
      ind = 0; // absolute index

    reader(function(rows, newline) {
      var nl = newline && (new Buffer(newline)).length || 0;
      rows.forEach(function(row, i) {
        q.forEach(function(fn) { fn.call(self, row, ind, nl); });
        ++ind;
      });
    });
    return self;
  }

  self.merge = function(dataset) {
    return require('./lib/merge')(self.from())
      .to(self.to()).with(dataset)
      .intype(intype(self.from()))
      .outtype(self.outtype());
  }

  self.dump = function(to) {
    to = to || self.to();
    return _queue.push(writers[ self.outtype() ].call(self, to)) && self;
  }

  self.filter = function(to, key) {
    return _queue.push(filterer(to || self.to(), key)) && self;
  }

  self.chunks = function(cb) {
    return fs.createReadStream(self.from()).on('data', cb);
  }

  self.group = function(to, key, reduce) {
    reduce = reduce || { add: add, init: init }
    if (typeof key == "string") key = u.get(key);
    self._jobs(self._jobs() + 1);
    return _queue.push(grouper(to || self.to(), key, reduce)) && self;
  }

  self.index = function(to) {
    var rules = Array.prototype.slice.call(arguments, 1);
    return require('./lib/indexer')(self.from(), to || self.to())
      .parser(self).rules.apply(null, rules);
  }

  self.each = function(cb) {
    return _queue.push(cb) && self;
  }

  self.pipe = function(stream) {
    return fs.createReadStream(self.from()).pipe(stream);
  }

  self.lines = function(cb) {
    var start, newline = self.newline(), ind = -1;
    return fs.createReadStream(self.from()).on('data', function(d) {
      var str = d.toString();
      start && (str = start + str);

      var rows = str.split(newline); // gets array of rows
      start = rows.pop(); // stashing last row;
      rows.forEach(function(r) { cb.call(self, r, ++ind); });
    }).on('end', function() {
      if (start) cb.call(self, start, ++ind);
      self.emit("read_complete", self);
      self.emit("finish", self);
    });
  }

  self.reduce = function(to, fn, init) {
    if (!fn) { fn = reducer, init = {} }
    var prev = init, to = to || self.to();
    self._post().push(function() {
      var fd = fs.openSync(to, 'w');
      fs.writeSync(fd, JSON.stringify(prev));
      fs.close(fd);
    });
    return _queue.push(function(row, ind, raw) {
      prev = fn(prev, row, ind, raw);
    }) && self;
  }

  self.map = function(to, fn) {
    to = to || self.to();
    var fd = fs.openSync(to, 'a');
    self._post().push(function() { fs.close(fd); });
    return _queue.push(function(row, ind, raw) {
      var b = JSON.stringify(fn(row, ind, raw));
      fs.writeSync(fd, b, 0, b.length);
    }) && self;
  }

  self.transform = function(to, fn) {
    to = to || self.to();
    var fd = fs.openSync(to, 'a');
    self._post().push(function() { fs.close(fd); });
    return _queue.push(function(row, ind, raw) {
        var dat = fn(row, ind, dat), b;
        if (dat != undefined) {
          b = new Buffer(JSON.stringify(dat) + '\n');
          fs.writeSync(fd, b, 0, b.length);
        }
      }) && self;
  }

  function reader(cb) {
    var start, from = self.from(), newline = self.newline();
    self._pre().forEach(function(f) { f.call(self); });

    fs.createReadStream(from).on('data', function(d) {
      var str = d.toString();
      if (start) str = start + str;

      var rows = str.split(newline), // gets array of rows
        n = str.match(newline); // actual newline char

      start = rows.pop(); // stashes last row
      cb.call(self, rows, n && n[0]);
    }).on('end', function() {
      if (start) {
        var n = start.match(newline);
        cb.call(self, [start], n && n[0]); // if leftover row
      }
      self._post().forEach(function(f) { f.call(self); });
      self.emit("read_complete", self);
    });
  }

  function grouper(to, key, reduce) {
    to = to || self.to();
    var group = {}
    self._post().push(function() {
      to && fs.writeFile(to, JSON.stringify(group), errors);
    });
    return function(row, ind, raw) {
      var val = key(row), g = group[val];
      if (g != undefined) group[val] = reduce.add(group[val], row, ind, raw);
      else group[val] = reduce.add(reduce.init(row, ind, raw), row, ind, raw);
    }
  }

  function filterer(to, key) {
    to = to || self.to();
    var write = writers[ self.outtype() ].call(self, to);
    return function(row, ind, raw) {
      if (key(row, ind, raw)) write(row, ind, raw);
    }
  }

  function reducer(p, v, ind) {
    for (var i in v) {
      if (p[i] != undefined) {
        var field = p[i];
        if (field[ v[i] ] != undefined) ++field[v[i]];
        else field[ v[i] ] = 1;
      } else {
        var field = p[i] = {}
        field[ v[i] ] = 1;
      }
    }
    return p;
  }

  function intype(from) {
    var ext = path.extname(from).replace('.', '');
    return (ext == "json" || !( ext in readers)) ? self.intype() : ext;
  }

  function add(p,v) { return ++p; }
  function init(p,v) { return 0; }
  function errors(e) { if (e) throw e; self.emit("write_complete"); }
});
