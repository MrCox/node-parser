var events = require('events'),
  fs = require('fs');

module.exports = {
  chain : chain,
  entries: entries,
  keys: keys,
  values: values,
  entries: entries,
  merge: merge,
  get: getter(id).get,
  id: id,
  map: map,
  readBytes: readBytes,
  copy: copy,
  requote: requote
}

function requote(_) {
  return _.replace(/[\\\^\$\*\+\?\|\[\]\(\)\.\{\}]/g, "\\$&");
}

function copy(_) {
  return JSON.parse(JSON.stringify(_));
}

function readBytes(fd, index) {
  var l = index[1]- index[0] + 1, b = new Buffer(l);
  fs.readSync(fd, b, 0, l, index[0]);
  return b;
}

function map(obj) {
  return function(_) { return obj[_]; }
}

function range(n) {
  var c = [], i = -1;
  while (++i < n) c.push(i);
  return c;
}

function getter(fn) {
  fn.get = function() {
    var args = arguments;
    return getter(function(d) {
      var r = fn(d);
      for (var i = 0; i < args.length; i++) r = r && r[args[i]];
      return r;
    });
  }
  fn.map = function() {
    var args = arguments;
    return getter(function(d, j) {
      var r = args[0].call(this, fn(d), j);
      for (var i = 1; i < args.length; i++) r = r && args[i].call(this, r, j);
      return r;
    });
  }
  fn.or = function(str_or_fn) {
    var alt = typeof str_or_fn == "string"
      ? getter(id).get(str_or_fn) : str_or_fn;
    return getter(function(d, j) {
      return fn(d, j) || alt(d, j);
    });
  }
  fn.add = function() {
    var args = Array.prototype.map.call(arguments, function(a) {
        return typeof a == "string" ? idf(a) : a;
      });
    return getter(function(d, j) {
        var r = fn(d, j);
        args.forEach(function(a) { r = r + a(d, j); });
        return r;
      });
  }
  return fn;
}

function merge(ar) {
  return arguments.length > 1
    ? Array.prototype.concat.apply([], arguments)
    : Array.prototype.concat.apply([], ar);
}

function keys(obj) {
  return Object.keys(obj);
}

function values(obj) {
  var n = [];
  for (var i in obj) n.push(obj[i]);
  return n;
}

function entries(obj) {
  var n = [];
  for (var i in obj) n.push({ key: i, value: obj[i] });
  return n;
}

function chain(cb) {

  return function() {
    var self = Object.create(events.EventEmitter.prototype);
    self._vars = {}

    self.var = function(name, def, fn) {
      self._vars[name] = def;
      self[name] = function(_) {
        if (!arguments.length) return self._vars[name];
        arguments[arguments.length] = self._vars[name];
        self._vars[name] = _;
        fn && fn.apply(self, arguments);
        return self;
      }
    }

    self.function = function(name, fn) {
      self[name] = function() {
        fn.apply(self, arguments);
        return self;
      }
    }

    self.call = function(fn) {
      var args = Array.prototype.slice.call(arguments);
      args.unshift(self);
      fn.apply(self, args);
      return self;
    }

    self.vars = function() {
      Array.prototype.forEach.call(arguments, function(a) { self.var(a) });
    }

    cb && cb.apply(self, arguments);
    return self;
  }
}

function id(d) { return d; }
