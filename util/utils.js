var events = require('events'),
  fs = require('fs');

module.exports = {
  chain : chain,
  entries: entries,
  keys: keys,
  values: values,
  merge: merge,
  uniques: uniques,
  get: getter(id).get,
  id: id,
  idf: idf,
  idn: idn,
  closure: closure,
  range: range,
  map: map,
  set: set,
  min: min,
  max: max,
  extent: extent,
  sum: sum,
  mean: mean,
  mode: mode,
  median: median,
  quantile: quantile,
  ascending: ascending,
  descending: descending,
  y2k_date: y2k_date,
  readBytes: readBytes,
  copy: copy,
  objEqual: objEqual,
  quickSort: quickSort,
  requote: requote,
  countChar: countChar,
  objEmpty: objEmpty,
  shuffle: shuffle,
  randomArray: randomArray
}
function randomArray(n, factor) {
  var ar = [];
  if (factor) {
    for (var i = 0; i < n; i++) ar.push( Math.random() * i * factor );
  } else {
    for (var i = 0; i < n; i++) ar.push( Math.random() * i );
  }
  return ar;
}
function shuffle(array) {
  var m = array.length, t, i;
  while (m) {
    i = Math.random() * m-- | 0;
    t = array[m], array[m] = array[i], array[i] = t;
  }
  return array;
}
function objEmpty(obj) {
  for (var i in obj) return false;
  return true;
}
function countChar(str, _) {
  var c = 0;
  for (var i = 0; i < str.length; i++) (str[i] == _) && ++c;
  return c;
}
function requote(_) {
  return _.replace(/[\\\^\$\*\+\?\|\[\]\(\)\.\{\}]/g, "\\$&");
}
function copy(_) {
  return JSON.parse(JSON.stringify(_));
}
function objEqual(a, b) {
  var bc = copy(b);
  for (var i in a) { if (a[i] != bc[i]) return false; delete bc[i]; }
  for (var i in bc) { if (ac[i] != bc[i]) return false; }
  return true;
}
function readBytes(fd, index) {
  var l = index[1]- index[0] + 1, b = new Buffer(l);
  fs.readSync(fd, b, 0, l, index[0]);
  return b;
}
function y2k_date(str) {
  if (str.length == 6) str = '0' + str;
  else if (str.length == 1) return str;
  var date_str = str.slice(3, 5) + '/', map = ['19', '20', '21'];
  date_str += str.slice(5) + '/';
  date_str += (map[str[0]] + str.slice(1, 3));
  return date_str;
}
function descending(key) {
  if (key) return function(a, b) {
    a = key(a), b = key(b);
    return b < a ? -1 : b > a ? 1 : b >= a ? 0 : NaN;
  }
  return function(a, b) {
    return b < a ? -1 : b > a ? 1 : b >= a ? 0 : NaN;
  }
}
function ascending(key) {
  if (key) return function(a, b) {
    a = key(a), b = key(b);
    return a < b ? -1 : a > b ? 1 : a >= b ? 0 : NaN;
  }
  return function(a, b) {
    return a < b ? -1 : a > b ? 1 : a >= b ? 0 : NaN;
  }
}
function median(ar, key) {
  if (key) ar = ar.map(key);
  return quantile(ar, .5);
}
function quantile(ar, dec) {
  var H = (ar.length - 1) * dec + 1,
      h = Math.floor(H), v = +ar[h - 1],
      e = H - h;
  return e ? v + e * (ar[h] - v) : v;
}
function mode(ar, key) {
  if (key) ar = ar.map(key);
  var counts = set(ar).counts(),
    most = 0,
    key;
  for (var i in counts) {
    if (counts[i] > most) {
      most = counts[i];
      key = i;
    }
  }
  return key;
}
function mean(ar, key) {
  if (key) ar = ar.map(key);
  return sum(ar) / ar.length;
}
function sum(ar, key, init) {
  if (key) ar = ar.map(key);
  var sum = init || 0;
  for (var i = 0; i < ar.length; i++) sum += ar[i];
  return sum;
}
function min(ar, key) {
  if (key) ar = ar.map(key);
  return Math.min.apply(null, ar);
}
function max(ar, key) {
  if (key) ar = ar.map(key);
  return Math.max.apply(null, ar);
}
function extent(ar, key) {
  if (key) ar = ar.map(key);
  var n = [ ar[0], ar[0] ];
  for (var i = 1; i < ar.length; i++) {
    if (ar[i] < n[0]) n[0] = ar[i];
    else if (ar[i] > n[1]) n[1] = ar[1];
  }
  return n;
}
function map(obj) {
  return function(_) { return obj[_]; }
}
function set(ar) {
  if (ar._set) return ar;
  var counts = {}, unique = [];

  if (!(ar instanceof Array) && typeof ar === "object") {
    // if an obj is passed, set is initialized this way and unique is returned
    for (var i in ar) (counts[i] = 1) && unique.push(i);
    ar = unique;
    ar._object = true;
  }

  ar._set = true;
  ar.counts = function() { return counts; }
  // if ar was an object, unique is returned => pushing to ar is redundant
  ar.add = ar._object
    ? function(_) {
        if (!counts[_]) unique.push(_), counts[_] = 1;
        else ++counts[_];
        return ar;
      }
    : function(_) {
        if (!counts[_]) unique.push(_), ar.push(_), counts[_] = 1;
        else ++counts[_];
        return ar;
      }

  ar.uniques = function() { return unique; }

  /* set operations */
  ar.cardinality = function() { return unique.length; }
  ar.intersection = function(ar2) {
    if (!ar2._set) ar2 = set(ar2);
    var n = set([]);
    ar2.each(function(d) { if (d in counts) n.add(d); });
    return n;
  }
  ar.union = function(ar2) {
    return set( merge(ar, ar2) );
  }
  ar.complement = function(ar2) {
    if (!ar2._set) ar2 = set(ar2);
    var n = set([]);
    ar2.each(function(d) { if (!(d in counts)) n.push(d), n.add(d); });
    return n;
  }
  ar.minus = function(ar2) {
    return ar2.complement(ar);
  }
  ar.diff = function(ar2) {
    if (!ar2._set) ar2 = set(ar2);
    return ar.complement(ar2).union(ar2.complement(ar));
  }

  /* booleans */
  ar.disjoint = function(ar2) {
    return ( ar.intersection(ar2).length > 0 );
  }
  ar.subsetOf = function(ar2) {
    return ( ar2.complement(ar).length == 0 )
  }
  ar.equal = function(ar2) {
    return ( ar.subsetOf(ar2) && ar2.subsetOf(ar) )
  }
  ar.has = function(_) { return _ in counts; }

  /* iterating */
  ar.each = function(cb, t) {
    unique.forEach(cb, t);
    return ar;
  }
  ar.map = function(cb, t) {
    return set(Array.prototype.map.apply(ar, arguments));
  }
  ar.filter = function(cb, t) {
    return set(Array.prototype.filter.apply(ar, arguments));
  }

  /* aliases for convenience */
  ar.size = ar.cardinality;
  ar.plus = ar.union;
  ar.intersect = ar.intersection;

  /* adding items if necessary (ie, ar is an array) */
  (!unique.length) && ar.forEach(ar.add);
  return ar;
}
function range(n) {
  var c = [], i = -1;
  while (++i < n) c.push(i);
  return c;
}
function idf(_) {
  return function() { return _; }
}
function idn(n) {
  return function() { return arguments[n]; }
}
function closure(fn, args) {
  args = args || [];
  return function() {
    return fn.apply(this, Args(arguments).concat(args));
  }
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
function uniques(ar, key) {
  if (key) ar = ar.map(key);
  var n = {}
  ar.forEach(function(a) { n[a] = null; });
  return keys(n);
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
  if (cb instanceof Array) return fluentArray.apply(cb, arguments);
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
function Args(args, n) {
  return Array.prototype.slice.call(args, n);
}
function id(d) { return d; }

/* fluentArray prototype */
var fluentArray_prototype = [];
// maps elements, retaining methods of fluent array
fluentArray_prototype.map = function(_) {
  var mapped = fluentArray(Array.prototype.map.apply(this, arguments),
      this._methods, this._bind);
  mapped.back = back(this);
  mapped.data(this.data());
  return this._fn ? mapped.call(mapped._fn = this._fn) : mapped;
}
// filters, but retains methods of fluent array
fluentArray_prototype.filter = function() {
  var methods = this._methods,
    b = this._bind,
    filtered = arguments.length
      ? fluentArray(Array.prototype.filter.apply(this, arguments), methods, b)
      : fluentArray(Array.prototype.filter.call(this, id), methods, b);
  filtered.back = back(this);
  filtered.data(this.data());
  return this._fn ? filtered.call(filtered._fn = this._fn) : filtered;
}
// takes a callback which is passed the fluent array as the first arg
fluentArray_prototype.call = function(fn) {
  var args = Array.prototype.slice.call(arguments, 1);
  args.unshift(this);
  if (fn) fn.apply(this, args);
  return this;
}
// saves array for data-driven attributes
fluentArray_prototype.data = function(_) {
  if (!arguments.length) return this._data;
  this._data = _;
  return this;
}
// will map the array according to what is returned by the method;
// analogous to d3's attr function.
fluentArray_prototype.method = function(_) {
  var args = Array.prototype.slice.call(arguments, 1);
  if (args.length) return this.map(function(obj) {
    return obj && obj[_] && obj[_].apply(obj, args);
  });
  else return this.map(function(obj) {
    return obj && obj[_] && obj[_]();
  });
}
// for each method name passed, applies that method and returns result
fluentArray_prototype.methods = function() {
  var result = this;
  Array.prototype.forEach.call(arguments, function(method) {
    result = result.method(method);
  });
  result.back = back(this);
  return result;
}
// applies each item in array to method on contents
fluentArray_prototype.onAll = function(_, ar) {
  if (ar instanceof Function)
    return this.map(function(obj, i) {
      var arg = this.data()
        ? ar.call(obj, this.data()[i], i)
        : ar.call(obj, null, i);
      return obj && obj[_] && obj[_].call(obj, arg);
    }, this);
  else return this.method.apply(this, arguments);
}
// merges sub-arrays into one fluent array
fluentArray_prototype.merge = function(ms) {
  var n = [];
  this.forEach(function(ar) {
    ar && ar instanceof Array && n.push.apply(n, ar);
  });
  n = fluentArray(n, ms || this._methods);
  n.back = back(this);
  return n;
}
// turns contents into fluent arrays, returning first if it's the only one
fluentArray_prototype.spawn = function(ms) {
  var result = this.map(function(ar) {
      return ar instanceof Array ? fluentArray(ar, ms || methods) : ar;
    });
  if (result.length == 1 && result[0].__fluent__)
    result = result[0], result.back = back(this);
  return result;
}
fluentArray_prototype.__fluent__ = true;

// convenience for creating fluentArrays with custom methods
function fluentArray(_, methods, bind, fn) {
  var s = _ || [], fa = fluentArray_prototype;
  inherit(s, fluentArray_prototype);
  s._methods = methods;
  if (!s.back) s.back = back(s);
  // adds methods to the fluent array, like our d3 selection mods.
  var method = bind ? s._bind = true && s.onAll : s.method;
  if (methods) methods.forEach(function(name) {
    if (!(name in fa)) s[name] = function() {
        if (arguments.length) {
          var args = Array.prototype.slice.call(arguments);
          args.unshift(name);
          return method.apply(s, args);
        } else return s.method(name);
      }
  });
  return fn ? s.call((s._fn = fn)) : s;
}
function back(ar) {
  return function(num) {
    if (!arguments.length) return ar;
    else if (num <= 0) return this;
    var result = this;
    for (var j = 0; j < num; j++) result = result.back();
    return result;
  }
}
function inherit(inh, con) {
  for (var i in con) inh[i] = con[i];
}
function quickSort(a, f) {
  return quicksort_by(f || id)(a, 0, a.length);
}
function insertionsort_by(f) {
  return function(a, lo, hi) {
    for (var i = lo + 1; i < hi; ++i) {
      for (var j = i, t = a[i], x = f(t); j > lo && f(a[j - 1]) > x; --j) {
        a[j] = a[j - 1];
      }
      a[j] = t;
    }
    return a;
  }
}
function quicksort_by(f) {
  var insertionsort = insertionsort_by(f),
    size_threshold = 32;

  return sort;
  function sort(a, lo, hi) {
    return (hi - lo < size_threshold ? insertionsort : quicksort)(a, lo, hi);
  }
  function quicksort(a, lo, hi) {
    // Compute the two pivots by looking at 5 elements.
    var sixth = (hi - lo) / 6 | 0,
        i1 = lo + sixth,
        i5 = hi - 1 - sixth,
        i3 = lo + hi - 1 >> 1,  // The midpoint.
        i2 = i3 - sixth,
        i4 = i3 + sixth;

    var e1 = a[i1], x1 = f(e1),
        e2 = a[i2], x2 = f(e2),
        e3 = a[i3], x3 = f(e3),
        e4 = a[i4], x4 = f(e4),
        e5 = a[i5], x5 = f(e5);

    var t;
    // Sort the selected 5 elements using a sorting network.
    if (x1 > x2) t = e1, e1 = e2, e2 = t, t = x1, x1 = x2, x2 = t;
    if (x4 > x5) t = e4, e4 = e5, e5 = t, t = x4, x4 = x5, x5 = t;
    if (x1 > x3) t = e1, e1 = e3, e3 = t, t = x1, x1 = x3, x3 = t;
    if (x2 > x3) t = e2, e2 = e3, e3 = t, t = x2, x2 = x3, x3 = t;
    if (x1 > x4) t = e1, e1 = e4, e4 = t, t = x1, x1 = x4, x4 = t;
    if (x3 > x4) t = e3, e3 = e4, e4 = t, t = x3, x3 = x4, x4 = t;
    if (x2 > x5) t = e2, e2 = e5, e5 = t, t = x2, x2 = x5, x5 = t;
    if (x2 > x3) t = e2, e2 = e3, e3 = t, t = x2, x2 = x3, x3 = t;
    if (x4 > x5) t = e4, e4 = e5, e5 = t, t = x4, x4 = x5, x5 = t;

    var pivot1 = e2, pivVal1 = x2,
        pivot2 = e4, pivVal2 = x4;

    a[i1] = e1;
    a[i2] = a[lo];
    a[i3] = e3;
    a[i4] = a[hi - 1];
    a[i5] = e5;

    var less = lo + 1,   // First element in the middle partition.
        great = hi - 2;  // Last element in the middle partition.

    var pivotsEqual = pivVal1 <= pivVal2 && pivVal1 >= pivVal2;
    if (pivotsEqual) {
      for (var k = less; k <= great; ++k) {
        var ek = a[k], xk = f(ek);
        if (xk < pivVal1) {
          if (k !== less) { a[k] = a[less]; a[less] = ek; }
          ++less;
        } else if (xk > pivVal1) {
          while (true) {
            var greatVal = f(a[great]);
            if (greatVal > pivVal1) {
              great--; continue;
            } else if (greatVal < pivVal1) {
              a[k] = a[less];
              a[less++] = a[great];
              a[great--] = ek;
              break;
            } else { a[k] = a[great]; a[great--] = ek; break; }
          }
        }
      }
    } else {
      for (var k = less; k <= great; k++) {
        var ek = a[k], xk = f(ek);
        if (xk < pivVal1) {
          if (k !== less) { a[k] = a[less]; a[less] = ek; }
          ++less;
        } else {
          if (xk > pivVal2) {
            while (true) {
              var greatVal = f(a[great]);
              if (greatVal > pivVal2) {
                great--;
                if (great < k) break;
                continue;
              } else {
                if (greatVal < pivVal1) {
                  a[k] = a[less];
                  a[less++] = a[great];
                  a[great--] = ek;
                } else { a[k] = a[great]; a[great--] = ek; }
                break;
              }
            }
          }
        }
      }
    }

    a[lo] = a[less - 1];
    a[less - 1] = pivot1;
    a[hi - 1] = a[great + 1];
    a[great + 1] = pivot2;

    sort(a, lo, less - 1);
    sort(a, great + 2, hi);

    if (pivotsEqual) return a;

    if (less < i1 && great > i5) {
      var lessVal, greatVal;
      while ((lessVal = f(a[less])) <= pivVal1 && lessVal >= pivVal1) ++less;
      while ((greatVal = f(a[great])) <= pivVal2 && greatVal >= pivVal2) --great;
      for (var k = less; k <= great; k++) {
        var ek = a[k], xk = f(ek);
        if (xk <= pivVal1 && xk >= pivVal1) {
          if (k !== less) { a[k] = a[less]; a[less] = ek; }
          less++;
        } else {
          if (xk <= pivVal2 && xk >= pivVal2) {
            while (true) {
              var greatVal = f(a[great]);
              if (greatVal <= pivVal2 && greatVal >= pivVal2) {
                great--;
                if (great < k) break;
                continue;
              } else {
                if (greatVal < pivVal1) {
                  a[k] = a[less];
                  a[less++] = a[great];
                  a[great--] = ek;
                } else { a[k] = a[great]; a[great--] = ek; }
                break;
              }
            }
          }
        }
      }
    }
    return sort(a, less, great + 1);
  }
}
