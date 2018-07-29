var parser = require('../parser'),
  fs = require('fs'),
  u = require('../util/utils'),
  writers = require('../util/writers'),
  readers = require('../util/readers'),
  path = require('path'),
  chain = u.chain;

module.exports = function(from) { return merger(from); };

var merger = chain(function(from) {
  var self = this, _listeners = [];
  self.var('parser', parser(from));
  self.var('intype', 'rows');
  self.var('outtype', 'json');
  self.var('reduce_empty');
  self.var('reduce', reduce);
  self.vars('to', 'with', 'key', 'index');
  self.execute = function(cb) {
    var to = self.to(),
      key = self.key() || u.id,
      fd = fs.openSync(self.with(), 'r'),
      reduce = self.reduce(),
      reduce_empty = self.reduce_empty(),
      index = self.index(),
      parse = self.parser(),
      write = writers[self.outtype()].call(parse, to);

    var k, ind, newrec; // forward declaration -- for use in reduce funcs
    parse.intype(intype(from))
      .each( reduce_empty ? REDUCE_EMPTY : REDUCE )
      .execute(function() {
        fs.close(fd);
        cb && cb.call(parse);
      });

    function REDUCE(row) {
      k = key(row);
      ind = index[k];
      if (ind != undefined) {
        newrec = reduce(row, ind.map(get_recs));
        if (newrec != undefined) write(newrec);
      };
    };
    function REDUCE_EMPTY(row) {
      k = key(row);
      ind = index[k];
      if (ind != undefined) {
        newrec = reduce(row, ind.map(get_recs));
        if (newrec != undefined) write(newrec);
      } else {
        newrec = reduce_empty(row);
        if (newrec != undefined) write(newrec);
      };
    };
    function get_recs(d) {
      return JSON.parse(readBytes(fd, d).toString());
    };
  };
  self.on = function(event, listener) {
    _listeners.push({ event: event, listener: listener });
    apply_listeners(self.parser());
    return self;
  };
  function apply_listeners(parser) {
    if (!parser) return;
    _listeners.forEach(function(o) { parser.on(o.event, o.listener); });
  };
  function readBytes(fd, index) {
    var l = index[1] - index[0] + 1, b = new Buffer(l);
    fs.readSync(fd, b, 0, l, index[0]);
    return b;
  };
  function reduce(row, recs) {
    row._matches = recs;
    return row;
  };
  function intype(from) {
    var ext = path.extname(from).replace('.', '');
    return (ext == "json" || !( ext in readers)) ? self.intype() : ext;
  };
});
