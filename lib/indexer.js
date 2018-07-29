var parser = require('../parser'),
  fs = require('fs'),
  u = require('../util/utils'),
  path = require('path'),
  indexers = require('../util/indexers'),
  chain = u.chain;

module.exports = chain(function(from, to) {
  var self = this, _rules = [], _listeners = [];
  self.var('parser', parser(from, to));
  self.var('from', from);
  self.var('to', to);
  self.var('intype', 'rows');
  self.var('index_type', 'bytes');
  self.rule = function(fn_or_str) {
    if (typeof fn_or_str === "string") fn_or_str = u.get(fn_or_str);
    _rules.push(fn_or_str);
    return self;
  };
  self.rules = function() {
    for (var i = 0; i < arguments.length; i++) self.rule(arguments[i]);
    return self;
  };
  self.on = function(event, listener) {
    _listeners.push({ event: event, listener: listener });
    apply_listeners(self.parser());
    return self;
  };
  self.execute = function(cb) {
    to = to || self.to();
    from = from || self.from();

    var parse = self.parser();
    parse.queue().push(
      indexers[self.index_type()][intype(from)].call(parse, to, rule())
    );
    parse._jobs(parse._jobs() + 1).execute(cb);
    return self;

    function rule() {
      return !_rules.length ? by_ind
        : function(row, ind, raw) {
            return _rules.map(function(r) { return r(row, ind, raw); });
          };
    };
    function by_ind(row, ind) { return ['' + ind]; };
  };
  function apply_listeners(parser) {
    if (!parser) return;
    _listeners.forEach(function(o) { parser.on(o.event, o.listener); });
  };
  function intype(from) {
    var ext = path.extname(from).replace('.', '');
    return (ext == "json" || !( ext in indexers[self.index_type()] ))
      ? self.intype() : ext;
  };
});
