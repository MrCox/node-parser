var u = require('./utils'),
  fs = require('fs'),
  getters = require('./getters'),
  id = u.id;

module.exports = function(_) { return querier(_);};
var querier = u.chain(function(indexObj) {
  if (typeof indexObj === "string") indexObj = require(indexObj);
  var self = this;
  self.var('index', indexObj.index);
  self.var('source', fs.openSync(indexObj.data,'r'));
  self.var('getter', getters.stream[indexObj.type](self));
  self.var('mode', 'stream', function(_) {
    self.getter(getters[_][indexObj.type](self));
  });
  self.get = function() {
    var g = self.getter();
    if (self.mode() == "whole") {
      if (!arguments.length) return g(vals);
      var n = [];
      Array.prototype.forEach.call(arguments, function(vals) {
        n.push.apply(n, g(vals));
      });
      return n;
    } else {
      var l = arguments.length, cb, finish;

      if (typeof arguments[l - 2] == "function") {
        cb = arguments[l - 2];
        finish = arguments[l - 1];
      } else if (typeof arguments[l - 1] == "function") {
        cb = arguments[l - 1];
      } else throw Error("Final argument must be function in stream mode");

      if (indexObj.type == "list") g(self.index(), cb, finish);
      else {
        var vals = (cb && finish)
          ? Array.prototype.slice.call(arguments, 0, l - 2)
          : Array.prototype.slice.call(arguments, 0, l - 1);

        var args = !vals.length ? u.values(self.index())
          : Array.prototype.map.call(arguments, u.map(self.index())).filter(id);
        g(args, cb, finish);
      };
    };
    return self;
  };
  process.on('exit', function() {
    if (indexObj.type != "route") {
      fs.close(self.source());
      console.log("closing data file...");
    };
  });
});
