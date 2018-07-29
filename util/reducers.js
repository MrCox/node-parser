var types = {};
types.integer = {
  init  : function() {
            return { sum:0, count:0, mean:0, min:Infinity, max:-Infinity }
          },
  add   : function(p, v) {
            p.sum += v;
            p.count += 1;
            p.mean = p.sum / p.count;
            p.min = v <= p.min ? v : p.min;
            p.max = v >= p.max ? v : p.max;
            return p;
          },
  remove: function(p, v) {
            p.sum -= v;
            p.count -= 1;
            p.mean = p.sum / p.count;
            p.min = v <= p.min ? v : p.min;
            p.max = v >= p.max ? v : p.max;
            return p;
          }
};
types.number = types.integer;
types.year = types.integer;
types.date = {
  add   : function(p, v) {
            p.count += 1;
            p.start = v <= p.start ? v : p.start;
            p.end = v >= p.end ? v : p.end;
            p.duration = p.end - p.start;
            return p;
          },
  remove: function(p, v) {
            p.count -= 1;
            p.start = v <= p.start ? v : p.start;
            p.end = v >= p.end ? v : p.end;
            p.duration = p.end - p.start;
            return p;
          },
  init  : function() {
            return {count:0, start: new Date(0), end: new Date(0), duration: 0};
          }
};
types.string = {
  add   : function(p, v) {
            if (p.uniques.indexOf(v) == -1) p.uniques.push(v);
            p.uniqueEntries = p.uniques.length;
            p.count += 1;
            return p;
          },
  remove: function(p, v) {
            var ind = p.uniques.indexOf(v);
            p.uniques.splice(ind, 1);
            p.uniqueEntries = p.uniques.length;
            p.count -= 1;
            return p;
          },
  init  : function() {return { count:0, uniques:[], uniqueEntries:0 }},
};
module.exports = types;
