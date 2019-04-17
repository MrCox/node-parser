var parser = require("../parser");

parser("./visits.json")
  .intype("json")
  .outtype("csv")
  .dump("visits.csv")
  .execute();
