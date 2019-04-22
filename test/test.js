var parser = require("../parser");

/*parser("./visits.json")
  .intype("json")
  .outtype("csv")
  .newline(false)
  .dump("_visits.csv")
  .execute();*/

parser("./visits.json")
  .newline(false)
  .intype("json")
  .outtype("csv")
  .dump("visits.csv")
  .execute();
