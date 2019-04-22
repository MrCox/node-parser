var parser = require("../parser");

/*parser("./visits.json")
  .intype("json")
  .outtype("csv")
  .newline(false)
  .dump("_visits.csv")
  .execute();*/

parser("./newvisits.json")
  .intype("json")
  .outtype("csv")
  .dump("visits.csv")
  .execute();
