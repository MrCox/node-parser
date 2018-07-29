var parser = require("./parser");

parser("./Consumer_Complaints.csv")
  .intype("csv")
  .outtype("json")
  .dump("Consumer_Complaints.json")
  .execute();
