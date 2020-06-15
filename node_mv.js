const fs = require("fs");

const fileNm = "./data-verify-db.2020-06-09";
const resultNm = "mv.sh";

const re = /(.*)\/D-/;

let datas = fs
  .readFileSync(fileNm)
  .toString()
  .split("\n");

let parsing = [];

for (data of datas) {
  let match = re.exec(data);
  data = data.replace(match[1], "./crawldoc/node_always_ok");
  data = "mv " + data + " ./crawldoc/node_always/. \n";
  parsing.push(data);
}

// for (data of parsing) {
//   console.log(data);
// }

fs.writeFileSync(resultNm, parsing, "utf8");
