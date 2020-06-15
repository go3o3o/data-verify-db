const fs = require("fs-extra");
const path = require("path");
const readline = require("readline");
const async = require("async");
const moment = require("moment");

const dmapDao = require("../dao/dmapDao");
const logger = require("./logger");

var scdParser = (function() {
  var parse = function(filePath, always_yn, callback) {
    var docs = [];
    var doc = {};
    async.waterfall(
      [
        function(callback) {
          var fileName = path.basename(filePath, "SCD");
          var seq = /B-(.*)-.*-.*-.*-/g.exec(fileName)[1];
          doc.seq = seq;

          dmapDao.selectCustomerInfo(doc, function(err, res) {
            if (err) {
              callback(err);
            } else {
              doc.customer_id = res[0].customer_id;
              callback(null, doc);
            }
          });
        },
        function(doc, callback) {
          var stream = fs.createReadStream(filePath);
          stream.on("error", function(err) {
            callback(err);
          });
          var rl = readline.createInterface({
            input: stream
          });
          var channel = "";
          rl.on("line", function(line) {
            if (line.indexOf("<DOCID>") != -1) {
              var doc_id = line
                .substring(line.indexOf("<DOCID>") + 7)
                .replace("\r", "");
              doc.doc_id = doc_id;
            } else if (line.indexOf("<SOURCE>") != -1) {
              var source = line
                .substring(line.indexOf("<SOURCE>") + 8)
                .replace("\r", "");
              channel = source;
            } else if (line.indexOf("<SECTION>") != -1) {
              var section = line
                .substring(line.indexOf("<SECTION>") + 9)
                .replace("\r", "");
              channel = channel + " > " + section;
              doc.channel = channel;
            } else if (line.indexOf("<KEYWORD>") != -1) {
              var keyword = line
                .substring(line.indexOf("<KEYWORD>") + 9)
                .replace("\r", "");
              doc.keyword = keyword;
            } else if (line.indexOf("<TITLE>") != -1) {
              var doc_title = line.substring(line.indexOf("<TITLE>") + 7);
              doc.doc_title = doc_title;
            } else if (line.indexOf("<CONTENT>") != -1) {
              var doc_content = line.substring(line.indexOf("<CONTENT>") + 9);
              doc.doc_content = doc_content;
            } else if (line.indexOf("<DATE>") != -1) {
              var datetime = line
                .substring(line.indexOf("<DATE>") + 6)
                .replace("\r", "");
              var yyyy = datetime.substring(0, 4);
              var MM = datetime.substring(4, 6);
              var DD = datetime.substring(6, 8);

              var doc_datetime = yyyy + "-" + MM + "-" + DD;

              doc.doc_datetime = doc_datetime;
            } else if (line.indexOf("<URL>") != -1) {
              var doc_url = line
                .substring(line.indexOf("<URL>") + 5)
                .replace("\r", "");
              doc.doc_url = doc_url;
            } else if (line.indexOf("<WRITER>") != -1) {
              var doc_writer = line
                .substring(line.indexOf("<WRITER>") + 8)
                .replace("\r", "");
              doc.doc_writer = doc_writer;

              doc.collect_type = 0;
              doc.always_yn = always_yn;

              doc.collect_datetime = moment().format("YYYY-MM-DD");

              docs.push(doc);
            }
          }).on("close", function() {
            logger.debug(
              "[scdParser] " +
                filePath +
                " has parsed: " +
                docs.length +
                " docs."
            );
            callback(null, docs);
          });
        }
      ],
      callback
    );
  };
  return {
    parse: parse
  };
})();

if (exports) {
  module.exports = scdParser;
}
