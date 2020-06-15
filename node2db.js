const fs = require("fs-extra");
const async = require("async");
const crypto = require("crypto");
const utf8 = require("utf8");
const moment = require("moment");
const shelljs = require("shelljs");

const logger = require("./lib/logger");
const verifyDao = require("./dao/verifyDao");

var nodePaths = ["./crawldoc/node_always", "./crawldoc/node_retroactive"];
var nodeOkPaths = [
  "./crawldoc/node_always_ok",
  "./crawldoc/node_retroactive_ok"
];

var getFiles = function(path, files) {
  fs.readdirSync(path).forEach(function(file) {
    var subpath = path + "/" + file;
    if (fs.lstatSync(subpath).isDirectory()) {
      getFiles(subpath, files);
    } else {
      if (file !== ".DS_Store") {
        files.push(path + "/" + file);
      }
    }
  });
};

var md5Generator = function(channel, doc_datetime, customer_id) {
  var arrAsString =
    "['" + channel + "', '" + doc_datetime + "', '" + customer_id + "']";
  var md5 = crypto
    .createHash("md5")
    .update(utf8.encode(arrAsString))
    .digest("hex");
  return md5;
};

var __notNull = function(obj) {
  if (obj !== null && obj !== undefined && obj !== "") {
    return true;
  } else {
    return false;
  }
};

async.waterfall(
  [
    function(callback) {
      logger.debug("[node2db] Step #1. 일주일 전 상시수집 데이터 삭제");
      logger.debug("[node2db] Step #1-1. DELETE FROM tb_collect_count");
      verifyDao.deleteWeekAgoCount(callback);
    },
    function(callback) {
      logger.debug("[node2db] Step #1-2. DELETE FROM tb_collect_doc");
      verifyDao.deleteWeekAgoData(callback);
    },
    function(callback) {
      logger.debug("[node2db] Step #2. 수집된 파일 파싱하여 배열에 담기");
      var files_always = [];
      var files_retro = [];
      getFiles(nodePaths[0], files_always);
      getFiles(nodePaths[1], files_retro);
      callback(null, files_always, files_retro);
    },
    function(files_always, files_retro, callback) {
      var counts = [];
      var datas = [];

      logger.debug(
        "[node2db] Step #3. 상시 수집데이터 파싱 " + files_always.length
      );
      async.each(files_always, function(file, callback) {
        var doc = {};
        var goodData = true;

        var json = fs.readJSONSync(file);
        // logger.info(file);

        if (Array.isArray(json)) {
          for (var i = 0; i < json.length; i++) {
            goodData = true;
            doc = {};
            var datetime_check = "";
            var datetime = moment().format("YYYY-MM-DD");
            var customer_id = "";
            var keyword = "";

            if (json[i].customer !== undefined) {
              customer_id = json[i].customer;
            } else if (json[i].customer_id !== undefined) {
              customer_id = json[i].customer_id;
            }

            if (json[i].keyword !== undefined) {
              keyword = json[i].keyword;
            } else if (json[i].search_keyword_text !== undefined) {
              keyword = json[i].search_keyword_text;
            }

            if (__notNull(json[i].doc_datetime)) {
              datetime_check = json[i].doc_datetime;
            }
            if (__notNull(json[i].pub_year)) {
              datetime_check =
                json[i].pub_year +
                "-" +
                json[i].pub_month +
                "-" +
                json[i].pub_day;
            }

            if (datetime_check.includes("Invalid")) {
              goodData = false;
            } else {
              datetime = moment(new Date(datetime_check)).format("YYYY-MM-DD");
            }

            if (
              !__notNull(json[i].doc_title || !__notNull(json[i].doc_content))
            ) {
              goodData = false;
            }

            doc.doc_title = json[i].doc_title;
            doc.doc_content = json[i].doc_content;
            doc.doc_writer = json[i].doc_writer;
            doc.doc_url = json[i].doc_url;
            doc.channel = json[i].source;
            doc.customer_id = customer_id;
            doc.keyword = keyword;
            doc.collect_type = 1;
            doc.always_yn = "Y";
            doc.md5 = md5Generator(doc.channel, datetime, doc.customer_id);

            if (!goodData) {
              doc.collect_datetime = datetime;
              doc.doc_datetime = "";
              datas.push(doc);
            } else {
              doc.collect_datetime = moment().format("YYYY-MM-DD");
              doc.doc_datetime = datetime;
            }
            counts.push(doc);
          }
        } else {
          goodData = true;
          doc = {};
          var datetime_check = "";
          var datetime = moment().format("YYYY-MM-DD");
          var customer_id = "";
          var keyword = "";

          if (json.customer !== undefined) {
            customer_id = json.customer;
          } else if (json.customer_id !== undefined) {
            customer_id = json.customer_id;
          }
          if (json.keyword !== undefined) {
            keyword = json.keyword;
          } else if (json.search_keyword_text !== undefined) {
            keyword = json.search_keyword_text;
          }

          if (__notNull(json.doc_datetime)) {
            datetime_check = json.doc_datetime;
          }
          if (__notNull(json.pub_year)) {
            datetime_check =
              json.pub_year + "-" + json.pub_month + "-" + json.pub_day;
          }

          if (datetime_check.includes("Invalid")) {
            goodData = false;
          } else {
            datetime = moment(new Date(datetime_check)).format("YYYY-MM-DD");
          }

          if (!__notNull(json.doc_title || !__notNull(json.doc_content))) {
            goodData = false;
          }

          doc.doc_title = json.doc_title;
          doc.doc_content = json.doc_content;
          doc.doc_writer = json.doc_writer;
          doc.doc_url = json.doc_url;
          doc.channel = json.source;
          doc.customer_id = customer_id;
          doc.keyword = keyword;
          doc.collect_type = 1;
          doc.always_yn = "Y";
          doc.md5 = md5Generator(doc.channel, datetime, doc.customer_id);

          if (!goodData) {
            doc.collect_datetime = datetime;
            doc.doc_datetime = "";
            datas.push(doc);
          } else {
            doc.collect_datetime = moment().format("YYYY-MM-DD");
            doc.doc_datetime = datetime;
          }
          counts.push(doc);
        }
      });
      logger.debug("[node2db] Step #4. 소급 수집데이터 파싱");
      async.each(files_retro, function(file, callback) {
        var doc = {};
        // logger.info(file);
        var json = fs.readJSONSync(file);

        if (Array.isArray(json)) {
          for (var i = 0; i < json.length; i++) {
            goodData = true;
            doc = {};
            var datetime_check = "";
            var datetime = moment().format("YYYY-MM-DD");
            var customer_id = "";
            var keyword = "";

            if (json[i].customer !== undefined) {
              customer_id = json[i].customer;
            } else if (json[i].customer_id !== undefined) {
              customer_id = json[i].customer_id;
            }

            if (json[i].keyword !== undefined) {
              keyword = json[i].keyword;
            } else if (json[i].search_keyword_text !== undefined) {
              keyword = json[i].search_keyword_text;
            }

            if (__notNull(json[i].doc_datetime)) {
              datetime_check = json[i].doc_datetime;
            }
            if (__notNull(json[i].pub_year)) {
              datetime_check =
                json[i].pub_year +
                "-" +
                json[i].pub_month +
                "-" +
                json[i].pub_day;
            }

            if (datetime_check.includes("Invalid")) {
              goodData = false;
            } else {
              datetime = moment(new Date(datetime_check)).format("YYYY-MM-DD");
            }

            if (
              !__notNull(json[i].doc_title || !__notNull(json[i].doc_content))
            ) {
              goodData = false;
            }

            doc.doc_title = json[i].doc_title;
            doc.doc_content = json[i].doc_content;
            doc.doc_writer = json[i].doc_writer;
            doc.doc_url = json[i].doc_url;
            doc.channel = json[i].source;
            doc.customer_id = customer_id;
            doc.keyword = keyword;
            doc.collect_type = 1;
            doc.always_yn = "Y";
            doc.md5 = md5Generator(doc.channel, datetime, doc.customer_id);

            if (!goodData) {
              doc.collect_datetime = datetime;
              doc.doc_datetime = "";
              datas.push(doc);
            } else {
              doc.collect_datetime = moment().format("YYYY-MM-DD");
              doc.doc_datetime = datetime;
            }
            counts.push(doc);
          }
        } else {
          goodData = true;
          doc = {};
          var datetime_check = "";
          var datetime = moment().format("YYYY-MM-DD");
          var customer_id = "";
          var keyword = "";

          if (json.customer !== undefined) {
            customer_id = json.customer;
          } else if (json.customer_id !== undefined) {
            customer_id = json.customer_id;
          }
          if (json.keyword !== undefined) {
            keyword = json.keyword;
          } else if (json.search_keyword_text !== undefined) {
            keyword = json.search_keyword_text;
          }

          if (__notNull(json.doc_datetime)) {
            datetime_check = json.doc_datetime;
          }
          if (__notNull(json.pub_year)) {
            datetime_check =
              json.pub_year + "-" + json.pub_month + "-" + json.pub_day;
          }

          if (datetime_check.includes("Invalid")) {
            goodData = false;
          } else {
            datetime = moment(new Date(datetime_check)).format("YYYY-MM-DD");
          }

          if (!__notNull(json.doc_title || !__notNull(json.doc_content))) {
            goodData = false;
          }

          doc.doc_title = json.doc_title;
          doc.doc_content = json.doc_content;
          doc.doc_writer = json.doc_writer;
          doc.doc_url = json.doc_url;
          doc.channel = json.source;
          doc.customer_id = customer_id;
          doc.keyword = keyword;
          doc.collect_type = 1;
          doc.always_yn = "Y";
          doc.md5 = md5Generator(doc.channel, datetime, doc.customer_id);

          if (!goodData) {
            doc.collect_datetime = datetime;
            doc.doc_datetime = "";
            datas.push(doc);
          } else {
            doc.collect_datetime = moment().format("YYYY-MM-DD");
            doc.doc_datetime = datetime;
          }
          counts.push(doc);
        }
      });
      callback(null, counts, datas, files_always, files_retro);
    },
    function(counts, datas, files_always, files_retro, callback) {
      logger.debug("[node2db] Step #5. DB에 넣기 ");
      logger.debug(" ### data length: " + datas.length);

      async.each(datas, function(doc, callback) {
        verifyDao.insertCollectData(doc, function(err, res) {
          if (err) {
            callback(err);
          } else {
            callback(null);
          }
        });
      });

      logger.debug(" ### count length: " + counts.length);
      async.each(
        counts,
        function(doc, callback) {
          verifyDao.insertCollectCount(doc, function(err, res) {
            if (err) {
              callback(err);
            } else {
              callback(null);
            }
          });
        },
        function(err) {
          if (err) {
            callback(err);
          } else {
            callback(null, files_always, files_retro);
          }
        }
      );
    },
    function(files_always, files_retro, callback) {
      async.each(files_always, function(file, callback) {
        shelljs.mv(file, nodeOkPaths[0]);
      });
      async.each(files_retro, function(file, callback) {
        shelljs.mv(file, nodeOkPaths[1]);
      });
      callback(null);
    }
  ],
  function(err) {
    if (err) {
      logger.error("Error occurred : " + err);
      process.exit(-1);
    } else {
      logger.info("[node2db] All files processed.");
      logger.debug("[node2db] Finished");

      process.exit(0);
    }
  }
);
