const fs = require("fs-extra");
const async = require("async");
const crypto = require("crypto");
const utf8 = require("utf8");
const shelljs = require("shelljs");
const dateformat = require("dateformat");

const logger = require("./lib/logger");
const scdParser = require("./lib/scdParser");
const verifyDao = require("./dao/verifyDao");

var dmapPaths = ["./crawldoc/dmap_always", "./crawldoc/dmap_retroactive"];
var dmapOkPaths = [
  "./crawldoc/dmap_always_ok",
  "./crawldoc/dmap_retroactive_ok"
];

var getFiles = function(path, files) {
  fs.readdirSync(path).forEach(function(file) {
    var subpath = path + "/" + file;
    if (fs.lstatSync(subpath).isDirectory()) {
      getFiles(subpath, files);
    } else {
      files.push(path + "/" + file);
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
  if (obj !== null && obj !== undefined) {
    return true;
  } else {
    return false;
  }
};

async.waterfall(
  [
    function(callback) {
      logger.debug("[dmap2db] Step #1. 일주일 전 상시수집 데이터 삭제");
      logger.debug("[dmap2db] Step #1-1. DELETE FROM tb_collect_count");
      verifyDao.deleteWeekAgoCount(callback);
    },
    function(callback) {
      logger.debug("[dmap2db] Step #1-2. DELETE FROM tb_collect_doc");
      verifyDao.deleteWeekAgoData(callback);
    },
    function(callback) {
      logger.debug("[dmap2db] Step #2. 수집된 파일 파싱하여 배열에 담기");
      var files_always = [];
      getFiles(dmapPaths[0], files_always);

      logger.debug("[dmap2db] Step #3. 상시 수집데이터 파싱");
      var datas = [];
      var counts = [];

      async.each(
        files_always,
        function(file, callback) {
          scdParser.parse(file, "Y", function(err, scd) {
            async.each(scd, function(doc, err) {
              doc.md5 = md5Generator(
                doc.channel,
                doc.doc_datetime,
                doc.customer_id
              );

              if (
                !__notNull(doc.doc_datetime) ||
                !__notNull(doc.doc_title) ||
                !__notNull(doc.doc_content)
              ) {
                doc.collect_datetime = dateformat(new Date(), "yyyy-mm-dd");
                datas.push(doc);
              } else {
                doc.collect_datetime = doc.doc_datetime;
              }
              counts.push(doc);
            });
            callback(null);
          });
        },
        function(err) {
          if (err) {
            callback(err);
          } else {
            callback(null, counts, datas, files_always);
          }
        }
      );
    },
    function(counts, datas, files_always, callback) {
      logger.debug("[dmap2db] Step #4. 소급 수집데이터 파싱");
      var files_retro = [];
      getFiles(dmapPaths[1], files_retro);

      async.each(
        files_retro,
        function(file, callback) {
          scdParser.parse(file, "N", function(err, scd) {
            async.each(scd, function(doc, err) {
              doc.md5 = md5Generator(
                doc.channel,
                doc.doc_datetime,
                doc.customer_id
              );
              if (
                !__notNull(doc.doc_datetime) ||
                !__notNull(doc.doc_title) ||
                !__notNull(doc.doc_content)
              ) {
                doc.collect_datetime = dateformat(new Date(), "yyyy-mm-dd");
                datas.push(doc);
              } else {
                doc.collect_datetime = doc.doc_datetime;
              }
              counts.push(doc);
            });
            callback(null);
          });
        },
        function(err) {
          if (err) {
            callback(err);
          } else {
            callback(null, counts, datas, files_always, files_retro);
          }
        }
      );
    },
    function(counts, datas, files_always, files_retro, callback) {
      logger.debug("[dmap2db] Step #5. DB에 넣기 ");
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
          callback(null, files_always, files_retro);
        }
      );
    },
    function(files_always, files_retro, callback) {
      async.each(files_always, function(file, callback) {
        shelljs.mv(file, dmapOkPaths[0]);
      });
      async.each(files_retro, function(file, callback) {
        shelljs.mv(file, dmapOkPaths[1]);
      });
      callback(null);
    }
  ],
  function(err) {
    if (err) {
      logger.error("Error occurred : " + err);
      process.exit(-1);
    } else {
      logger.debug("[dmap2db] Finished");

      process.exit(0);
    }
  }
);
