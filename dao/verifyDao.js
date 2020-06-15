const mariaPool = require("./mariaPool");
const logger = require("../lib/logger");

var verifyDao = (function() {
  /**
   * tb_collect_count 일주일 전 상시수집 데이터 삭제
   */
  var deleteWeekAgoCount = function(callback) {
    mariaPool.getConnection("verify", function(err, conn) {
      if (err) {
        logger.debug(err);
        callback(err);
      }

      var query =
        "DELETE FROM " +
        " tb_collect_count " +
        "WHERE " +
        " doc_datetime <= date_add(now(), interval -1 week) " +
        " AND always_yn='Y'";

      conn.query(query, function(err) {
        if (err) {
          conn.release();
          logger.debug("[verifyDao] deleteWeekAgoCount error");
          callback(err);
        } else {
          conn.release();
          logger.info("[verifyDao] deleteWeekAgoCount Success");
          callback(null);
        }
      });
    });
  };

  /**
   * tb_collect_doc 일주일 전 상시수집 데이터 삭제
   */
  var deleteWeekAgoData = function(callback) {
    mariaPool.getConnection("verify", function(err, conn) {
      if (err) {
        logger.debug(err);
        callback(err);
      }

      var query =
        "DELETE FROM " +
        " tb_collect_doc " +
        "WHERE " +
        " collect_datetime <= date_add(now(), interval -1 week) " +
        " AND always_yn='Y'";

      conn.query(query, function(err) {
        if (err) {
          conn.release();
          logger.debug("[verifyDao] deleteWeekAgoData error");
          callback(err);
        } else {
          conn.release();
          logger.info("[verifyDao] deleteWeekAgoData Success");
          callback(null);
        }
      });
    });
  };

  /**
   * 수집데이터 tb_collect_count 에 insert 하기
   */
  var insertCollectCount = function(doc, callback) {
    var params = [];

    mariaPool.getConnection("verify", function(err, conn) {
      if (err) {
        logger.debug(err);
        callback(err);
      }

      var query =
        "INSERT INTO tb_collect_count " +
        "   (customer_id, channel, keyword, doc_datetime, collect_type, always_yn, reg_dt) " +
        "VALUES " +
        "   (?, ?, ?, ?, ?, ?, now())";

      params = [
        doc.customer_id,
        doc.channel,
        doc.keyword,
        doc.doc_datetime,
        doc.collect_type,
        doc.always_yn
      ];

      conn.query(query, params, function(err) {
        if (err) {
          conn.release();
          conn.disconnect();
          logger.debug("[verifyDao] insertCollectCount error");
          callback(err);
        } else {
          conn.release();
          callback(null);
        }
      });
    });
  };

  /**
   * 수집데이터 중 datetime || title || content 잘못 수집된 데이터 tb_collect_doc 에 insert 하기
   */
  var insertCollectData = function(doc, callback) {
    var params = [];
    mariaPool.getConnection("verify", function(err, conn) {
      if (err) {
        logger.debug(err);
        callback(err);
      }

      var query =
        "INSERT IGNORE INTO tb_collect_doc " +
        "   (collect_datetime, doc_title, doc_content, doc_writer, doc_url, customer_id, doc_datetime, channel, attach_yn, collect_type, always_yn, md5, reg_dt) " +
        "VALUES " +
        "   (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())";

      params = [
        doc.collect_datetime,
        doc.doc_title,
        doc.doc_content,
        doc.doc_writer,
        doc.doc_url,
        doc.customer_id,
        doc.doc_datetime,
        doc.channel,
        doc.attach_yn,
        doc.collect_type,
        doc.always_yn,
        doc.md5
      ];

      conn.query(query, params, function(err) {
        if (err) {
          conn.release();
          logger.debug("[verifyDao] insertCollectData error");
          callback(err);
        } else {
          conn.release();
          callback(null);
        }
      });
    });
  };

  return {
    deleteWeekAgoCount: deleteWeekAgoCount,
    deleteWeekAgoData: deleteWeekAgoData,
    insertCollectCount: insertCollectCount,
    insertCollectData: insertCollectData
  };
})();

if (exports) {
  module.exports = verifyDao;
}
