const mariaPool = require("./mariaPool");
const logger = require("../lib/logger");

var nodeDao = (function() {
  /**
   *
   */
  var selectChannelInfo = function(doc, callback) {
    var params = [];
    mariaPool.getConnection("node", function(err, conn) {
      var query =
        "SELECT " +
        " cce.type_cd " +
        "FROM " +
        " tb_crawl_channel cc, tb_crawl_channel_engine cce " +
        "WHERE " +
        "cc.name=? " +
        "AND cc.chrome_use_yn='Y' " +
        "AND cc.seq=cce.channel_seq " +
        "AND cce.type_cd!='CET001'";

      if (doc.channel !== undefined) {
        params.push(doc.channel);
      }
      conn.query(query, params, function(err, results, fields) {
        if (err) {
          callback(err);
        } else {
          if (results !== undefined) {
            if (results[0].type_cd === "CET002") {
              callback(null, 1);
            } else if (results[0].type_cd === "CET003") {
              callback(null, 2);
            }
          } else {
            callback(`ERR_DB_SELECT_CHANNEL ${doc.channel}`);
          }
        }
      });
    });
  };

  return {
    selectChannelInfo: selectChannelInfo
  };
})();

if (exports) {
  module.exports = nodeDao;
}
