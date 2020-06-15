const mariaPool = require("./mariaPool");
const logger = require("../lib/logger");

var dmapDao = (function() {
  var selectCustomerInfo = function(doc, callback) {
    var params = [];
    mariaPool.getConnection("dmap", function(err, conn) {
      if (err) {
        logger.debug(err);
        callback(err);
      }

      var query =
        "SELECT " +
        "   pk.customer_id " +
        "FROM " +
        "   tb_project_keyword pk " +
        "WHERE pk.seq=?";
      if (doc.seq !== undefined) {
        params.push(doc.seq);
      }
      conn.query(query, params, function(err, results, fields) {
        if (err) {
          conn.release();
          logger.debug("[dmapDao] selectCustomerInfo error");
          callback(err);
        } else {
          conn.release();
          logger.info("[dmapDao] selectCustomerInfo Success");
          callback(null, results);
        }
      });
    });
  };

  return {
    selectCustomerInfo: selectCustomerInfo
  };
})();

if (exports) {
  module.exports = dmapDao;
}
