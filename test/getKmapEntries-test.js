var testCase = require('nodeunit').testCase;
var solrKue = require('../logic/solrWriteKmapAssetKue.js');
var solr = require('solr-client');
const KMTERMS_DEV_UNAUTH = {
  'host': 'ss251856-us-east-1-aws.measuredsearch.com',
  'port': 443,
  'path': '/solr',
  'secure': true,
  'core': 'kmterms_dev'
};

  module.exports = testCase({

    "getKmapEntries: single entry": function (test) {
      var client = solr.createClient(KMTERMS_DEV_UNAUTH);
      test.expect(2);
      const kmapid = "places-8862";

      solrKue.getKmapEntries(client, "uid:" + kmapid, 10, 0, function(err, ret) {
          if (err) {
            console.dir(err);
            test.fail();
          } else {
            // console.dir (ret);
            test.ok(ret != null);
            test.equal( ret[0].uid,kmapid, "wrong uid returned");
          }
          test.done();
      });
    },


    "getKmapEntries: multiple entries" : function(test) {
      var client = solr.createClient(KMTERMS_DEV_UNAUTH);
      test.expect(2);
      const query = "header:lhasa*";

      solrKue.getKmapEntries(client, query, 100, 0, function(err,resp) {
        if(err) { console.dir(err); test.fail() }
        else {
          test.ok(Array.isArray(resp),"Response must be an array");
          test.equals(resp.length,10, "Wrong number of responses");

          for (var i=0; i < resp.length; i++) {
            var entry = resp[i];
            console.log(entry.uid + " " + entry.header);
          }

        }
        test.done();
      })


    }
  });
