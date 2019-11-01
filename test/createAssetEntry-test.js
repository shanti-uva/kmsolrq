const DEBUG = false;
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

    "createAssetEntry: single entry": function (test) {
      var client = solr.createClient(KMTERMS_DEV_UNAUTH);
      var config = {
          service : "kmaps",
          baseurl : "https://mandala.shanti.virginia.edu"
      };
      const kmapid = "terms-8862";
      // const kmapid = "places-437";
      // const kmapid = "terms-115326";


      test.expect(2);
      solrKue.getKmapEntries(client, "uid:" + kmapid, 10, 0, function(err, resp) {
          if (err) {
            console.dir(err);
            test.fail();
          } else {
            var kmapEntry = resp[0];

            if (DEBUG) console.error(JSON.stringify(kmapEntry, undefined, 2));

            solrKue.createAssetEntry(kmapEntry, config, function(err, assetEntry) {
              if (err) {
                console.dir(err);
                test.fail(err);
              } else {
               console.dir(assetEntry);
               test.ok(assetEntry);
               test.equals(assetEntry.kmapid[0], kmapid);
              }
            });
          }
          test.done();
      });
    },


    "getKmapEntries: multiple entries" : function(test) {
      var client = solr.createClient(KMTERMS_DEV_UNAUTH);
      test.expect(2);
      const query = "text:lhasa";

      solrKue.getKmapEntries(client, query, 10, 0, function(err,resp) {
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


