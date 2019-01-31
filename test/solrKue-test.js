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

const KMASSETS_DEV_AUTH = {
  'host': 'ss251856-us-east-1-aws.measuredsearch.com',
  'port': 443,
  'path': '/solr',
  'secure': true,
  'core': 'kmassets_dev',
};

module.exports = testCase({

  // "need to check that jobspecs include query": function (test) {
  //   var queue = solrKue.createQueue();
  //   var badspecs = {};
  //   var goodspecs = {
  //     query: "*:*"
  //   };
  //   // console.dir(solrKue.addJob);
  //
  //   test.expect(4);
  //
  //   solrKue.addJob(queue, badspecs, function (err, job) {
  //     test.ok(err != null);
  //     test.ok(job == null);
  //   });
  //
  //   solrKue.addJob(queue, goodspecs, function (err, job) {
  //     test.ok(err == null);
  //     test.ok(job != null);
  //     console.log("job returned");
  //     // console.dir(job);
  //   });
  //
  //   test.done();
  // },

  // "need to handle null jobspecs": function (test) {
  //   var queue = null;
  //   var jobspecs = null;
  //   test.expect(2);
  //   solrKue.addJob(queue, jobspecs, function (err, job) {
  //     console.log(err);
  //     console.log(job);
  //     test.ok(err !== null);
  //     test.ok(job == null);
  //   });
  //   test.done();
  // },
  //
  // "generate jobs": function (test) {
  //
  //   var write_client = solr.createClient(KMASSETS_DEV_AUTH);
  //
  //   var read_client = solr.createClient(KMTERMS_DEV_UNAUTH);
  //
  //   var config = {
  //     write_client: write_client,
  //     read_client: read_client
  //   };
  //
  //   solrKue.generateJobspecs(config,
  //     "tree: (places subjects)",
  //     function () {
  //       console.log("DONE CALLBACK");
  //       test.done();
  //     },
  //     function (err, obj) {
  //       console.log("PER JOB CALLBACK: ");
  //       console.dir(err);
  //       // console.dir(obj);
  //       console.log( "RESULT COUNT: " + obj.response.docs.length);
  //     }
  //   )
  // }


});

