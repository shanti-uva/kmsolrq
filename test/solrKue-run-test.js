var testCase = require('nodeunit').testCase;
var updateAssetKue = require('../logic/solrUpdateAssetIndexKeyKue.js');
var createAssetKue = require('../logic/solrWriteKmapAssetKue.js');
var kmapsKue = require('../logic/solrWriteKmapAssetKue.js');
var solr = require('solr-client');
var async = require('async');
var kue = require('kue');
var _ = require('lodash');

const TIMEOUT = 50000;
const CHECK_DELAY = 1000;
const REVERSE = false;
const SHUFFLE = false;

const KMTERMS_DEV_UNAUTH = {
  'host': 'ss251856-us-east-1-aws.measuredsearch.com',
  'port': 443,
  'family': 4,
  'path': '/solr',
  'secure': true,
  'core': 'kmterms_dev',
  'solrVersion': '6.4.2'
};

const KMASSETS_DEV_AUTH = {
  'host': 'ss251856-us-east-1-aws.measuredsearch.com',
  'port': 443,
  'family': 4,
  'path': '/solr',
  'secure': true,
  'core': 'kmassets_dev',
  'solrVersion': '6.4.2'
};

const KMTERMS_STAGE_UNAUTH = {
  'host': 'ss395824-us-east-1-aws.measuredsearch.com',
  'port': 443,
  'family': 4,
  'path': '/solr',
  'secure': true,
  'core': 'kmterms_stage',
  'solrVersion': '6.4.2'
};

const KMASSETS_STAGE_AUTH = {
  'host': 'ss395824-us-east-1-aws.measuredsearch.com',
  'port': 443,
  'family': 4,
  'path': '/solr',
  'secure': true,
  'core': 'kmassets_stage',
  'solrVersion': '6.4.2'
};

const KMTERMS_PROD_UNAUTH = {
  'host': 'ss395824-us-east-1-aws.measuredsearch.com',
  'port': 443,
  'family': 4,
  'path': '/solr',
  'secure': true,
  'core': 'kmterms_prod',
  'solrVersion': '6.4.2'
};

const KMASSETS_PROD_AUTH = {
  'host': 'ss395824-us-east-1-aws.measuredsearch.com',
  'port': 443,
  'family': 4,
  'path': '/solr',
  'secure': true,
  'core': 'kmassets',
  'solrVersion': '6.4.2'
};

const KMTERMS_PREDEV_UNAUTH = {
  'host': 'ss251856-us-east-1-aws.measuredsearch.com',
  'port': 443,
  'family': 4,
  'path': '/solr',
  'secure': true,
  'core': 'kmterms_predev',
  'solrVersion': '6.4.2'
};

const KMASSETS_PREDEV_AUTH = {
  'host': 'ss251856-us-east-1-aws.measuredsearch.com',
  'port': 443,
  'family': 4,
  'path': '/solr',
  'secure': true,
  'core': 'kmassets_predev',
  'solrVersion': '6.4.2'
};


// TODO: ys2n: need to fix service name, to have a more proper Idenitifier (per kmaps asset type).


var configSet = {
  "read_client" :solr.createClient(KMTERMS_DEV_UNAUTH),
  "write_client":solr.createClient(KMASSETS_DEV_AUTH),
  "service_name":"kmaps-dev_shanti_virginia_edu",
  "baseurl": "https://mandala-dev.shanti.virginia.edu",
  "write_user": "solradmin",
  "write_pass": "IdskBsk013"
}

configSet.write_client.basicAuth(configSet.write_user,configSet.write_pass);

var config = {
  service : configSet.service_name,
  baseurl : configSet.baseurl,
  read_client : configSet.read_client,
  write_client: configSet.write_client,
  overwrite: function(src, old) {

    if (!old) {
      return true;
    }

    if (src.uid) {
      if (src.uid.startsWith("dev_")) {
        return true;
      }
    } else {
      return false;
    }

  },
  concurrency: 10
};

module.exports = testCase({
  "big run": function (test) {
    console.log("big run started");
    var queue = createAssetKue.createQueue();
    var qlist = [
      // "name:bumthang",
      // "name:Chukha",
      // "ancestor_uids_generic:(places-427 subjects-8260)",
      "uid:places-637",
      // "uid:places-637",
      // "name:lhasa",
      // "places-10000",
      // "subjects-9297",
      "tree:terms",
      // "tree:places",
      // "tree:subjects",
      // "tree:terms",
      //"uid:subjects-7299",
      //"ancestor_uids_generic:(subjects-20)",
      //"tree:subjects",
      // "tree:terms",
      // "tree:terms",
      // "ancestor_uids_generic:(places-427 subjects-8260 subjects-20 places-2)",
      // "name:lhasa",
    ];
    var length = qlist.length;
    async.series(
      [
        function(done) {
	  let wait = 0;
          console.error("ranging to eliminate old jobs");
          kue.Job.rangeByState( 'active', 0, 100, 'asc', function( err, jobs ) {
            console.error("Removing " + jobs.length + " active jobs");
            jobs.forEach( function( job ) {
              // console.log("job: " + job.id);
              job.remove( function(){
                 // console.log( 'removed ', job.id );
              });
            });
          });
          kue.Job.rangeByState( 'inactive', 0, 60000, 'asc', function( err, jobs ) {
            console.error("Removing " + jobs.length + " inactive jobs");
	    let wait = jobs.length;
	    const log_period = 10;
	    let count = 0;
            jobs.forEach( function( job ) {
              job.remove( function(){
		if (count++%log_period === 0 || count >= jobs.length ) {
                 console.log( 'removed ', count, ' of ', jobs.length, ' jobs');
		}
              });
            });
	    let fullwait = wait + 500;
	    console.log ("waiting " + fullwait + " on removing " + jobs.length + " inactive jobs");
            setTimeout(function() {
               done();
	    }
            ,fullwait);
          });
	},
        function (done) {

        console.error("qlist: " + JSON.stringify(qlist));

        async.mapSeries(qlist, function (q, next) {
            console.error("### calling generateJobspecs with " + q );
            kmapsKue.generateJobspecs(config, q, function (err, subquerylist) {
              // console.error("mapping with " + JSON.stringify(subquerylist));

              if (REVERSE) {
                subquerylist = _.reverse(subquerylist);
              }

              if (SHUFFLE) {
                subquerylist = _.shuffle(subquerylist);
              }


	//	throw new Error("stop it");
              async.mapSeries(subquerylist,

                function (query, next2) {
                  console.error("### calling addJob with " + JSON.stringify(query));
                  createAssetKue.addJob(queue, query, function (err, ret) {
                    // console.error("CALLBACK FOR " + JSON.stringify(query));
                    if (err) {
                      console.error("error object returned " + err);
                      next2(err);
                    }
                    if (ret) {
                      // console.log("object returned");
                      /* console.dir(ret); */
                      next2(null, ret);
                    }
                  });
                },
                function(err, ret) {
                  console.error(err);
                  // console.error("ret: " + ret);
                  next(err,ret);
                }
              );
            });
          },

          function (err, list) {
            console.error("done with Queueing!")
            if (err) console.error(err);
            // console.dir(list);
            done();
          });
      },
      function (done) {
        // process the queue
        console.error("processing the queue!");
        createAssetKue.processQueue(config, queue, function (err, ret) {
          // console.log("processQueue processed: " + ret);
          if (err) {
            test.ok(false);
          } else {
            test.ok(true);
          }
        });
        test.expect(length + 1);

        var shutdown = function () {
          queue.shutdown(function (err, ret) {
            if (err) {
              console.error("queue shutdown error: " + err);
            }
            if (ret) {
              console.error("queue shutdown return: " + ret);
            }
            console.log('[ All jobs finished. Kue is shut down. ]');
            test.done();
          });
        };

        var checkCount = function() {
          var count = 0;
          var counter = function() {
            count = count + 1;
            return count;
          }
          return counter;
        }();

        var checkforShutdown = setInterval(function () {
          queue.inactiveCount(function (err, queued) {
            var checks = checkCount();
            if (checks % 10 === 0) {
              var read_host = config.read_client.options.host;
              var read_core = config.read_client.options.core;
              var write_host = config.write_client.options.host;
              var write_core = config.write_client.options.core;
              console.error("checkforshutdown " + new Date().toISOString() + " [ inactives: " + queued + "\terr: " + err + "]  checkcount: " + checks + "(read: " + read_host + " " + read_core + " write: " + write_host + " " + write_core)

            }

            if (queued === 0) {
              queue.activeCount(function (err, actives) {
                console.error("checkforshutdown [ activeCount: " + actives + "\terr: " + err + "]");
                if (actives === 0) {
                  shutdown();
                  clearInterval(checkforShutdown);
                }
              });
            }
          });

        }, CHECK_DELAY);

        // setInterval(function () {
        //   console.error("TIMEOUT");
        //   shutdown();
        //   clearInterval(checkforShutdown);
        // }, TIMEOUT);

      }]
  );
}
});
