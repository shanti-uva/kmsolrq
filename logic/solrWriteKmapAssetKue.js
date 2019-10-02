const DEBUG = false;
const DEFAULT_ROWS = 500;
const DEFAULT_CONCURRENCY = 3;
const FORCE_OVERWRITE = false;
const SCHEMA_VERSION = 14;

var kue = require('kue');
var check = require('type-check').typeCheck;
var solr = require('solr-client');
var _ = require('lodash');
var async = require('async');
var xml2js = require('xml2js');
var striptags = require('striptags');
var localStorage;

if (typeof localStorage === "undefined" || localStorage === null) {
  var LocalStorage = require('node-localstorage').LocalStorage;
  localStorage = new LocalStorage('./scratch');
}

var createQueue = exports.createQueue = function () {
  var queue = kue.createQueue();
  queue.on("job enqueue", function () {
    console.log("job queued " + JSON.stringify(arguments));
  }).on("job complete", function () {
    console.log("job done " + JSON.stringify(arguments));
  }).on('progress', function (progress, data) {
    console.log('\r  job #' + job.id + ' ' + progress + '% complete with data ', data);
  });

  return queue;
};

var addJob = exports.addJob = function (queue, jobspecs, cb) {

  // console.log("jobspecs = " + JSON.stringify(jobspecs));
  // if (!check('{ query: String }', jobspecs)) {
  //   cb("jobspecs must have query parameter", null);
  //   return;
  // }

  var job = queue.create(
    'process',
    jobspecs
  ).removeOnComplete(true).save();

  // TODO: handle failures.
  cb(null, job);
};

var getKmapEntries = exports.getKmapEntries =
  function (read_client, query, rows, start, callback) {
    console.log("getKmapEntries(): ARGUMENTS: " + JSON.stringify({
      query: query,
      rows: rows,
      start: start
    }, undefined, 2));
    var q = read_client.createQuery().q(query).matchFilter("block_type", "parent").rows(rows).start(start);

    async.setImmediate(function () {
      read_client.search(q, function (err, resp) {
        if (err) {
          console.error("ERROR reading: " + err);
        }

        console.log("####### Response received");
        console.log("numFound = " + resp.response.numFound);
        console.log("start = " + resp.response.start);

        async.setImmediate(function () {
          if (!err) console.log("calling back: " + resp.response.docs.length + " docs");

          // Let's cache a map of the uid's and headers
          for (var i=0 ; i < resp.response.docs.length; i++){
            var doc = resp.response.docs[i];

            if (!localStorage.getItem(doc.uid)) {
              console.log("Caching: " + doc.uid + " = " + doc.header + "  kmapid:  " + JSON.stringify(doc.kmapid) );
              localStorage.setItem(doc.uid, doc.header);
            }
          }
          callback(err, resp.response.docs);
        });
      });
    });
  };


var recordKmap = exports.recordKmap = function recordKmap(names, ids, domain) {

  if (typeof names !== "object") {
    console.log("names is " + JSON.stringify(names));
    return;
  }

  if (typeof ids !== "object") {
    console.log("ids is " + JSON.stringify(ids));
    return;
  }

  if (names.length !== ids.length) {
    console.log("lengths of the arrays do not match!");
    return;
  }


  for (var i=0; i < names.length; i++) {
    var name = names[i];
    var id = ids[i];
    var uid = domain + "-" + id;
    var old = localStorage.getItem(uid);


    if (DEBUG) {
      console.log(">>> NAME: " + name);
      console.log(">>> ID:" + id);
      console.log(">>> UID: " + uid);
    }

    if (old) {
      if (old !== name) {
        console.log ("#######################################################");
        var msg = "############## NAME MISMATCH: old=" + old + " new=" + name;
        console.log(msg);
        // throw new Error(msg);
      }
      // console.log("SKIPPING: " + uid + "=>" + name);
    } else {
      console.log("PUTTING: " + uid + "=>" + name);
      localStorage.setItem(uid,name);
    }

  }
}




var lookupKmapIds = exports.getlookupKmapIds =
  function(kmapids) {
      console.log("lookupKmapIds sees args = " + JSON.stringify(arguments));
      var kmapList = [];


      console.log("KMAPIDS: " + JSON.stringify(kmapids));

      for (var i = 0; i < kmapids.length; i++) {


        var kid = kmapids[i];
        var name = localStorage.getItem(kid)
        if (name === null) {
          name = kid;
        }
        var entry = name + "|" + kid;
        kmapList.push(entry);
      }

      console.log("lookupKmapIds returning " + JSON.stringify(kmapList));
      return kmapList;
  };

var filterDone = exports.filterDone =
  function (write_client, docs, callback) {
    var uidlist = _.map(docs, function (x) {
      return x.uid;
    })
    var uidq = _.join(uidlist, " ");
    var q = write_client.createQuery().q("uid:(" + uidq + ")").fl("uid");
    // write_client.get("select", q, function (err, x) {
    //
    //
    //
    //
    //
    //
    // });
  };


// TODO:  use a template to write the asset entries
// var service = config.service;
// var type = kmapEntry.tree;
// var id = kmapEntry.uid.split("\-")[1];
// var uid = service + "_" + kmapEntry.uid;
// var caption = (kmapEntry.caption_eng) ? kmapEntry.caption_eng
// TODO: use a map to list entries to copy?

var createAssetEntry = exports.createAssetEntry =
  function (kmapEntry, config, callback) {

    // console.dir({ kmapEntry: kmapEntry });

    function cleanEntries(entries) {
      var cleaned = [];
      for (var i = 0; i < entries.length; i++) {
        var stripped = striptags(entries[i]);
        stripped = stripped.replace('&nbsp;', '');
        if (stripped) {
          // console.log("original:" + entries[i]);
          // console.log("cleaned: " + stripped);
          cleaned.push(stripped);
        }
      }
      return cleaned;
    }

// pass through several functions to populate the asset entry
    async.waterfall(
      [
        function (next) {

          var service = config.service;
          var type = kmapEntry.tree;
          var id = kmapEntry.uid.split("\-")[1];

          var prefix = (service !== "prod")?config.service + "_":""
          prefix = "";
          var uid = prefix + kmapEntry.uid;

          var kmapProps = Object.entries(kmapEntry);
          var header = kmapEntry.header;
          var feature_types = kmapEntry.feature_types;


          console.dir(kmapEntry);

          // throw new Error("stop");

          function processNames(rentries) {
            // filter by name_* fields
            var name_entries1 = _.filter(rentries, function (x) {
              return x[0].startsWith("name_")
            });

            var name_entries = _.map(name_entries1, function (x) {
              return x[1];
            })

            // console.error("name_entries: " + name_entries.length);
            // console.error("name_entries: " + JSON.stringify(name_entries));

            // collect up and flatten the names
            const flat = _.flatten(name_entries);
            const uniq = _.uniq(flat);
            var names = _.sortBy(uniq);
            // console.error("NAMES: " + JSON.stringify(names));
            return names;
          }

          var names = processNames(kmapProps);
          var text = _.flatten([names, kmapEntry.text]);

          // relateds: use the field kmapid_strict if it exists otherwise use kmapid
          var relateds = [];
          if (kmapEntry.kmapid_strict) {
            relateds = kmapEntry.kmapid_strict;
          } else if (kmapEntry.kmapid) {
            relateds = kmapEntry.kmapid;
          }

          // add other relateds
          if (kmapEntry.associated_subject_ids) {
              relateds = _.uniq(_.concat(relateds,kmapEntry.associated_subject_ids)).map( function(x) { return "subjects-" + x});
          }

          var kmapList = lookupKmapIds(relateds);

          // DERIVE kmapid_is from ancestors_uids_generic
          var generateId = function(x) {
            var parts=x.split("-");
            var type = parts[0];
            var id = Number(parts[1]);
            id *= 100;

            if (type === "places") {
              id += 1;
            } else if (type === "subjects") {
              id += 2;
            } else if (type === "terms") {
              id +=3;
            } else {
              console.error("UNKNOWN kmap type: " + type + " from kmapid " + x);
            }
            return id;

          };

          var uid_i = generateId(type + "-" + id);
          var kmapid = [];
          var kxlist_subjects = [];
          var kxlist_places = [];
          var kxlist_terms = [];
          if (kmapEntry.ancestor_uids_generic) {
            kmapid = kmapEntry.ancestor_uids_generic;
          } else if (kmapEntry['ancestor_uids_tib.alpha']) {
            kmapid = kmapEntry['ancestor_uids_tib.alpha'];
          }

          //  The current "template for writing asset enries for kmaps".
          var stricts = [ kmapEntry.uid ];
          if (relateds) {
            stricts = _.concat(stricts,relateds);
          }

          var ancestorsTxt = kmapEntry.ancestors;
          var ancestorIdsIs = kmapEntry.ancestor_ids_generic;

          //
          if (kmapEntry['ancestors_tib.alpha']) {
            ancestorsTxt = kmapEntry['ancestors_tib.alpha'];
          }


          //
          if (kmapEntry['ancestor_ids_tib.alpha']) {
            ancestorIdsIs = kmapEntry['ancestor_ids_tib.alpha'];
          }

          if (kmapEntry.ancestors) {

            console.log("ANCESTORS_TXT = " + JSON.stringify(kmapEntry.ancestors));
            console.log("ANCESTOR_IDS = " + JSON.stringify(kmapEntry.ancestor_ids_generic));

            if (kmapEntry.ancestors.length !== kmapEntry.ancestor_ids_generic.length) {
              throw new Error("Counts don't match!")
            }

            var domain = kmapEntry.tree;
            var uidlist = _.map(ancestorIdsIs, function (x) {
              return domain + "-" + x
            });

            console.log("UIDLIST = " + uidlist);
            recordKmap(kmapEntry.ancestors, uidlist, domain);

            kmapid = _.uniq(_.sortBy(_.concat(stricts, relateds, kmapid, uidlist), function (x) {
              return x;
            }));

            var kmapid_is = _.map(kmapid, generateId);

            console.log("USING kmapid = " + JSON.stringify(kmapid));

            var looky = lookupKmapIds(kmapid);
            console.log("LOOKY = " + looky);

            _.each(looky, function (x) {
              if (DEBUG) console.log("EACHING: " + x);
              if (x.indexOf("|places") !== -1) {
                kxlist_places.push(x);
              } else if (x.indexOf("|subjects") !== -1) {
                kxlist_subjects.push(x);
              } else if (x.indexOf("|terms") != -1) {
                kxlist_terms.push(x);
              }
            });

            if (DEBUG) {
              console.log(" places = " + kxlist_places);
              console.log(" subjects = " + kxlist_subjects);
              console.log(" terms = " + kxlist_terms);
            }
          }

          var doc = {
            "schema_version_i": SCHEMA_VERSION,
            "asset_type": type,
            "service": service,
            "id": id,
            "uid": uid,
            "uid_i": uid_i,
            "url_html": config.baseurl + "/" + type + "/" + id + "/overview/nojs",
            "kmapid":kmapid,
            "kmapid_is": kmapid_is,
            "kmapid_strict": stricts,
            "text": text,
            "names_txt": names,
            "name_autocomplete": kmapEntry.name_autocomplete,
            "name_tibt": kmapEntry.name_tibt,
            "name_latin": kmapEntry.name_latin,
            "title": header,
            "feature_types_ss": feature_types,
            "associated_subjects_ss" : kmapEntry.associated_subjects,
            "ancestors_txt": ancestorsTxt,
            "ancestor_ids_is": ancestorIdsIs,
            // "ancestor_uids_generic": kmapEntry.ancestor_uids_generic,
            "kmapid_subjects_idfacet": kxlist_subjects,
            "kmapid_places_idfacet": kxlist_places,
            "kmapid_terms_idfacet": kxlist_terms,
            "related_uid_ss": relateds,
            "position_i": kmapEntry.position_i
          };

          // map the associated data if available
          if (kmapEntry.associated_subject_185_ss) doc["data_language_context_ss"] = kmapEntry.associated_subject_185_ss;
          if (kmapEntry.associates_subject_286_ss) doc["data_tibetan_grammatical_function_ss"] = kmapEntry.associates_subject_286_ss;
          if (kmapEntry.associated_subject_190_ss) doc["data_register_ss"] = kmapEntry.associated_subject_190_ss;
          if (kmapEntry.associated_subject_187_ss) doc["data_literary_period_ss"] = kmapEntry.associated_subject_187_ss;
          if (kmapEntry.associated_subject_5812_ss) doc["data_grammars_ss"] = kmapEntry.associated_subject_5812_ss;
          if (kmapEntry.associated_subject_272_ss) doc["data_tibet_and_himalayas_ss"] = kmapEntry.associated_subject_272_ss;
          if (kmapEntry.associated_subject_9310_ss) doc["data_phoneme_ss"] =  kmapEntry.associated_subject_9310_ss;

          // clean captions
          var caption = null;
          if (kmapEntry.caption_eng) {
            caption = cleanEntries(kmapEntry.caption_eng);
          }
          if (caption && caption.length) {
            // console.error("Setting captions_eng: " + JSON.stringify(caption, undefined));
            doc.caption = caption;
          }

          // clean texts
          var newtext = null;
          if (kmapEntry.text) {
            newtext = cleanEntries(kmapEntry.text);
          }
          if (newtext && newtext.length) {
            doc.text = newtext;
          }

          next(null, doc);
        },
        function (doc, next) {

          // Currently a NULL function

          // xml2js.parseString(kmapEntry.caption_eng, {
          //   valueProcessors: [
          //     function (value, name) {
          //       console.log("value =" + value);
          //       console.log("name = " + name);
          //       return value;
          //     }]
          // },
          //   function (err, parsed) {
          //   if (parsed) {
          //     console.log("PARSED: " + parsed + " ERR: " + err);
          //     doc.caption = parsed;
          //   }
          next(null, doc);
          // })
        }
      ],
      function (err, doc) {
        if(doc.caption) {
          if(DEBUG) console.log("GOT: " + doc.uid + " " + doc.title);
          // console.dir(doc);
        }
        async.nextTick(function () {
          callback(null, doc)
        });
      }
    )

    // var caption = (kmapEntry.caption_eng) ? kmapEntry.caption_eng : "Caption for " + kmapEntry.uid;
  }

var writeAssetDoc = exports.writeAssetDoc =
  function (config, new_doc, callback) {
    var write_client = config.write_client;
    // console.error("WRITING ASSET CLIENT: " + write_client);
    // console.error("WRITING ASSET CONFIG: " + JSON.stringify(config, undefined, 2));
    var overwrite = function (newdoc, olddoc) {
      if (FORCE_OVERWRITE) {
        return true;
      }
      // console.log ("newdoc schema version: " + newdoc.schema_version_i);
      // console.log ("olddoc schema version: " + olddoc.schema_version_i);
      return (newdoc.schema_version_i > olddoc.schema_version_i);
    };

    var query = write_client.createQuery().df("uid").q(new_doc.uid).rows(1).start(0);

    //  TODO: refactor to use waterfall and retry BOTH queries...

    var add_retry = async.retryable(
      {
        times: 5,
        interval: function (attempts) {
          console.error("RETRY: " + attempts);
          var pause =  50 * Math.pow(2, attempts);
          console.log ("retry waiting " + pause);
          return pause;
        },
        errorFilter: function (err) {
          console.error("RETRY ON ERROR: " + err.code);
          console.error(err);
          if (err.code !== "ENOTFOUND") {
            console.error("Unknown error: " + JSON.stringify(err, undefined, 2));
          }
          return true;
        }
      },
      function (doc, cb) {
        write_client.add(doc, cb);
      }
    )

    var check_retry = async.retryable(
      {
        times: 5,
        interval: function (attempts) {
          console.error("check RETRY: " + attempts);
          var pause =  50 * Math.pow(2, attempts);
          console.log ("retry waiting " + pause);
          return pause;
        },
        errorFilter: function (err) {
          console.error("check RETRY ON ERROR: " + err.code);
          if (err.code !== "ENOTFOUND") {
            console.error("Unknown error: " + JSON.stringify(err, undefined, 2));
          }
          return true;
        }
      },
      function (query, cb) {
        write_client.get("select", query, function(doc,err) {
          // if (err) {
          //   console.error(err);
          // }
          // if (doc) {
          //   console.dir(doc);
          // }
          cb(doc,err);
        });
      }
    )


    check_retry(query, function (err, existing) {
      if (err) {
        console.error("error while trying to check entry: " + new_doc.uid + ": \n" + err);
        // console.dir(this);
      } else {
        // console.log("WRITE_CLIENT: get existing....");
        // console.dir(obj);
      }

      // console.log( "OVERWRITE "  + new_doc.uid + ": " + overwrite(new_doc, old_doc));

      if (!existing.response.numFound || overwrite(new_doc, existing.response.docs[0])) {
        var core = write_client.options.core;
        console.error("WRITING ASSET DOC: [" + core + "] " + new_doc.uid + ": " + JSON.stringify(new_doc.title));

        add_retry(new_doc, function (err, obj) {
          if (err) {
            console.error("ERROR: " + JSON.stringify(err) + " " + JSON.stringify(obj));
            console.error("Problem writing: " + JSON.stringify(new_doc, undefined, 2));
            callback(err, obj);
          } else {
            if (obj.responseHeader.status !== 0) {
              console.error('Solr response with non-zero status:', obj);
            }
            callback(null, obj);
          }
        });
      } else {
        // console.log("skipping: " + new_doc.uid);
        callback(null, {});
      }
    });
  }

var processQueue = exports.processQueue =
  function (config, queue, processQueueCallback) {

    var read_client = config.read_client;
    var write_client = config.write_client;

    // console.error("read_client = " + read_client);
    // console.error("write_client = " + JSON.stringify(write_client));

    // JOB DATA:   { "query": "uid:places-12345" }
    const concurrency = config.concurrency || DEFAULT_CONCURRENCY;
    queue.process('process', concurrency, function (job, jobdone) {
      console.error("PROCESSING job: " + JSON.stringify(job));

      function getCounter() {
        var count = 0;
        var full_count = 0;
        var counter = {
          done: function () {
            count++;
          },
          count: function () {
            return count;
          },
          number: function () {
            return full_count;
          },
          setCount: function (count) {
            full_count = count;
          }
        }
        return counter;
      }

      var counter = getCounter();

      async.waterfall(
        [
          function (next) {
            getKmapEntries(read_client, job.data.query, job.data.rows, job.data.start, next);
          },
          function (entries, next) {
            counter.setCount(entries.length);
            async.concat(entries,
              function (kmapEntry, next) {
                createAssetEntry(kmapEntry, config, next);
              },
              next
            )
          },
          function (output, next) {
            // console.dir(output);
            // console.dir(next);
            // console.error("HERE");
            // console.dir(writeAssetDoc);
            async.eachOfLimit(output, 1, function (doc, i, each_cb) {
                counter.done(i);
                var n = output.length;
                var p = Math.ceil(n / 4);
                if (i === 0 || i === p || i === p * 2 || i === p * 3 || i === n) {
                  console.error(" count: " + counter.count() + " / " + counter.number());
                }
                job.progress(counter.count(), counter.number());
                writeAssetDoc(config, doc, each_cb);
              }, next
            );
          }
        ],
        function (err, result) {
          if (err) {
            throw err;
            jobdone();
            processQueueCallback("fail", null);
          } else {
            // console.dir("IN THE RESULT CALLBACK")
            // console.dir(result);
            jobdone();
            processQueueCallback(null, "success");
          }
        }
      );
    });
  };

var XgenerateJobs = exports.XgenerateJobs =
  function (config, query, doneCallback, jobCallback) {

    var client = config.read_client;

    var start = config.start || 0;
    var rows = config.rows || DEFAULT_ROWS;
    var num = rows + 1;

    async.doUntil(
      function (iterCallback) {
        // console.error("JOB: start = " + start + " rows = " + rows + " num = " + num);
        var query = client.createQuery().q("tree:(subjects)").start(start).rows(rows);
        client.search(query, function (err, results) {
          if (err) {
            jobCallback(err);
            iterCallback();
          } else {
            // console.log("start:  " + results.response.start);
            // console.log("length: " + results.response.docs.length);
            num = results.response.numFound;
            jobCallback(null, results);
            iterCallback();
          }
        });
        start += rows;
      },
      function () {
        // console.error("CHECK: start = " + start + " rows = " + rows + " num = " + num);
        const done = (start > num);
        // console.error("done = " + done);
        return done
      },
      function (err, results) {
        console.dir(err);
        console.log(results);
        done();
      })
  };

var generateJobspecs = exports.generateJobspecs =
  function (config, query, callback) {
    var client = config.read_client;
    var rows = config.rows || DEFAULT_ROWS;
    var q = client.createQuery().q(query);
    client.search(q, function (err, results) {
      if (err) {
        // console.error(client);
        // console.error(q);
        // console.error("WHAT THE HELL! " + JSON.stringify(err, undefined, 2));
        callback(err);
      } else {
        var num = results.response.numFound;
        var chunks = Math.ceil(num / rows);
        var specs = [];

        for (var i = 0; i < chunks; i++) {
          var start = i * rows;
          var job = {
            query: query,
            title: query + "(" + rows + ") start = " + start,
            start: start,
            rows: rows
          };
          specs.push(job);
        }

        // console.log("generateJobspecs: calling back with " + JSON.stringify(specs));
        callback(null, specs);
      }
    })
  };
