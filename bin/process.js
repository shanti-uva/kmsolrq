const sq = require('../logic/solrWriteKmapAssetKue.js');

var queue;

sq.createQueue( function(err, que) {
  this.queue = que;



})
;
