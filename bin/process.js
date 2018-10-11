const sq = require('../logic/solrKue.js');

var queue;

sq.createQueue( function(err, que) {
  this.queue = que;



})
;
