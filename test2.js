"use strict";

const fs = require('fs');
const zlib = require('zlib');
const Stream = require('stream');

const Writer = require('./lib/object_writer');

const dataframe = { "x" : [2,4,8,16,32], "y" : ["ab","ac","ad","ae","af"], "z" : [false,false,true,true,true]};

let obj_writer = null, obj_writer2 = null;

let intwriter1 = fs.createWriteStream('ints1.Rda');
let intwriter2 = fs.createWriteStream('ints2.Rda');


const Readable = require('stream').Readable;
const util = require('util');

const stream_done_promise = function(stream) {
  return new Promise(function(resolve) {
    stream.on('close',resolve);
  });
};

function IntStream(max,increment,options) {
  if (! (this instanceof IntStream)) return new IntStream(max,options);
  if (! options) options = {};
  options.objectMode = true;
  this.count = 1;
  this.max = max || 10;
  this.increment = increment || 1;
  Readable.call(this, options);
}

util.inherits(IntStream, Readable);

IntStream.prototype._read = function read() {
  var self = this;
  if (self.count <= self.max) {
    self.push(self.count);
    self.count += self.increment;
  } else {
    self.push(null);
    self.emit('close');
  }
};

obj_writer = new Writer(intwriter1);
obj_writer2 = new Writer(intwriter2);

const vec_length = 1e05;

let intstream = new IntStream(vec_length);
let intstream2 = new IntStream(2*vec_length,2);

let promise_1  = obj_writer.writeValue(intstream,"real").then(function() {
  console.log("Done writing int vector");
  obj_writer.stream.end();
}).then( () => stream_done_promise(intwriter1));

let promise_2 = obj_writer2.writeValue(intstream2,"real").then(function() {
  console.log("Done writing second int vector");
  obj_writer2.stream.end();
}).then( () => stream_done_promise(intwriter2));

intwriter1.on('close', () => console.log("Ended first stream"));
intwriter2.on('close', () => console.log("Ended second stream"));

Promise.all([promise_1,promise_2]).then(function() {
  console.log("Putting streams together");
  var gz = zlib.createGzip();

  let writer = gz;

  gz.pipe(fs.createWriteStream('test.Rdata'));

  let written_vector = fs.createReadStream('ints1.Rda');
  let written_vector2 = fs.createReadStream('ints2.Rda');

  obj_writer = new Writer(writer);

  obj_writer.writeHeader();
  obj_writer.listPairs( {"frame" : {"cola" : written_vector, "colb" : written_vector2 }},
                        ["frame"],
                        [{ "type": "dataframe", "length" : vec_length, "keys": ["cola", "colb"], "types" : ["int","real"] }]
                        ).then( () => obj_writer.stream.end() );
});
