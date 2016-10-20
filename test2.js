"use strict";

const fs = require('fs');
const zlib = require('zlib');
const Stream = require('stream');

const Writer = require('./lib/object_writer');

const dataframe = { "x" : [2,4,8,16,32], "y" : ["ab","ac","ad","ae","af"], "z" : [false,false,true,true,true]};

let obj_writer = null;

let intwriter1 = fs.createWriteStream('ints1.Rda');
let intwriter2 = fs.createWriteStream('ints2.Rda');


const Readable = require('stream').Readable;
const util = require('util');

function IntStream(max,options) {
  if (! (this instanceof IntStream)) return new IntStream(max,options);
  if (! options) options = {};
  options.objectMode = true;
  this.count = 0;
  this.max = max;
  Readable.call(this, options);
}

util.inherits(IntStream, Readable);

IntStream.prototype._read = function read() {
  var self = this;
  if (self.count <= self.max) {
    self.push(self.count);
    self.count += 1;
  } else {
    self.push(null);
    self.emit('close');
  }
};

obj_writer = new Writer(intwriter1);

let intstream = new IntStream(9);

intstream.on('end', () => console.log("Ended"));

obj_writer.writeValue(intstream,"int").then(function() {
  console.log("Done writing int vector");
  obj_writer.stream.end();
});

console.log("Passed block");

// intstream.on('end',() => obj_writer.stream.end() );