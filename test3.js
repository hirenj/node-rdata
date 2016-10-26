"use strict";

const fs = require('fs');
const zlib = require('zlib');
const Stream = require('stream');

const Writer = require('./lib/object_writer');

const objects = [ { "x" : 2, "y" : "ab", "z": false},
                  { "x" : 4, "y" : "ac", "z": false},
                  { "x" : 8, "y" : "ad", "z": true},
                  { "x" : 16, "y" : "ae", "z": true},
                  { "x" : 32, "y" : "af", "z": true}
                ];

let gz = zlib.createGzip();

let writer = gz;

gz.pipe(fs.createWriteStream('test.Rdata'));

const Readable = require('stream').Readable;
const util = require('util');

function ObjectStream(max,increment,options) {
  if (! (this instanceof ObjectStream)) return new ObjectStream(options);
  if (! options) options = {};
  options.objectMode = true;
  Readable.call(this, options);
}

util.inherits(ObjectStream, Readable);

ObjectStream.prototype._read = function read() {
  var self = this;
  console.log(objects);
  if (objects.length > 0) {
    self.push(objects.shift());
  } else {
    self.push(null);
    self.emit('close');
  }
};

let obj_writer = new Writer(writer);
obj_writer.writeHeader();
obj_writer.listPairs( {"frame" : new ObjectStream()},
                      ["frame"],
                      [{ "type": "dataframe", "length" : 5, "keys": ["x", "y","z"], "types" : ["int", "string", "logical"] }]
                      ).then( () => { console.log("Wrote frame data"); obj_writer.stream.end(); });



