"use strict";
const ObjectWriter = require('..');
const chai = require('chai');
const expect = chai.expect;
const assert = chai.assert;
const tempfile = require('temp');

const fs = require('fs');
const zlib = require('zlib');
const Stream = require('stream');

const objects = [ { "x" : 2, "y" : "ab", "z": false},
                  { "x" : 4, "y" : "ac", "z": false},
                  { "x" : 8, "y" : "ad", "z": true},
                  { "x" : 16, "y" : "ae", "z": true},
                  { "x" : 32, "y" : "af", "z": true}
                ];


tempfile.track();

const run_rscript = function(file,command) {
  var exec = require('child_process').exec;
  var cmd = 'R --silent -e "load(\''+file+'\'); '+command.replace('$','\\$')+';" | grep -v ">"';
  return new Promise(function(resolve,reject) {
    exec(cmd, function(error, stdout, stderr) {
      if (error) {
        reject(error);
      } else {
        resolve(stdout.split(' ').slice(1).join(' '));
      }
    });
  });
}

const row_count = function(file,variable) {
  return run_rscript(file,'nrow('+variable+')').then( (val) => parseInt(val) );
}

const object_class = function(file,variable) {
  return run_rscript(file,'class('+variable+')').then( (val) => val.replace(/[\n"]/g,'') );
}

const Readable = require('stream').Readable;
const util = require('util');

function ObjectStream(max,options) {
  if (! (this instanceof ObjectStream)) return new ObjectStream(max,options);
  if (! options) options = {};
  options.objectMode = true;
  this.counter = 0;
  this.total = max;
  Readable.call(this, options);
}

util.inherits(ObjectStream, Readable);

ObjectStream.prototype._read = function read() {
  var self = this;
  if (typeof this.counter == 'undefined' || this.counter < self.total) {
    self.push(objects[0]);
    this.counter = this.counter || 0;
    this.counter += 1;
    return;
  }
  if (objects.length > 0) {
    self.push(objects.shift());
  } else {
    self.push(null);
    self.emit('close');
  }
};

describe('Writing a stream', function() {
  it('Writes data out from an object stream',function(done){
    this.timeout(20000);
    let vec_length = 5e04;
    let gz = zlib.createGzip();

    let writer = gz;

    let file_stream = gz.pipe(tempfile.createWriteStream());

    let object_writer = new ObjectWriter(writer);
    let path = object_writer.stream.path;
    object_writer.writeHeader();
    object_writer.listPairs( {"frame" : new ObjectStream(vec_length)},
                          ["frame"],
                          [{ "type": "dataframe", "keys": ["x", "y","z"], "types" : ["real", "string", "logical"] }]
                          ).then( () => object_writer.finish() )
                          .then( () => file_stream.path )
    .then( (file) => {
      return row_count(file,'frame')
             .then( (count) => { expect(count).equals(vec_length+5); })
             .then( () => file );
    })
    .then( (file) => {
      return object_class(file,'frame$x')
             .then( (clazz) => { expect(clazz).equals("numeric"); })
             .then( () => file );
    })
    .then( (file) => {
      return object_class(file,'frame$y')
             .then( (clazz) => { expect(clazz).equals("character"); })
             .then( () => file );
    })
    .then( (file) => {
      return object_class(file,'frame$z')
             .then( (clazz) => { expect(clazz).equals("logical"); })
             .then( () => file );
    })
    .then( () => done() )
    .catch( done );
  });
  it('Writes data out from an object stream including nulls',function(done){
    objects.push({ "x" : null, "y" : null, "z": null});
    this.timeout(20000);
    let vec_length = 5e01;
    let gz = zlib.createGzip();

    let writer = gz;

    let file_stream = gz.pipe(tempfile.createWriteStream());

    let object_writer = new ObjectWriter(writer);
    let path = object_writer.stream.path;
    object_writer.writeHeader();
    object_writer.listPairs( {"frame" : new ObjectStream(vec_length)},
                          ["frame"],
                          [{ "type": "dataframe", "keys": ["x", "y","z"], "types" : ["real", "string", "logical"] }]
                          ).then( () => object_writer.finish() )
                          .then( () => file_stream.path )
    .then( (file) => {
      return row_count(file,'frame')
             .then( (count) => { expect(count).equals(vec_length+1); })
             .then( () => file );
    })
    .then( (file) => {
      return object_class(file,'frame$x')
             .then( (clazz) => { expect(clazz).equals("numeric"); })
             .then( () => file );
    })
    .then( (file) => {
      return object_class(file,'frame$y')
             .then( (clazz) => { expect(clazz).equals("character"); })
             .then( () => file );
    })
    .then( (file) => {
      return object_class(file,'frame$z')
             .then( (clazz) => { expect(clazz).equals("logical"); })
             .then( () => file );
    })
    .then( () => done() )
    .catch( done );
  });
});
