"use strict";
const ObjectWriter = require('..');
const chai = require('chai');
const expect = chai.expect;
const assert = chai.assert;
const tempfile = require('temp');
const zlib = require('zlib');
const fs = require('fs');

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
  this.total = Math.floor(this.max / this.increment);
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


describe('Pregenerating columns', function() {
  it('Writes a stream of values',function(done){
    this.timeout(15000);
    let obj_writer = new ObjectWriter(tempfile.createWriteStream());
    let obj_writer2 = new ObjectWriter(tempfile.createWriteStream());

    const vec_length = 5e04;
    let promise_1  = obj_writer.realVector(new IntStream(vec_length))
                               .then( () => console.log("Wrote stream 1"))
                               .then( () => obj_writer.finish() )
                               .catch(done);
    let promise_2  = obj_writer2.realVector(new IntStream(2*vec_length,2))
                                .then( () => console.log("Wrote stream 2"))
                                .then( () => obj_writer2.finish() )
                                .catch(done);

    Promise.all([promise_1,promise_2]).then(function() {
      console.log("Putting streams together");
      let gz = zlib.createGzip();

      let writer = gz;

      let file_stream = gz.pipe(tempfile.createWriteStream());

      let written_vector = fs.createReadStream(obj_writer.stream.path);
      let written_vector2 = fs.createReadStream(obj_writer2.stream.path);

      obj_writer = new ObjectWriter(writer);

      obj_writer.writeHeader();
      return obj_writer.listPairs( {"frame" : {"cola" : written_vector, "colb" : written_vector2 }},
                        ["frame"],
                        [{ "type": "dataframe", "keys": ["cola", "colb"], "types" : ["real","real"] }]
                        ).then( () => obj_writer.finish() )
                        .then( () => file_stream.path );
    })
    .then( (file) => {
      return row_count(file,'frame')
             .then( (count) => { expect(count).equals(vec_length); })
             .then( () => file );
    })
    .then( (file) => {
      return object_class(file,'frame$cola')
             .then( (clazz) => { expect(clazz).equals("numeric"); })
             .then( () => file );
    })
    .then( (file) => {
      return object_class(file,'frame$colb')
             .then( (clazz) => { expect(clazz).equals("numeric"); })
             .then( () => file );
    })
    .then( () => done() )
    .catch(done);
  });
});