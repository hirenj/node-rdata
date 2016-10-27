"use strict";

const async = require('async');
const Stream = require('stream');
const temp = require('temp');
const fs = require('fs');
const LengthRewriter = require('./transforms').LengthRewriter;
const KeyExtractor = require('./transforms').KeyExtractor;
const ByteWriter = require('./transforms').ByteWriter;


temp.track();

const NILSXP     =  0;    /* nil = NULL */
const SYMSXP     =  1;    /* symbols */
const LISTSXP    =  2;    /* lists of dotted pairs */
const CLOSXP     =  3;    /* closures */
const ENVSXP     =  4;    /* environments */
const PROMSXP    =  5;    /* promises: [un]evaluated closure arguments */
const LANGSXP    =  6;    /* language constructs (special lists) */
const SPECIALSXP =  7;    /* special forms */
const BUILTINSXP =  8;    /* builtin non-special forms */
const CHARSXP    =  9;    /* "scalar" string type (internal only)*/
const LGLSXP     = 10;    /* logical vectors */
const INTSXP     = 13;    /* integer vectors */
const REALSXP    = 14;    /* real variables */
const CPLXSXP    = 15;    /* complex variables */
const STRSXP     = 16;    /* string vectors */
const DOTSXP     = 17;    /* dot-dot-dot object */
const ANYSXP     = 18;    /* make "any" args work.
           Used in specifying types for symbol
           registration to mean anything is okay  */
const VECSXP     =  19;   /* generic vectors */
const EXPRSXP    =  20;   /* expressions vectors */
const BCODESXP   =  21;   /* byte code */
const EXTPTRSXP  =  22;   /* external pointer */
const RAWSXP     =  24;   /* raw bytes */
const S4SXP      =  25;   /* S4, non-vector */
const FUNSXP     =  99;   /* Closure or Builtin or Special */
const REFSXP     = 255;

const LATIN1_MASK  = (1<<2);
const UTF8_MASK    = (1<<3);
const ASCII_MASK   = (1<<6);

const IS_OBJECT_BIT_MASK = (1 << 8);
const HAS_ATTR_BIT_MASK  = (1 << 9);
const HAS_TAG_BIT_MASK   = (1 << 10);

let packed_version = function(v, p, s) {
  return s + (p * 256) + (v * 65536);
};

let encode_int = function(value) {
  let buf = new Buffer(4);
  buf.writeInt32BE(value);
  return buf;
};

const encode_real = function(value) {
  let buf = new Buffer(8);
  buf.writeDoubleBE(value);
  return buf;
};

let encode_flags = function(base_type,options) {
  if ( ! options ) {
    options = {};
  }
  let flags = base_type;
  if (options.is_object)
    flags |= IS_OBJECT_BIT_MASK;

  if (options.has_attributes)
    flags |= HAS_ATTR_BIT_MASK;

  if (options.has_tag)
    flags |= HAS_TAG_BIT_MASK;

  return flags;
};

const write_vector = function(vector,method) {
  let self = this;
  if (vector instanceof Stream && vector._readableState.objectMode) {
    vector.pipe( new ByteWriter(method.bind(self)) ).pipe(this.stream,{end: false});
    return new Promise(function(resolve) {
      vector.on('end',resolve);
      vector.on('finish',resolve);
    });
  } else {
    return new Promise(function(resolve) {
      async.eachOfSeries(vector,function(el,idx,callback) {
        if (el !== null) {
          self.write(method(el));
        }
        process.nextTick(callback);
      },function(err) {
        resolve();
      });
    }).catch(function(err) {
      console.log(err);
    });
  }

};

const stringScalar = function(string) {
  let type = encode_int(CHARSXP | (ASCII_MASK << 12) );
  let bytes = new Buffer(string);
  let length = encode_int(bytes.length);
  return Buffer.concat([type,length,bytes]);
};

const realScalar = function(real) {
  return encode_real(real);
};

const intScalar = function(int) {
  return encode_int(int);
};

const logicalScalar = function(bool) {
  return encode_int(bool ? 1 : 0);
};

const stringVector = function(vector) {
  let self = this;
  this.write(encode_int(encode_flags(STRSXP)));
  this.write(encode_int(vector.length || vector.total));
  return write_vector.bind(self)(vector,stringScalar);
};

const realVector = function(vector) {
  let self = this;
  this.write(encode_int(encode_flags(REALSXP)));
  this.write(encode_int(vector.length || vector.total));
  return write_vector.bind(self)(vector,realScalar);
};

const intVector = function(vector) {
  let self = this;
  this.write(encode_int(encode_flags(INTSXP)));
  this.write(encode_int(vector.length || vector.total));
  return write_vector.bind(self)(vector,intScalar);
};

const logicalVector = function(vector) {
  let self = this;
  this.write(encode_int(encode_flags(LGLSXP)));
  this.write(encode_int(vector.length || vector.total));
  return write_vector.bind(self)(vector,logicalScalar);
};

const symbol = function(string) {
  this.write(encode_int(encode_flags(SYMSXP)));
  this.write(stringScalar(string));
  return Promise.resolve();
};

const listPairs = function(pairs,keys,types) {
  let self = this;
  return new Promise(function(resolve) {
    async.eachOfSeries(keys,function(key,idx,callback) {
      self.write(encode_int(encode_flags(LISTSXP,{ has_tag: true })))
      self.symbol(key);
      self.writeValue(pairs[key],types[idx]).then(callback);
    }, function(err) {
      // FIXME - magic value here
      self.write(encode_int(0xfe));
      resolve();
    });
  });
};

const consume_frame_stream = function(objects,keys,types,length) {
  let self = this;
  let outputs = keys.map(function(key,idx) {
    console.log(key,idx);
    let stream = new Stream.PassThrough({objectMode: true});
    let output = new ObjectWriter(temp.createWriteStream());
    output.path = output.stream.path;
    let promise = output.writeValue(stream,types[idx],length);
    promise.catch((err) => console.log(err));
    promise.then(() => output.stream.end() );
    output.promise = new Promise(function(resolve,reject) {
      output.stream.on('finish',() => { console.log("Wrote file for ",key); resolve(); });
    });
    output.instream = stream;
    return output;
  });
  outputs.forEach( (output,idx) => { objects.pipe( new KeyExtractor(keys[idx]) ).pipe(output.instream)  });
  return Promise.all( outputs.map( (output) => output.promise )).then(function() {
    console.log("Wrote all temp files");
    let filenames = outputs.map( (output) => output.path );
    let streams = filenames.map( (file) => fs.createReadStream(file));
    let frame = {}
    keys.forEach(function(key,idx) {
      frame[key] = streams[idx];
    });
    return self.dataFrame(frame,keys,types,length);
  });
};

const dataFrame = function(object,keys,types,length) {
  var self = this;
  if (object instanceof Stream && object._readableState.objectMode) {
    return (consume_frame_stream.bind(self))(object,keys,types,length);
  }
  this.write(encode_int(encode_flags(VECSXP,{ is_object: true, has_attributes: true })));
  this.write(encode_int(keys.length));
  if ( length === null || typeof length === 'undefined' ) {
    length = object[keys[0]].length;
  }
  console.log("Writing data frame of length ",length);
  return new Promise(function(resolve) {
    async.eachOfSeries(keys,function(column,idx,callback) {
      self.writeValue(object[column],types[idx],length).then(callback);
    }, function(err) {
      // FIXME - Magic number for min int value
      let attributes = { "names": keys, "class" : ["data.frame"], "row.names" : [-1*(0x80000000),-1*length] };
      self.listPairs(attributes,[ "names","row.names","class" ] ,["string","int","string"]).then(resolve);
    });
  });
};

const writeHeader = function() {
  this.write(new Buffer('RDX2\nX\n'));
  this.write(encode_int(2));
  this.write(encode_int(packed_version(3,0,0)));
  this.write(encode_int(packed_version(2,3,0)));
  return Promise.resolve();
};

const writeValue = function(value,type,length) {
  var self = this;
  // When we pass a buffer stream to the writeValue, we assume
  // that we are taking a stream of encoded bytes
  if (value instanceof Stream.Readable && ! value._readableState.objectMode) {
    if (typeof length === 'undefined') {
      value.pipe(self.stream,{end: false});
    } else {
      value.pipe(new LengthRewriter(length)).pipe(self.stream,{end: false});
    }
    return new Promise(function(resolve,reject) {
      value.on('end',function() {
        console.log("Ended input stream for ",type);
        resolve();
      });
      value.on('error',reject);
    });
  }
  if (value instanceof Stream && value._readableState.objectMode) {
    // This is an object mode stream
  }
  let value_written = null;

  if (type === "string") {
    value_written = this.stringVector(value);
  }
  if (type === "int") {
    value_written = this.intVector(value);
  }
  if (type === "real") {
    value_written = this.realVector(value);
  }
  if (type === "logical") {
    value_written = this.logicalVector(value);
  }
  if (type.type === "dataframe") {
    value_written = this.dataFrame(value,type.keys,type.types,type.length);
  }
  return value_written;
};

function ObjectWriter(stream) {
  this.stream = stream;
};

ObjectWriter.prototype.write = function(buffer) {
  this.stream.write(buffer);
};

ObjectWriter.prototype.stringScalar = stringScalar;
ObjectWriter.prototype.realScalar = realScalar;
ObjectWriter.prototype.intScalar = intScalar;
ObjectWriter.prototype.logicalScalar = logicalScalar;
ObjectWriter.prototype.stringVector = stringVector;
ObjectWriter.prototype.realVector   = realVector;
ObjectWriter.prototype.intVector    = intVector;
ObjectWriter.prototype.logicalVector= logicalVector;
ObjectWriter.prototype.symbol       = symbol;
ObjectWriter.prototype.listPairs    = listPairs;
ObjectWriter.prototype.dataFrame    = dataFrame;
ObjectWriter.prototype.writeValue   = writeValue;
ObjectWriter.prototype.writeHeader  = writeHeader;

module.exports = ObjectWriter;
