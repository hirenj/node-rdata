"use strict";
/*jshint esversion: 6, node:true, unused:false, varstmt:true */

const async = require("async");
const Stream = require("stream");
const temp = require("temp");
const fs = require("fs");
const LengthRewriter = require("./transforms").LengthRewriter;
const KeyExtractor = require("./transforms").KeyExtractor;
const ByteWriter = require("./transforms").ByteWriter;
const ObjectCounter = require("./transforms").ObjectCounter;
const zlib = require("zlib");

const create_package = require("./package").create_package;


temp.track();

function ObjectWriter(stream,options) {
  options = options || {};
  if (options.gzip) {
    let gz = zlib.createGzip();
    gz.pipe(stream);
    this.stream = gz;
  } else {
    this.stream = stream;
  }
}

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

const NILVALUESXP= 254;
const REFSXP     = 255;

const LATIN1_MASK  = (1<<2);
const UTF8_MASK    = (1<<3);
const ASCII_MASK   = (1<<6);

const IS_OBJECT_BIT_MASK = (1 << 8);
const HAS_ATTR_BIT_MASK  = (1 << 9);
const HAS_TAG_BIT_MASK   = (1 << 10);

const NA_INT = -1*Math.pow(2,31);
const NA_STRING = -1;
// This is a special R constant value
const NA_REAL = new Buffer("7ff00000000007a2","hex");

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
  if (options.is_object) {
    flags |= IS_OBJECT_BIT_MASK;
  }

  if (options.has_attributes) {
    flags |= HAS_ATTR_BIT_MASK;
  }

  if (options.has_tag) {
    flags |= HAS_TAG_BIT_MASK;
  }

  return flags;
};

const writeHeader = function() {
  this.write(new Buffer("RDX2\nX\n"));
  this.write(encode_int(2));
  this.write(encode_int(packed_version(3,0,0)));
  this.write(encode_int(packed_version(2,3,0)));
  return Promise.resolve();
};

const writeValue = function(value,type,length) {
  let self = this;
  // When we pass a buffer stream to the writeValue, we assume
  // that we are taking a stream of encoded bytes
  // If we wish to rewrite the length portion of the
  // written out bytes, (i.e. the length is defined)
  // we should modify the stream of bytes on the fly
  if (value instanceof Stream.Readable && ! value._readableState.objectMode) {
    let target_stream = value;

    if (typeof length !== "undefined") {
      target_stream = value.pipe(new LengthRewriter(length));
    }
    target_stream.pipe(self.stream,{end:false});

    return new Promise(function(resolve,reject) {
      target_stream.on("end",resolve);
      value.on("error",reject);
    });
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
    value_written = this.dataFrame(value,type.keys,type.types,(type.attributes ? { attributes: type.attributes } : {}));
  }

  if ( ! value_written ) {
    value_written = Promise.reject(new Error("No valid data type given"));
  }

  return value_written;
};

const write_vector = function(vector,method) {
  let self = this;
  if (vector instanceof Stream && vector._readableState.objectMode) {
    let byte_pipe = vector.pipe( new ByteWriter(method.bind(self)) );
    byte_pipe.pipe(this.stream,{end: false});
    return new Promise(function(resolve) {
      // We want to specifically end
      // the promise once we have no more
      // readable elements left in the queue
      byte_pipe.on("end", resolve);
    });
  } else {
    return new Promise(function(resolve,reject) {
      async.eachOfSeries(vector,function(el,idx,callback) {
        self.write(method(el));
        process.nextTick(callback);
      },function(err) {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    }).catch(function(err) {
      console.log(err);
    });
  }

};

const stringScalar = function(string) {
  // NA val - 0000 0009 ffff ffff
  let type = encode_int(CHARSXP | (ASCII_MASK << 12) );
  if (string === null) {
    type = encode_int(CHARSXP);
    return Buffer.concat([type,encode_int(NA_STRING)]);
  }
  let bytes = new Buffer(string);
  let length = encode_int(bytes.length);
  return Buffer.concat([type,length,bytes]);
};

const realScalar = function(real) {
  if (real === null) {
    return NA_REAL;
  }
  return encode_real(real);
};

const intScalar = function(int) {
  if (int === null) {
    int = NA_INT;
  }
  if ( ! Number.isFinite(int) ) {
    int = NA_INT;
  }
  return encode_int(int);
};

const logicalScalar = function(bool) {
  if (bool === null) {
    return encode_int(NA_INT);
  }
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
  return new Promise(function(resolve,reject) {
    async.eachOfSeries(keys,function(key,idx,callback) {
      self.write(encode_int(encode_flags(LISTSXP,{ has_tag: true })));
      symbol.call(self,key);
      writeValue.call(self,pairs[key],types[idx]).then(callback).catch(callback);
    }, function(err) {
      if (err) {
        reject(err);
      } else {
        self.write(encode_int(NILVALUESXP));
        resolve();
      }
    });
  });
};

const environment = function(pairs,types_map) {
  let self = this;
  let keys = Object.keys(pairs);
  let types = keys.map( (key) => types_map[key]);
  return self.listPairs(pairs,keys,types);
};

const consume_frame_stream = function(objects,keys,types,options) {
  let self = this;
  let outputs = keys.map(function(key,idx) {
    let stream = new Stream.PassThrough({objectMode: true});
    let output = new ObjectWriter(temp.createWriteStream());
    output.path = output.stream.path;
    // The streams we write out have incorrect
    // length values, but they get rewritten
    // once we put the stream back together again
    let promise = writeValue.call(output,stream,types[idx],0);
    promise.catch((err) => console.log(err));
    promise.then(() => output.stream.end() );
    output.promise = new Promise(function(resolve,reject) {
      output.stream.on("finish",resolve);
      output.stream.on("error",reject);
    });
    output.instream = stream;
    return output;
  });
  let counter = new ObjectCounter();
  objects.setMaxListeners(2*outputs.length);
  outputs.forEach( (output,idx) => { (idx === 0 ? objects.pipe(counter) : objects).pipe( new KeyExtractor(keys[idx]) ).pipe(output.instream); });
  return Promise.all( outputs.map( (output) => output.promise )).then(function() {
    let filenames = outputs.map( (output) => output.path );
    let streams = filenames.map( (file) => fs.createReadStream(file));
    let frame = {};
    keys.forEach(function(key,idx) {
      frame[key] = streams[idx];
    });
    options.length = counter.total;
    return self.dataFrame(frame,keys,types,options);
  });
};

const extract_length = function(stream) {
  return new Promise((resolve,reject) => {
    stream.on("error",reject);
    stream.once("data", (dat) => {
      if (dat.length < 8) {
        reject(new Error("Didn't read enough bytes in"));
      } else {
        resolve(dat.readInt32BE(4));
      }
    });
    stream.pause();
  });
};

const dataFrame = function(object,keys,types,options) {
  let self = this;
  if ( ! options ) {
    options = {};
  }
  let length = options.length;
  if (object instanceof Stream && object._readableState.objectMode) {
    return (consume_frame_stream.bind(self))(object,keys,types,options);
  }
  this.write(encode_int(encode_flags(VECSXP,{ is_object: true, has_attributes: true })));
  this.write(encode_int(keys.length));
  if ( length === null || typeof length === "undefined" ) {
    length = object[keys[0]].length;
  }
  if ( (length === null || typeof length === "undefined") && object[keys[0]] instanceof Stream ) {
    length = extract_length(object[keys[0]]);
  } else {
    length = Promise.resolve(length);
  }
  length.then((val) => console.log("Writing data frame of length ",val));

  return new Promise(function(resolve,reject) {
    async.eachOfSeries(keys,function(column,idx,callback) {
      writeValue.call(self,object[column],types[idx],options.length).then(callback);
    }, function(err) {
      if (err) {
        reject(err);
      } else {
        length.then((length_val) => {
          let attributes = { "names": keys, "class" : ["data.frame"], "row.names" : [NA_INT,-1*length_val] };
          let attribute_names = [ "names","row.names","class" ];
          let attribute_types = ["string","int","string"];
          if (options.attributes) {

            options.attributes.names.forEach( (key,idx) => {
              if (attribute_names.indexOf(key) >= 0) {
                return;
              }
              attribute_names.push(key);
              attribute_types.push(options.attributes.types[idx]);
              attributes[key] = options.attributes.values[key];
            });
          }
          return self.listPairs(attributes,attribute_names,attribute_types);
        }).then(resolve);
      }
    });
  });
};

ObjectWriter.prototype.write = function(buffer) {
  this.stream.write(buffer);
};

ObjectWriter.prototype.finish = function() {
  let self = this;
  return new Promise(function(resolve,reject) {
    self.stream.on("close",resolve);
    self.stream.on("finish",resolve);
    self.stream.on("error",reject);
    self.stream.end();
  });
};



ObjectWriter.prototype.stringVector = stringVector;
ObjectWriter.prototype.realVector   = realVector;
ObjectWriter.prototype.intVector    = intVector;
ObjectWriter.prototype.logicalVector= logicalVector;
ObjectWriter.prototype.listPairs    = listPairs;
ObjectWriter.prototype.environment  = environment;
ObjectWriter.prototype.dataFrame    = dataFrame;
ObjectWriter.prototype.writeHeader  = writeHeader;




module.exports = ObjectWriter;
module.exports.suffix = "RData.tar.gz";
module.exports.package = create_package;