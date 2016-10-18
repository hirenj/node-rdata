"use strict";

const async = require('async');
const Stream = require('stream');

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

const stringScalar = function(string) {
  this.write(encode_int(CHARSXP | (ASCII_MASK << 12) ));
  let bytes = new Buffer(string);
  this.write(encode_int(bytes.length));
  this.write(bytes);
};

const stringVector = function(vector) {
  let self = this;
  this.write(encode_int(encode_flags(STRSXP)));
  this.write(encode_int(vector.length));
  vector.forEach(function(el) {
    self.stringScalar(el);
  });
};

const realVector = function(vector) {
  let self = this;
  this.write(encode_int(encode_flags(REALSXP)));
  this.write(encode_int(vector.length));
  vector.forEach(function(el) {
    self.write(encode_real(el));
  });
};

const intVector = function(vector) {
  let self = this;
  this.write(encode_int(encode_flags(INTSXP)));
  this.write(encode_int(vector.length));
  vector.forEach(function(el) {
    self.write(encode_int(el));
  });
};

const logicalVector = function(vector) {
  let self = this;
  this.write(encode_int(encode_flags(LGLSXP)));
  this.write(encode_int(vector.length));
  vector.forEach(function(el) {
    self.write(encode_int(el? 1 : 0));
  });
};

const symbol = function(string) {
  this.write(encode_int(encode_flags(SYMSXP)));
  this.stringScalar(string);
};

const listPairs = function(pairs,keys,types) {
  let self = this;
  async.eachOfSeries(keys,function(key,idx,callback) {
    self.write(encode_int(encode_flags(LISTSXP,{ has_tag: true })))
    self.symbol(key);
    (self.writeValue(pairs[key],types[idx]) || Promise.resolve()).then(callback);
  }, function(err) {
    // FIXME - magic value here
    self.write(encode_int(0xfe));
  });
};

const dataFrame = function(object,keys,types) {
  var self = this;
  this.write(encode_int(encode_flags(VECSXP,{ is_object: true, has_attributes: true })));
  this.write(encode_int(keys.length));
  async.eachOfSeries(keys,function(column,idx,callback) {
    (self.writeValue(object[column],types[idx]) || Promise.resolve()).then(callback);
  }, function(err) {
    // FIXME - Magic number for min int value
    let attributes = { "names": keys, "class" : ["data.frame"], "row.names" : [-1*(0x80000000),-1*object[keys[0]].length] };
    self.listPairs(attributes,[ "names","row.names","class" ] ,["string","int","string"]);
  });
};

const writeHeader = function() {
  this.write(new Buffer('RDX2\nX\n'));
  this.write(encode_int(2));
  this.write(encode_int(packed_version(3,0,0)));
  this.write(encode_int(packed_version(2,3,0)));
};

const writeValue = function(value,type) {
  var self = this;
  if (value instanceof Stream) {
    value.on('data',function(buf) {
      self.stream.write(buf);
    });
    return new Promise(function(resolve,reject) {
      value.on('end',function() {
        resolve();
      });
      value.on('error',reject);
    });
  }

  if (type === "string") {
    this.stringVector(value);
  }
  if (type === "int") {
    this.intVector(value);
  }
  if (type === "real") {
    this.realVector(value);
  }
  if (type === "logical") {
    this.logicalVector(value);
  }
  if (type.type === "dataframe") {
    this.dataFrame(value,type.keys,type.types);
  }
};

function ObjectWriter(stream) {
  this.stream = stream;
};

ObjectWriter.prototype.write = function(buffer) {
  this.stream.write(buffer);
};

ObjectWriter.prototype.stringScalar = stringScalar;
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
