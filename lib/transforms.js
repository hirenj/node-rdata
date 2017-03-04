"use strict";
/*jshint esversion: 6, node:true, unused:false, varstmt:true */

const Transform = require("stream").Transform;
const inherits = require("util").inherits;
const Null = Symbol("Null value");

function LengthRewriter(length,options) {
  if ( ! (this instanceof LengthRewriter)) {
    return new LengthRewriter(length,options);
  }

  if (! options) {
    options = {};
  }
  options.objectMode = false;
  this.written_count = 0;
  this.length = length;
  Transform.call(this, options);
}

inherits(LengthRewriter, Transform);

LengthRewriter.prototype._transform = function _transform(buf, encoding, callback) {

  let written_count = this.written_count;

  if (written_count <= 4) {
    let offset = 4 - written_count;
    if (offset >= 0 && offset < buf.length) {
      buf.writeInt32BE(this.length || 0,offset);
    }
    written_count += buf.length;
  }

  this.written_count = written_count;

  this.push(buf);
  callback();
};

function KeyExtractor(key,options) {
  if ( ! (this instanceof KeyExtractor)) {
    return new KeyExtractor(key,options);
  }

  if (! options) {
    options = {};
  }
  options.objectMode = true;
  console.log("Extracting key",key);
  this.key = key;
  Transform.call(this, options);
}

inherits(KeyExtractor, Transform);

KeyExtractor.prototype._transform = function _transform(obj, encoding, callback) {
  this.push(obj[this.key] || Null);
  callback();
};


function ByteWriter(transform,options) {
  if ( ! (this instanceof ByteWriter)) {
    return new ByteWriter(transform,options);
  }

  if (! options) {
    options = {};
  }
  options.objectMode = true;
  this.transform = transform;
  Transform.call(this, options);
  this._readableState.objectMode = false;
}

inherits(ByteWriter, Transform);

ByteWriter.prototype._transform = function _transform(obj, encoding, callback) {
  this.push(this.transform(obj === Null ? null : obj));
  callback();
};


function ObjectCounter(options) {
  if ( ! (this instanceof ObjectCounter)) {
    return new ObjectCounter(options);
  }

  if (! options) {
    options = {};
  }
  options.objectMode = true;
  this.total = 0;
  Transform.call(this, options);
}

inherits(ObjectCounter, Transform);

ObjectCounter.prototype._transform = function _transform(obj, encoding, callback) {
  this.total += 1;
  this.push(obj);
  callback();
};


exports.ObjectCounter = ObjectCounter;
exports.ByteWriter = ByteWriter;
exports.KeyExtractor = KeyExtractor;
exports.LengthRewriter = LengthRewriter;