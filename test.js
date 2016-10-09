"use strict";

const fs = require('fs');


const NILSXP   =   0;  /* nil = NULL */
const SYMSXP   =   1;    /* symbols */
const LISTSXP   =    2;    /* lists of dotted pairs */
const CLOSXP    =  3;    /* closures */
const ENVSXP    =  4 ;  /* environments */
const PROMSXP    =   5 ;  /* promises: [un]evaluated closure arguments */
const LANGSXP    =   6;    /* language constructs (special lists) */
const SPECIALSXP =  7;   /* special forms */
const BUILTINSXP =  8;   /* builtin non-special forms */
const CHARSXP    =   9;    /* "scalar" string type (internal only)*/
const LGLSXP    = 10 ;  /* logical vectors */
const INTSXP    = 13;    /* integer vectors */
const REALSXP   =   14 ;  /* real variables */
const CPLXSXP   =   15;    /* complex variables */
const STRSXP    = 16 ;  /* string vectors */
const DOTSXP    = 17 ;  /* dot-dot-dot object */
const ANYSXP    = 18;    /* make "any" args work.
           Used in specifying types for symbol
           registration to mean anything is okay  */
const VECSXP   =  19;    /* generic vectors */
const EXPRSXP  =    20;    /* expressions vectors */
const BCODESXP  =  21;    /* byte code */
const EXTPTRSXP  = 22;    /* external pointer */
const RAWSXP     = 24;    /* raw bytes */
const S4SXP      = 25;    /* S4, non-vector */
const FUNSXP    =  99;    /* Closure or Builtin or Special */
const REFSXP =           255 ;

const LATIN1_MASK  = (1<<2);
const UTF8_MASK = (1<<3);
const ASCII_MASK =  (1<<6);


let packed_version = function(v, p, s) {
  return s + (p * 256) + (v * 65536);
}

let encode_int = function(value) {
  let buf = new Buffer(4);
  buf.writeInt32BE(value);
  console.log(buf);
  return buf;
};

let encode_flags = function(type,is_object,has_attributes,has_tag) {
  let flags = type;
  if (is_object)
    flags |= IS_OBJECT_BIT_MASK;

  if (has_attributes)
    flags |= HAS_ATTR_BIT_MASK;

  if (has_tag)
    flags |= HAS_TAG_BIT_MASK;

  return flags;
}

const dataframe = { "cola" : [1,2,3], "colb" : ["a","b","c"]};


let writer = fs.createWriteStream('test.Rda')

// Write header stuff

writer.write(new Buffer('RDX2\nX\n'));
writer.write(encode_int(2));
console.log("Here");
writer.write(encode_int(packed_version(3,0,0)));
writer.write(encode_int(packed_version(2,3,0)));

// Write data frame

// Write the list container first
writer.write(encode_int(encode_flags(VECSXP,false,true,false)));
writer.write(encode_int(Object.keys(dataframe).length));

writer.write(encode_int(encode_flags(INTSXP,false,false,false)));
writer.write(encode_int(dataframe["cola"].length));
dataframe["cola"].forEach(function(el) {
  writer.write(encode_int(el));
});

writer.write(encode_int(encode_flags(STRSXP,false,false,false)));
writer.write(encode_int(dataframe["colb"].length));
dataframe["colb"].forEach(function(el) {
  writer.write(encode_int(SexpType.CHARSXP | (UTF8_MASK << 12) ));
  let bytes = Buffer.from(el, 'utf8');
  writer.write(encode_int(bytes.length));
  writer.write(bytes);
});

// Write attributes
writer.write(encode_int(encode_flags(LISTSXP,false,false,false)));

// Need to write symbol and then value for:
// names (string vector), class (string vector) and row.names (int vector?)
// end with null value

writer.end();