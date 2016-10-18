"use strict";

const fs = require('fs');
const zlib = require('zlib');

const Writer = require('./lib/object_writer');

const dataframe = { "x" : [2,4,8,16,32], "y" : ["ab","ac","ad","ae","af"], "z" : [false,false,true,true,true]};

let obj_writer = null;

let intwriter1 = fs.createWriteStream('ints1.Rda');
let intwriter2 = fs.createWriteStream('ints2.Rda');


obj_writer = new Writer(intwriter1);

obj_writer.writeValue(intstream,{ "type" : "int", "count" : 1e06 });

intstream.on('end',() => obj_writer.stream.end() );