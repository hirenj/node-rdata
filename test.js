"use strict";

const Validator = require('jsonschema').Validator;

const validate = function(instance,schema) {
  return (new Validator()).validate(instance,schema);
};

const fs = require('fs');

const Writer = require('./lib/object_writer');

const dataframe = { "x" : [2,4,8,16,32], "y" : ["ab","ac","ad","ae","af"]};

let obj_writer = null;

// let framewriter = fs.createWriteStream('frame.Rda');

// let obj_writer = new Writer(framewriter);

// obj_writer.dataFrame(dataframe,["x","y"],["real","string"]);
setTimeout(function() {

// obj_writer.stream.end();

let writer = fs.createWriteStream('test.Rda');

let written_frame = fs.createReadStream('frame.Rda');
let written_frame2 = fs.createReadStream('frame.Rda');

obj_writer = new Writer(writer);

obj_writer.writeHeader();
obj_writer.listPairs( {"frame" : written_frame, "frame2" : written_frame2 },
                      ["frame", "frame2"],
                      [ 
                      ]);
setTimeout(function() {
obj_writer.stream.end();
},2000);

},1000);


// { "type" : "dataframe",
//                           "keys" : ["x","y"],
//                           "types" : ["real","string"]
//                         }