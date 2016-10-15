"use strict";

const Validator = require('jsonschema').Validator;

const validate = function(instance,schema) {
  return (new Validator()).validate(instance,schema);
};

const fs = require('fs');

const Writer = require('./lib/object_writer');

const dataframe = { "x" : [2,4,8,16,32], "y" : ["ab","ac","ad","ae","af"]};


let writer = fs.createWriteStream('test.Rda');

let obj_writer = new Writer(writer);

obj_writer.writeHeader();
obj_writer.listPairs( {"frame" : dataframe},
                      ["frame"],
                      [ { "type" : "dataframe",
                          "keys" : ["x","y"],
                          "types" : ["real","string"]
                        }
                      ]);
obj_writer.stream.end();