"use strict";

const ObjectWriter = require('..');
const chai = require('chai');
const expect = chai.expect;
const assert = chai.assert;
const tempfile = require('temp');

// tempfile.track();

const run_rscript = function(file,command) {
  var exec = require('child_process').exec;
  var cmd = 'R --silent -e "load(\''+file+'\'); '+command+';" | grep -v ">"';
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

const dataframe = { 'x' : [2,4,8,16,32], 'y' : ['ab','ac','ad','ae','af'], 'z' : [false,false,true,true,true]};
const dataframe2 = { 'x' : [2,4,8], 'y' : ['ab','ac','ad'], 'z' : [false,false,true]};

describe('Basic writing', function() {
  it('Writes a data frame',function(done){
    let writer = new ObjectWriter(tempfile.createWriteStream());
    let path = writer.stream.path;
    writer.writeHeader();
    writer.listPairs( {'frame' : dataframe },['frame'],[ { 'type': 'dataframe', 'keys' : ['x','y','z'], 'types' : ['int','string','logical'] }])
    .then(() => writer.finish() )
    .then( () => row_count(path,'frame') )
    .then( (count) => { expect(count).equals(5); })
    .then( () => done() )
    .catch( done );
  });
});

describe('Multiple variables in an environment', function() {
  it('Writes two data frames',function(done){
    let writer = new ObjectWriter(tempfile.createWriteStream());
    let path = writer.stream.path;
    writer.writeHeader();
    writer.listPairs( {'frame' : dataframe, 'frame2' : dataframe2 },
                      ['frame', 'frame2'],
                      [ { 'type': 'dataframe', 'keys' : ['x','y','z'], 'types' : ['int','string','logical'] },
                        { 'type': 'dataframe', 'keys' : ['x','y','z'], 'types' : ['int','string','logical'] }
                      ])
    .then(() => writer.finish() )
    .then( () => row_count(path,'frame') )
    .then( (count) => { expect(count).equals(5); })
    .then( () => row_count(path,'frame2') )
    .then( (count) => { expect(count).equals(3); })
    .then( () => done() )
    .catch( done );
  });
});
