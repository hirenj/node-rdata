"use strict";
const ObjectWriter = require('..');
const chai = require('chai');
const expect = chai.expect;
const assert = chai.assert;
const tempfile = require('temp');

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

const na_index = function(file,variable,column) {
  return run_rscript(file,'which(is.na('+variable+'$'+column+'))').then( (val) => { return val.replace('\n','').split(/\s+/).filter( (idx) => idx !== '' ).map( (idx) => parseInt(idx)); } );
}

const infinite_index = function(file,variable,column) {
  return run_rscript(file,'which(is.infinite('+variable+'$'+column+'))').then( (val) => { return val.replace('\n','').split(/\s+/).filter( (idx) => idx !== '' ).map( (idx) => parseInt(idx)); });
}



const dataframe = { 'x' : [2,4,8,-Infinity,Infinity], 'y' : ['ab','ac','ad','ae','af'], 'z' : [2,4,8,-Infinity,Infinity]};

describe('Writing Infinite values', function() {
  it('Correctly writes infinite values',function(done){
    let writer = new ObjectWriter(tempfile.createWriteStream());
    let path = writer.stream.path;
    writer.writeHeader();
    writer.listPairs( {'frame' : dataframe },['frame'],[ { 'type': 'dataframe', 'keys' : ['x','y','z'], 'types' : ['int','string','real'] }])
    .then(() => writer.finish() )
    .then( () => row_count(path,'frame') )
    .then( (count) => { expect(count).equals(5); })
    .then( () => na_index(path,'frame','x') )
    .then( (indices) => { expect(indices).eql([4,5]); })
    .then( () => na_index(path,'frame','z') )
    .then( (indices) => { expect(indices).eql([]); })
    .then( () => infinite_index(path,'frame','z') )
    .then( (indices) => { expect(indices).eql([4,5]); })

    .then( () => done() )
    .catch( done );
  });
});