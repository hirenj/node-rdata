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

const nan_index = function(file,variable,column) {
  return run_rscript(file,'which(is.nan('+variable+'$'+column+'))').then( (val) => { return val.replace('\n','').split(/\s+/).filter( (idx) => idx !== '' ).map( (idx) => parseInt(idx)); } );
}


const dataframe = { 'x' : [2,4,null,8,parseInt('x'),32], 'y' : ['ab','ac','ad',null,'ae','af'], 'z' : [false,false,true,true,null,true], 'xreal' : [2,4,null,8,parseInt('x'),32]};

describe('Writing NA values', function() {
  it('Writes a data frame',function(done){
    this.timeout(5000);
    let writer = new ObjectWriter(tempfile.createWriteStream());
    let path = writer.stream.path;
    writer.writeHeader();
    writer.listPairs( {'frame' : dataframe },['frame'],[ { 'type': 'dataframe', 'keys' : ['x','y','z','xreal'], 'types' : ['int','string','logical','real'] }])
    .then(() => writer.finish() )
    .then( () => row_count(path,'frame') )
    .then( (count) => { expect(count).equals(6); })
    .then( () => na_index(path,'frame','x') )
    .then( (indices) => { expect(indices).eql([3,5]); })
    .then( () => nan_index(path,'frame','x') )
    .then( (indices) => { expect(indices).eql([]); })
    .then( () => na_index(path,'frame','xreal') )
    .then( (indices) => { expect(indices).eql([3,5]); })
    .then( () => nan_index(path,'frame','xreal') )
    .then( (indices) => { expect(indices).eql([5]); })
    .then( () => na_index(path,'frame','y') )
    .then( (indices) => { expect(indices).eql([4]); })
    .then( () => na_index(path,'frame','z') )
    .then( (indices) => { expect(indices).eql([5]); })

    .then( () => done() )
    .catch( done );
  });
});