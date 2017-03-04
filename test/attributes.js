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

const get_attribute = function(file,variable,key) {
  return run_rscript(file,`attributes(${variable})\$${key}`).then( val => val.replace('\n','') );
}

const dataframe = { 'x' : [2,4,8,16,32], 'y' : ['ab','ac','ad','ae','af'], 'z' : [false,false,true,true], 'xreal' : [2,4,8,16,32]};

describe('Writing attributes onto a frame', function() {
  it('Writes a data frame',function(done){
    let writer = new ObjectWriter(tempfile.createWriteStream());
    let path = writer.stream.path;
    writer.writeHeader();
    writer.listPairs( {'frame' : dataframe },['frame'],[ { 'type': 'dataframe',
                                                           'keys' : ['x','y','z','xreal'],
                                                           'types' : ['int','string','logical','real'],
                                                           'attributes' : { values: { 'foo' : ['bar'] },
                                                                            names: ['foo'],
                                                                            types: ['string']
                                                                          } }])
    .then(() => writer.finish() )
    .then( () => get_attribute(path,'frame','foo') )
    .then( (value) => { expect(value).equals('\"bar\"'); })
    .then( () => done() )
    .catch( done );
  });
});