'use strict';
/*jshint esversion: 6, node:true, unused:false, varstmt:true */

const archiver = require('archiver');
const zlib = require('zlib');
const fs = require('fs');

const generate_description = function(filedata, prefix) {
  let title = filedata.title;
  let version = filedata.version;
  if ( prefix ) {
    prefix = `${prefix}.`;
  }
  let now = new Date().toISOString().split('T')[0];
  let date = now;
  let description = `\
Package: ${prefix}${title}
Version: ${version}
Date: ${date}
Depends: R (>= 3.1.0)
Description: ${title}
Title: ${title}
LazyData: yes
NeedsCompilation: yes`;
  return description;
};

const create_package = function(filedata, package_info ) {
  let data_filename = package_info.data_filename;
  let description = package_info.description;
  let package_prefix = package_info.prefix || '';
  let gz = zlib.createGzip();
  let archive = archiver('tar', { store: true });
  archive.pipe(gz);
  archive.append(fs.createReadStream(filedata.path), { name: `${filedata.title}/data/${data_filename}.rda` });
  archive.append('', { name: `${filedata.title}/NAMESPACE` });
  archive.append(generate_description(filedata,package_prefix),{ name: `${filedata.title}/DESCRIPTION` });
  archive.finalize();
  return gz;
};

exports.create_package = create_package;