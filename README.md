# node-rdata

Enable writing of JavaScript objects to R data frames

## Usage
```js
const RData = require('node-rdata');
let output = require('fs').createWriteStream('output.Rdata');

let writer = new RData(output);

let data = { 'x' : [2,4,8,16,32], 'y' : ['ab','ac','ad','ae','af'], 'z' : [false,false,true,true,true]};

let typeinfo =  { 	'type': 'dataframe',
					'keys' : ['x','y','z'],
					'types' : ['int','string','logical']
				};

let realvector = [1,2,3,4,5,6];

// Write the header for the R data file format
writer.writeHeader();

// We can write out all the variables we wish
// into an environment using the listPairs method
writer.environment( {'data' : data, 'realvector' : realvector },{'data' : typeinfo, 'realvector': 'real' })
	  .then( () => writer.finish() );
```

### Stream usage
```js
let data_stream = ...get a stream of objects from somewhere...

// data_stream is an object_mode stream, emitting simple
// objects with keys x,y and z
// e.g.
// { 'x' : 1, 'y' : 'test', 'z' : false }
// { 'x' : 2, 'y' : 'different', 'z' : true }


let typeinfo =  { 	'type': 'dataframe',
					'keys' : ['x','y','z'],
					'types' : ['int','string','logical']
				};

let output = require('fs').createWriteStream('output.Rdata');

let writer = new RData(output);

// Write the header for the R data file format
writer.writeHeader();

// We need to write out the data frame into an environment
writer.environment( {'data' : data_stream },{'data' : typeinfo })
	  .then( () => writer.finish() );

```