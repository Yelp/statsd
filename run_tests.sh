#!/usr/bin/env node
var to_run=['test/']

if (process.argv.length > 2) {
  process.argv.splice(0,2);
  to_run = process.argv
}

try {
    var reporter = require('nodeunit').reporters.default;
}
catch(e) {
    console.log("Cannot find nodeunit module.");
    console.log("Make sure to run 'npm install nodeunit'");
    process.exit();
}

process.chdir(__dirname);
reporter.run(to_run, null, function(failure) {
   process.exit(failure ? 1 : 0)
});
