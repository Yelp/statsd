var getHostname = require('os').hostname

function  createBackendLogger(backendname, use_debug, base_logger) {
  hostname = "";
  try {
    hostname = getHostname();
  } catch(err) {
    if(use_debug) {
      base_logger.log("Failed to load the hostname for the log", "ERROR");
      base_logger.log(err);
    }
  }

  var logPrefix = hostname + " " + backendname;
  var backendLogger = {
    log: function(msg, type) {
      base_logger.log(logPrefix + ": " + msg, type);
    },
    debug: function(msg) {
      if(use_debug) {
        base_logger.log(logPrefix + ": " + msg, 'DEBUG');
      }
    },
    info: function(msg) {
      base_logger.log(logPrefix + ": " + msg, 'INFO');
    },
    error: function(msg) {
      base_logger.log(logPrefix + ": " + msg, 'ERROR');
    }
  }

  return backendLogger;
}

module.exports = createBackendLogger
