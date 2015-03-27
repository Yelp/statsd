/*
 * This will write to an actual instance of signalfuse. It responds to
 * the 'stats' and 'flush' commands.
 *
 * On stats it will return the following stats:
 *
 *  - TODO
 *
 * On flush it will write out messages to signalfuse with multidimensional tagging.
 * Configuration includes::
 *
 *  {
 *    ...
 *    signalfuse: {
 *      host: "",
 *      port: #,
 *      token: "",
 *      globalPrefix: ""
 *    }
 *  }
 *
 */

var http = require('https');

var l; // the logger
var debug;

// ---------------------------------------------------------------------------
// HELPER functions
// ---------------------------------------------------------------------------
function buildStat(name, value, tags) {
  return {
    metric: name,
    value: value,
    dimensions: tags
  }
}

// ---------------------------------------------------------------------------
// The actual BACKEND
// ---------------------------------------------------------------------------
function SignalFuseBackend(startup_time, config, emitter) {
  var c = config.signalfuse || {};

  this.sfxConfig = c;
  this.config = config || {};

  var self = this;
  emitter.on('flush', function(timestamp, metrics) {
    self.flush(timestamp, metrics, self.post);
  });
  emitter.on('status', function(callback) {
    self.status(callback);
  });
}

SignalFuseBackend.prototype.getConfig = function() {
  return this.sfxConfig;
}

//
// This is intended to split the key itself into its tags and name
// some rules::
//
//    - '/' are replaced with '-'
//    - ' ' spaces (incl mutlitple) are replaced with '_'
//    - characters not in: [a-zA-Z], [0-9], [-_=.] are removed
//
// then the key is split on its '.' and the first is start
// of the metric's name. The rest are evaluated as such:
//
//    - if they contain an '=' they are a tag
//    - otherwise they are part of the name
//
//
SignalFuseBackend.prototype.parseKey = function(rawkey) {
  var metricName = "";
  var tags = {}

  var cleanerKey = rawkey.replace(/\s+/g, '_')
                         .replace(/\//g, '-')
                         .replace(/[^a-zA-Z_\-0-9=\.]/g, '');

  parts = cleanerKey.split('.');
  metricParts = [parts[0]];
  if(metricParts[0].indexOf('=') >= 0) {
    return {}; // can't start with an '=' in it
  }

  for (i = 1; i < parts.length; i++) {
    if(parts[i].indexOf('=') >= 0) {
      var tagParts = parts[i].split('=');
      tags[tagParts[0]] = tagParts[1];
    } else {
      metricParts.push(parts[i]);
    }
  }

  metricName = metricParts.join('.');

  return {metricName: metricName, tags: tags};
}

//
// handles transforming metrics of shape::
//
//  { metricname: { subkey: #, subkey: # ..}, .. }
//
// into discrete metrics, that are metricname.subkey = #
//
SignalFuseBackend.prototype.transformTimerData = function(timerData) {
  l.debug('Starting to process ' + Object.keys(timerData).length + ' timers');

  var globalPrefix = this.sfxConfig.globalPrefix;
  var resultingStats = [];

  for(rawKey in timerData) {
    if(timerData.hasOwnProperty(rawKey)) {
      var metricParts = this.parseKey(rawKey);
      var keyName = metricParts['metricName'];
      var tags = metricParts['tags'];
      tags['type'] = 'timerdata';

      for(subKey in timerData[rawKey]) {
        if(timerData[rawKey].hasOwnProperty(subKey)) {
          var val = timerData[rawKey][subKey];
          var fqMetricName = [globalPrefix, keyName, subKey].join('.');
          resultingStats.push(buildStat(fqMetricName, val, tags));
        }
      }
    }
  }

  return resultingStats;
}

//
// handles transforming metrics of this shape::
//
//   { 'metricname': [ #, #, #..], ... }
//
// turns them into discrete events
//
SignalFuseBackend.prototype.transformTimers = function(timers) {
  l.debug('Starting to process ' + Object.keys(timers).length + ' timers');

  var globalPrefix = this.sfxConfig.globalPrefix;
  var resultingStats = [];

  for(rawKey in timers) {
    if(timers.hasOwnProperty(rawKey)) {
      var metricParts = this.parseKey(rawKey);
      var keyName = metricParts['metricName'];
      var tags = metricParts['tags'];
      tags['type'] = 'timer';

      var fqMetricName = [globalPrefix, keyName].join('.')
      var events = timers[rawKey];
      for(var i = 0; i < events.length; i++){
        resultingStats.push(buildStat(fqMetricName, events[i], tags));
      }
    }
  }

  l.debug("Finished transforming metrics into " +
          resultingStats.length + " signalfuse metrics");

  return resultingStats;
}

//
// This will transform gauges, counters, and sets
// naming goes: globalPrefix.[metricname]
// also the tags will get a 'type: type' added
//
SignalFuseBackend.prototype.transformMetrics = function(metrics, type) {
  l.debug('Starting to process ' + Object.keys(metrics).length + ' metrics');

  var globalPrefix = this.sfxConfig.globalPrefix;
  var resultingStats = [];

  for(rawKey in metrics) {
    if(metrics.hasOwnProperty(rawKey)) {
      var value = metrics[rawKey];

      var metricParts = this.parseKey(rawKey);
      var keyName = metricParts['metricName'];
      var tags = metricParts['tags'];
      tags['type'] = type;

      var fqMetricName = [globalPrefix, keyName].join('.');
      resultingStats.push(buildStat(fqMetricName, value, tags));
    }
  }

  l.debug("Finished transforming metrics into " +
          resultingStats.length + " signalfuse metrics");

  return resultingStats;
};

SignalFuseBackend.prototype.flush = function(timestamp, metric, postcb) {
  console.log(timestamp + " " + metric  + " " + postcb);
  postcb({dict: 'dict'});
}

SignalFuseBackend.prototype.post = function(dict) {
  console.log('going to post ' +  dict);
}

SignalFuseBackend.prototype.status = function(callback) {
    callback.write(0, "Not yet implemented");
}

// ---------------------------------------------------------------------------
// export a build method
// ---------------------------------------------------------------------------
exports.init = function(startup_time, config, events, logger) {
  debug = config.debug;
  l = logger;
  l.__proto__.debug = function(msg) {
    if(debug) {
      l.log(msg);
    }
  };

  var instance = new SignalFuseBackend(startup_time, config, events);
  return instance;
};

