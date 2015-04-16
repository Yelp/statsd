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
 *    debug: true,
 *    signalfuse: {
 *      host: "",
 *      token: "",
 *      globalPrefix: "",
 *      useMultiKey: true
 *    }
 *  }
 *
 */

var http = require('https');
var util = require('util');

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
  this.sfxConfig.host = this.sfxConfig.host || "";
  this.sfxConfig.token = this.sfxConfig.token || "";

  if(this.sfxConfig.dryrun == undefined)
    this.sfxConfig.dryrun = false;

  if(this.sfxConfig.useMultiKey == undefined)
    this.sfxConfig.useMultiKey = true;

  // add the '.' so that we can just jam keys onto the end
  if(this.sfxConfig.globalPrefix == "" || this.sfxConfig.globalPrefix === undefined) {
    this.sfxConfig.globalPrefix = "";
  } else {
    this.sfxConfig.globalPrefix += '.';
  }

  this.sfxConfig.http = http;
  this.sfxConfig.onComplete = this.onComplete;
  this.sfxConfig.post = this.post;

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

SignalFuseBackend.prototype.onComplete = function(rsp) {
  if(rsp.statusCode != 200) {
    rsp.setEncoding('utf8');
    l.log('Post to signalfx FAILED ' + rsp.statusCode + " " + rsp.statusMessage);
    rsp.on('data', function(chunk) {
      l.log(chunk);
    });
  } else {
    l.debug("Finished Flush to signalfx: " + rsp.statusCode + " " + rsp.statusMessage);
  }
}

//
// Parses the key into its metric name and actual tags
// The expected format is a JSON list of tuples: (key, value)
// For example::
//
//    [
//        [
//            "metric_name",
//            "myname"
//        ],
//        [
//            "sometag",
//            "tagsval"
//        ]
//    ]
//
// The name 'metric_name' is reserved to mean exactly what it says
SignalFuseBackend.prototype.parseMultiKey = function(stringKey) {
  var parsed = undefined
  try {
    parsed = JSON.parse(stringKey);
  } catch(err) {
    l.debug("Failed to parse '" + stringKey + "' into valid JSON");
    return undefined
  }

  var mapVersion = {};
  parsed.forEach(function(item) {
    mapVersion[item[0]] = item[1];
  });
  mname = mapVersion['metric_name'];
  delete mapVersion['metric_name'];
  return {metric_name: mname, tags: mapVersion};
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
// UNLESS the 'useMultiKey' config value is enabled. Then it is parsed
// according to those rules. See 'parseMultiKey'
//
SignalFuseBackend.prototype.parseKey = function(rawkey) {
  var metricName = "";
  var tags = {}

  if(this.sfxConfig.useMultiKey == true) {
    var parsed = this.parseMultiKey(rawkey);
    if(parsed != undefined) {
      metricName = parsed['metric_name'];
      tags = parsed['tags'];
    }
  }

  if(metricName == "") {
    var cleanerKey = rawkey.replace(/\s+/g, '_')
                           .replace(/\//g, '-')
                           .replace(/[^a-zA-Z_\-0-9=\.]/g, '');

    parts = cleanerKey.split('.');
    metricParts = [parts[0]];
    if(metricParts[0].indexOf('=') >= 0) {
      return {}; // can't start with an '=' in it
    }

    for(i = 1; i < parts.length; i++) {
      if(parts[i].indexOf('=') >= 0) {
        var tagParts = parts[i].split('=');
        tags[tagParts[0]] = tagParts[1];
      } else {
        metricParts.push(parts[i]);
      }
    }

    metricName = metricParts.join('.');
  }

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
          var fqMetricName = globalPrefix + [keyName, subKey].join('.');
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

      var fqMetricName = globalPrefix + keyName;
      var events = timers[rawKey];
      for(var i = 0; i < events.length; i++) {
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
  l.debug('Starting to process ' + Object.keys(metrics).length + ' ' + type);

  var globalPrefix = this.sfxConfig.globalPrefix;
  var resultingStats = [];

  for(rawKey in metrics) {
    if(metrics.hasOwnProperty(rawKey)) {
      var value = metrics[rawKey];

      var metricParts = this.parseKey(rawKey);
      var keyName = metricParts['metricName'];
      var tags = metricParts['tags'];
      tags['type'] = type;

      var fqMetricName = globalPrefix + keyName;
      resultingStats.push(buildStat(fqMetricName, value, tags));
    }
  }

  l.debug("Finished transforming " + type + " into " +
          resultingStats.length + " signalfuse metrics");

  return resultingStats;
};

//
// Takes each of the different components, transformts them, then ships
// ships them off
//
SignalFuseBackend.prototype.flush = function(timestamp, metric, postcb) {
  l.debug(timestamp + ' starting a flush');

  var resultingMetrics = [];
  var partial = [];

  partial = this.transformMetrics(metric.counters, 'counter');
  Array.prototype.push.apply(resultingMetrics, partial);
  partial = this.transformMetrics(metric.sets, 'set');
  Array.prototype.push.apply(resultingMetrics, partial);
  partial = this.transformMetrics(metric.gauges, 'gauge');
  Array.prototype.push.apply(resultingMetrics, partial);
  partial = this.transformMetrics(metric.counter_rates, 'rate');
  Array.prototype.push.apply(resultingMetrics, partial);
  partial = this.transformTimers(metric.timers);
  Array.prototype.push.apply(resultingMetrics, partial);
  partial = this.transformTimerData(metric.timer_data);
  Array.prototype.push.apply(resultingMetrics, partial);

  postcb(resultingMetrics, this.sfxConfig);
}

SignalFuseBackend.prototype.post = function(metricList, sfxConfig) {
  l.debug('Sending ' + metricList.length + ' metrics to signal fuse');

  var postOptions = {
    host: sfxConfig.host,
    path: '/v2/datapoint',
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-SF-Token': sfxConfig.token
    }
  };

  var out = {gauge: metricList};
  var postData = JSON.stringify(out);

  l.debug('Payload will be:\n' + util.inspect(out, {depth:5, colors:true}));

  if(!sfxConfig.dryrun) {
    var req = sfxConfig.http.request(postOptions, sfxConfig.onComplete);
    req.on('error', function(res) {
      log.log("Somethign went terribly wrong trying to send data to signalfx");
      log.log("Payload:\n" + util.inspect(out, {depth:5, colors:true}));
      log.log("Error:" + util.inspect(res, {depth:5, colors:true}));
    });

    req.write(postData);
    req.end();
  } else {
    l.log('Not sending because of dryrun flag. request:');
    l.log(postData);
  }

  l.debug('Finished sending data to signalfx');
}

SignalFuseBackend.prototype.status = function(callback) {
  callback('not yet implemented', 'signalfuse');
}

// ---------------------------------------------------------------------------
// export a build method
// ---------------------------------------------------------------------------
exports.init = function(startup_time, config, events, logger) {
  debug = config.debug;
  l = logger;

  var instance = new SignalFuseBackend(startup_time, config, events);
  return instance;
};

