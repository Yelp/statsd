/*jshint node:true, laxcomma:true */

/*
 * Flush stats to graphite (http://graphite.wikidot.com/).
 *
 * To enable this backend, include 'signalfuse' in the backends
 * configuration array:
 *
 *   backends: ['signalfuse']
 *
 * This backend supports the following config options:
 *
 *   signalfuseHost: Hostname of graphite server.
 *   signalfusePort: Port to contact graphite server at.
 *   signalfuseToken: Organization ID registered to signalfuse.
 */

var http = require('https');

// this will be instantiated to the logger
var l;

var debug;
var flushInterval;
var signalfuseHost;
var signalfusePort;
var signalfuseToken;
var flush_counts;

// prefix configuration
var globalPrefix;
var prefixCounter;
var prefixTimer;
var prefixGauge;
var prefixSet;
var prefixStats;

// set up namespaces
var globalNamespace  = [];
var counterNamespace = [];
var timerNamespace   = [];
var gaugesNamespace  = [];
var setsNamespace    = [];

var signalfuseStats = {};

var post_stats = function signalfuse_post_stats(statString) {
  if (signalfuseHost) {
    var post_options = {
      host: signalfuseHost,
      //port: signalfusePort,
      path: '/v2/datapoint',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-SF-Token': signalfuseToken
      }
    };

    l.log('signalfuse endpoint: ' + signalfuseHost + ':' + signalfusePort + ' token: ' + signalfuseToken);
    // Set up the request
    var post_req = http.request(post_options, function(res) {
      res.setEncoding('utf8');
      var responseString = '';
      res.on('data', function (chunk) {
        l.log('------------------- Signalfuse response: ' + chunk + '\n');
        responseString += chunk;
      });

      res.on('end', function() {
        l.log("------------------- Signalfuse end: " +JSON.parse(responseString) + '\n');
      });

      res.on('error', function(e) {
        l.log("------------------- Signalfuse error: " + e + '\n');
      });
    });
    l.log("After post_req");

    // post the data
    post_req.write(statString);
    l.log("After write");
    post_req.end();
    l.log("After end");
  }
};

var flush_stats = function signalfuse_flush(ts, metrics) {
  var ts_suffix = ' ' + ts + "\n";
  var starttime = Date.now();
  var statDict = {'gauge': []};
  var numStats = 0;
  var key;
  var timer_data_key;
  var counters = metrics.counters;
  var gauges = metrics.gauges;
  var timers = metrics.timers;
  var sets = metrics.sets;
  var counter_rates = metrics.counter_rates;
  var timer_data = metrics.timer_data;
  var statsd_metrics = metrics.statsd_metrics;

  function parse_key(key) {
    tmp_key = key.replace(/\s+/g, '_')
                .replace(/\//g, '-')
                .replace(/[^a-zA-Z_\-0-9=\.]/g, '');

    parts = tmp_key.split('.');
    metric_name = parts[0];
    tags = {};

    for (i = 1; i < parts.length; i++) {
      if (parts[i].indexOf('=') == -1) {
        metric_name += '.' + parts[i];
      } else {
        // tag
        tags[parts[i].split('=')[0]] = parts[i].split('=')[1];
      }
    }
    l.log("Parsed key: " + metric_name);
    l.log("tags: " + JSON.stringify(tags));
    return {'metric_name': metric_name, 'tags': tags};
  };

  for (key in counters) {
    l.log("Processing counter");
    if (key.indexOf('dev7') != -1) continue;
    var value = counters[key];
    var valuePerSecond = counter_rates[key]; // pre-calculated "per second" rate
    var parsed_key = parse_key(key);
    var keyName = parsed_key['metric_name'];
    var tags = parsed_key['tags'];
    var namespace = counterNamespace.concat(keyName);

    var new_stat = {
      'metric': globalPrefix + '.' + namespace.concat('rate').join("."),
      'value': valuePerSecond,
      'dimensions': tags
    };
    l.log("Metric " + new_stat['metric']);
    statDict['gauge'].push(new_stat);
    if (flush_counts) {
      var new_stat = {
        'metric': globalPrefix + '.' + namespace.concat('count').join("."),
        'value': value,
        'dimensions': tags
      };
      statDict['gauge'].push(new_stat);
    }

    numStats += 1;
  }

  for (key in timer_data) {
    if (key.indexOf('dev7') != -1) continue;
    var parsed_key = parse_key(key);
    var keyName = parsed_key['metric_name'];
    var tags = parsed_key['tags'];
    var namespace = timerNamespace.concat(keyName);
    var the_key = namespace.join(".");

    for (timer_data_key in timer_data[key]) {
      if (typeof(timer_data[key][timer_data_key]) === 'number') {
        var new_stat = {
          'metric': globalPrefix + '.' + the_key + '.' + timer_data_key,
          'value': timer_data[key][timer_data_key],
          'dimensions': tags
        };
        statDict['gauge'].push(new_stat);
      } else {
        for (var timer_data_sub_key in timer_data[key][timer_data_key]) {
          if (debug) {
            l.log(timer_data[key][timer_data_key][timer_data_sub_key].toString());
          }
          var new_stat = {
            'metric': globalPrefix + '.' + the_key + '.' + timer_data_key + '.' + timer_data_sub_key,
            'value': timer_data[key][timer_data_key][timer_data_sub_key],
            'dimensions': tags
          };
          statDict['gauge'].push(new_stat);
        }
      }
    }
    numStats += 1;
  }

  for (key in gauges) {
    if (key.indexOf('dev7') != -1) continue;
    l.log("Processing gauge");
    var parsed_key = parse_key(key);
    var keyName = parsed_key['metric_name'];
    var tags = parsed_key['tags'];
    var namespace = gaugesNamespace.concat(keyName);

    var new_stat = {
      'metric': globalPrefix + '.' + namespace.join("."),
      'value': gauges[key],
      'dimensions': tags
    };
    l.log("Gauge " + new_stat['metric']);
    statDict['gauge'].push(new_stat);
    numStats += 1;
  }

  for (key in sets) {
    if (key.indexOf('dev7') != -1) continue;
    var parsed_key = parse_key(key);
    var keyName = parsed_key['metric_name'];
    var tags = parsed_key['tags'];
    var namespace = setsNamespace.concat(keyName);

    var new_stat = {
      'metric': globalPrefix + '.' +namespace.join(".") + '.count',
      'value': sets[key].values().length,
      'dimensions': tags
    };
    statDict['gauge'].push(new_stat);
    numStats += 1;
  }

  post_stats(JSON.stringify(statDict));

  if (debug) {
   l.log("numStats: " + numStats);
  }
};

var backend_status = function signalfuse_status(writeCb) {
  writeCb(null, 'signalfuse', 'not_yet_implemented', 0);
};

exports.init = function signalfuse_init(startup_time, config, events, logger) {
  debug = config.debug;
  l = logger;
  signalfuseHost = config.signalfuseHost;
  signalfusePort = config.signalfusePort;
  signalfuseToken = config.signalfuseToken;

  // set defaults for prefixes
  globalPrefix  = "yelp_metrics";
  prefixCounter = "counters";
  prefixTimer   = "timers";
  prefixGauge   = "gauges";
  prefixSet     = "sets";
  prefixStats   = "statsd";

  // In order to unconditionally add this string, it either needs to be
  // a single space if it was unset, OR surrounded by a . and a space if
  // it was set.
  globalNamespace = ['stats'];
  counterNamespace = ['stats'];
  timerNamespace = ['stats', 'timers'];
  gaugesNamespace = ['stats', 'gauges'];
  setsNamespace = ['stats', 'sets'];

  flushInterval = config.flushInterval;

  flush_counts = typeof(config.flush_counts) === "undefined" ? true : config.flush_counts;

  events.on('flush', flush_stats);
  events.on('status', backend_status);

  return true;
};
