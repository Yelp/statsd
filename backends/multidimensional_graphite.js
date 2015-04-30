var util = require('util')
  , events = require('events')
  , createBackendLogger = require('../lib/logging_utils')
  , logger = require('../lib/logger')

var log = console; // the logger
var debug;


function MultiDimensionalGraphite(startupTime, config, serverEmitter) {
  log.log('Starting up Multidimensional Graphite instance at ' + startupTime);

  this.config = config;
  this.innerEmitter = new events.EventEmitter();
  this.actualGraphite = require('./graphite');
  this.actualGraphite.init(startupTime, config, this.innerEmitter, log);
  var self = this;

  serverEmitter.on('flush', function(ts, metrics) {
    log.debug('Transforming \n' + util.inspect(metrics, {depth:5, colors: true}));
    var transdmetrics = self.transformMetrics(metrics);
    log.debug('Finished transforming result is \n' + util.inspect(transdmetrics, {depth:5, colors: true}));

    self.innerEmitter.emit('flush', ts, transdmetrics);
  });
};

/*
 * This is what actually transforms the name. It converts the string to JSON,
 * then pulls the 'graphite_keys' and for each part it gets the value and
 * then builds the final graphite key as: ``key=value.key2=value2``
 *
 * Some errors resulting in the original name being returned
 *  - not JSON
 *  - doesn't have the 'graphite_keys' key
 *
 * Errors which are *really* errors:
 *  - if the key from 'graphite_keys' DNE
 */
MultiDimensionalGraphite.prototype.transformKey = function(name) {
  var parsed = undefined;
  try {
    parsed = JSON.parse(name);
  } catch(err) {
    return name;
  }
  var mapVersion = {};
  parsed.forEach(function(item) {
    mapVersion[item[0]] = item[1];
  });

  var gKeys = mapVersion['graphite_keys'];
  if(gKeys === undefined) {
    return name;
  }

  finalKeys = [];

  gKeys.split(';').forEach(function(part) {
    var val = mapVersion[part];
    if(val === undefined) {
      log.error('Poorly formed Key detected, original key: "' + name +
        '", \ngraphite_keys: "' + gKeys +
        '", \nmissing value at key: "' + part + '"');
      return;
    } else {
      finalKeys.push([part, val].join('='));
    }
  });

  if(finalKeys.length == 0) {
    log.error("Failed to transform the name '" + name + "'");
    return undefined;
  }

  return finalKeys.join('.');
};

MultiDimensionalGraphite.prototype.transformMetrics = function(metrics) {
  var transMetrics = {};
  var transKey = this.transformKey;
  var transObj = function(obj) {
    var finalObj = {};
    for(var k in obj) {
      if(obj.hasOwnProperty(k)) {
        var newKey = transKey(k);
        finalObj[newKey] = obj[k];
      }
    }
    return finalObj;
  };

  transMetrics['counters'] = transObj(metrics['counters']);
  transMetrics['gauges'] = transObj(metrics['gauges']);
  transMetrics['sets'] = transObj(metrics['sets']);
  transMetrics['timers'] = transObj(metrics['timers']);
  transMetrics['timer_data'] = transObj(metrics['timer_data']);
  transMetrics['counter_rates'] = transObj(metrics['counter_rates']);
  transMetrics['pctThreshold'] = metrics['pctThreshold'];

  return transMetrics;
};

exports.multid_prototype = MultiDimensionalGraphite.prototype;

exports.init = function graphite_init(startup_time, config, events, logger) {

  log = createBackendLogger('multidimensions-graphite', config.debug, logger);

  return new MultiDimensionalGraphite(startup_time, config, events);
};
