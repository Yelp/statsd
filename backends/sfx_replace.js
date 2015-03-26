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
 *      globalPrefix: "",
 *      namespaces: {
 *        counter: "",
 *        timer: "",
 *        gauge: "",
 *        set: ""
 *      }
 *    }
 *  }
 *
 *  namespaces: they are an *optional* config value. They will automatically
 *  be put into their buckets if not specified (e.g. timer for timer, counter for counter...).
 *  for instance::
 *
 *  {
 *    signalfuse: {
 *      globalPrefix: "gp",
 *      namespaces: {
 *        counter: ["not_the_word_counter"]
 *      }
 *    }
 *  }
 *
 *  would result in namespaces:
 *
 *    - gp.timer, gp.set, gp.gauge gp.not_the_word_counter
 *
 */

var http = require('https');

var l; // the logger
var debug;

// ---------------------------------------------------------------------------
// HELPER functions
// ---------------------------------------------------------------------------
function namespaceMunge(namespaceContainer, nameToCheck, global) {
  if (namespaceContainer[nameToCheck] === undefined) {
    namespaceContainer[nameToCheck] = global.concat(nameToCheck);
  } else {
    // we have a name, but we need to munge it up with the global
    var ourVal = namespaceContainer[nameToCheck];
    namespaceContainer[nameToCheck] = global.concat(ourVal);
  }
}

function buildStat(namespace, value, tags) {
  return {
    metric: namespace.join('.'),
    value: value,
    dimensions: tags
  }
}

// ---------------------------------------------------------------------------
// The actual BACKEND
// ---------------------------------------------------------------------------
function SignalFuseBackend(startup_time, config, emitter) {
  var c = config.signalfuse || {};

  var globalNamespace = c.globalPrefix ? [c.globalPrefix] : [];
  // handle some namespacing
  c.namespaces = c.namespaces || {};
  namespaceMunge(c.namespaces, 'counter', globalNamespace);
  namespaceMunge(c.namespaces, 'timer', globalNamespace);
  namespaceMunge(c.namespaces, 'gauge', globalNamespace);
  namespaceMunge(c.namespaces, 'set', globalNamespace);
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
    if (parts[i].indexOf('=') >= 0) {
      var tagParts = parts[i].split('=');
      tags[tagParts[0]] = tagParts[1];
    } else {
      metricParts.push(parts[i]);
    }
  }

  metricName = metricParts.join('.');

  l.debug("Parsed " + rawkey + " into " + metricName +
          " and tags: " + JSON.stringify(tags));

  return {metricName: metricName, tags: tags};
}

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


//
// This method is intended to handle the counters from a metric. Transforming them
// into the signalfuse versions after parsing the keys & tags
//
SignalFuseBackend.prototype.transformCounters = function(counters, counterRates) {
  l.debug('Starting to process ' + Object.keys(counters).length + ' counters');

  var counterNamespace = this.sfxConfig.namespaces.counter;
  var resultingStats = [];

  for (var rawKey in counters) {
    if (counters.hasOwnProperty(rawKey)) {
      l.debug('Processing raw counter key: ' + rawKey);
      var value = counters[rawKey];
      var valuePerSecond = counterRates[rawKey];

      var metricParts = this.parseKey(rawKey);
      var keyName = metricParts['metricName'];
      var tags = metricParts['tags'];

      var namespace = counterNamespace.concat(keyName);

      resultingStats.push(buildStat(namespace.concat('rate'), valuePerSecond, tags));
      if (this.config.flush_counts) {
        resultingStats.push(buildStat(namespace.concat('count'), value, tags));
      }
    }
  }

  l.debug("Finished transforming " + counters.length + " counters into " +
          resultingStats.length + " signalfuse metrics");

  return resultingStats;
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

