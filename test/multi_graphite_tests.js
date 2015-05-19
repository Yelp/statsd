var multid = require('../backends/multidimensional_graphite.js');

function createConfig() {
  return {
    debug: true,
    flush_counts: true,
  };
}

// squash the output
function getLogger() {
  return {
    log: function(msg, level) { ; },
    info: function(msg, level) { ; },
    error: function(msg, level) { ; },
  }
  //return console;
}

// creates obj that we can use to sim the actual sending of metrics
function createEmitter() {
  var emitter = {
    on: function(key, listener) {
      // when a method is reg'd add it in so we can call it directly
      this[key] = listener;
    }
  };
  return emitter;
}

function compareObjs(test, actual, expected) {
  test.ok(actual !== undefined, "Actual is undefined");
  test.ok(expected !== undefined, "expected is undefined");

  var actualKeys = 0;
  for(aKey in actual) {
    if(actual.hasOwnProperty(aKey)) {
      actualKeys += 1;
      test.ok(expected[aKey] !== undefined, "Expected doesn't expect key " + aKey);
      test.deepEqual(actual[aKey], expected[aKey], "difference at key " + aKey);
    }
  }

  var expectedKeys = 0;
  for(eKey in expected){
    if(expected.hasOwnProperty(eKey)) {
      expectedKeys += 1;
    }
  }

  test.equal(actualKeys, expectedKeys, "Mismatch in the number of keys");
};

// ---------------------------------------------------------------------------
// TESTS
// ---------------------------------------------------------------------------
module.exports.testCreation = function(test) {
  var inst = multid.init(0, createConfig(), createEmitter(), getLogger());
  test.ok(inst !== undefined, 'Failed to create a Multidimensional Graphite instance');
  test.done();
};

module.exports.testKeyTransformation = function(test) {
  proto = multid.multid_prototype;

  var testVal = 'this.isnt.json';
  var actual = proto.transformKey(testVal);
  test.equal(actual, 'this.isnt.json');

  // doesn't have the right parts
  testVal = '[["graphite_keys", "host;port"]]';
  actual = proto.transformKey(testVal);
  test.equal(actual, undefined);

  // missing metric_name
  testVal =
    '[ \
    ["graphite_keys", "host;port"], \
    ["host", "somehost"], \
    ["port", "someport"] \
    ]';
  actual = proto.transformKey(testVal);
  test.equal(actual, undefined);

  // all parts present
  testVal =
    '[ \
    ["graphite_keys", "host;port"], \
    ["host", "somehost"], \
    ["port", "someport"], \
    ["metric_name", "test_metric"] \
    ]';
  actual = proto.transformKey(testVal);
  test.equal(actual, 'test_metric.host.somehost.port.someport');

  test.done();
};

module.exports.testTransformation = function(test) {
  var inMetrics = {
    counters: {
      'statsd.bad_lines_seen': 1,
      '[["graphite_keys", "host;port;package"], \
        ["metric_name", "test_metric"], \
        ["host", "myhost"], \
        ["package", "somepackage"], \
        ["port", "23"]\
      ]': 200
    },
    timers: {
      '[["graphite_keys", "host;port;package"], \
        ["metric_name", "test_metric"], \
        ["host", "timerhost"], \
        ["package", "timerpackage"], \
        ["port", "342"]\
      ]': [2, 3, 4]
    },
    gauges: {
      '[["graphite_keys", "host;port;package"], \
        ["metric_name", "test_metric"], \
        ["host", "gaugehost"], \
        ["package", "gaugepackage"], \
        ["port", "546"]\
      ]': 222
    },
    sets: {
      '[["graphite_keys", "host;port;package"], \
        ["metric_name", "test_metric"], \
        ["host", "sethost"], \
        ["package", "setpackage"], \
        ["port", "546"]\
      ]': 222
    },
    timer_data: {
      '[["graphite_keys", "host;port;package"], \
        ["metric_name", "test_metric"], \
        ["host", "timerhost"], \
        ["package", "timerpackage"], \
        ["port", "342"]\
      ]': {
        count_90: 1,
        mean_90: 2,
        upper_90: 3,
        sum_90: 4
      }
    },
    counter_rates: {
      'statsd.bad_lines_seen': 1,
      '[["graphite_keys", "host;port;package"], \
        ["metric_name", "test_metric"], \
        ["host", "myhost"], \
        ["package", "somepackage"], \
        ["port", "23"]\
      ]': 0.1
    },
    pctThreshold: [90]
  };

  var expected= {
    counters: {
      'statsd.bad_lines_seen': 1,
      'test_metric.host.myhost.port.23.package.somepackage': 200
    },
    timers: {
      'test_metric.host.timerhost.port.342.package.timerpackage': [2, 3, 4]
    },
    gauges: {
      'test_metric.host.gaugehost.port.546.package.gaugepackage': 222
    },
    sets: {
      'test_metric.host.sethost.port.546.package.setpackage': 222
    },
    timer_data: {
      'test_metric.host.timerhost.port.342.package.timerpackage': {
        count_90: 1,
        mean_90: 2,
        upper_90: 3,
        sum_90: 4
      }
    },
    counter_rates: {
      'statsd.bad_lines_seen': 1,
      'test_metric.host.myhost.port.23.package.somepackage': 0.1
    },
    pctThreshold: [90]
  };

  proto = multid.multid_prototype;
  var outMetrics = proto.transformMetrics(inMetrics);
  compareObjs(test, outMetrics['counters'], expected['counters']);
  compareObjs(test, outMetrics['gauges'], expected['gauges']);
  compareObjs(test, outMetrics['sets'], expected['sets']);
  compareObjs(test, outMetrics['counter_rates'], expected['counter_rates']);
  compareObjs(test, outMetrics['pctThreshold'], expected['pctThreshold']);
  compareObjs(test, outMetrics['timers'], expected['timers']);
  compareObjs(test, outMetrics['timer_data'], expected['timer_data']);

  test.done();
}

module.exports.testMissingKeys = function(test) { test.done(); }
