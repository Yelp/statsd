var sfx = require('../backends/signalfuse.js');

// generic configuration
function createConfig() {
  return {
    debug: true,
    flush_counts: true,
    signalfuse: {
      host: "sfxhost",
      token: "there are many tokens, but this one is mine",
      globalPrefix: "sfx_test"
    }
  };
}

// squash the output
function getLogger() {
  return { log: function(msg, level) { ; } }
  //return console;
}

function buildStat(name, value, tags) {
  return {
    metric: name,
    value: value,
    dimensions: tags
  }
}

// creates obj that we can use to sim the actual sending of metrics
function createEmitter() {
  var emitter = {
    on: function(key, listener) {
      // when a method is reg'd add it in so we can call it directly
      this.__proto__[key] = listener;
    }
  };
  return emitter;
}

function createFakeRequest() {
  var fakeRequest = {
    callToArgs: {},
    callToCount: {},
    logCall: function(methodname, args) {
      var storage = this.callToArgs[methodname] || [];
      storage.push(args);
      this.callToArgs[methodname] = storage;
      var callCount = this.callToCount[methodname] || 0;
      callCount += 1;
      this.callToCount[methodname] = callCount;
    },
    write: function(asString) {
      this.logCall('write', [asString]);
    },
    end: function() {
      this.logCall('end', []);
    }
  };

  return fakeRequest;
}

function checkYourself(test, actualMetricList, expectedMetricList) {
  var expectedMap = {};

  test.equal(actualMetricList.length, expectedMetricList.length);

  // create a map of the expected
  for (var ek = 0; ek < expectedMetricList.length; ek++) {
    var em = expectedMetricList[ek];
    expectedMap[em['metric']] = em;
  }

  for (var k = 0; k < actualMetricList.length; k++) {
    var m = actualMetricList[k];
    test.deepEqual(m, expectedMap[m['metric']]);
  }
}

function checkTwoMaps(test, actualMap, expectedMap) {
  test.deepEqual(typeof(actualMap), 'object', "actual map is not an object");
  test.deepEqual(typeof(expectedMap), 'object', "expected map is not an object");
  test.equal(Object.keys(actualMap).length,
             Object.keys(expectedMap).length);


  for(key in expectedMap) {
    if(expectedMap.hasOwnProperty(key)) {
      if(typeof(expectedMap[key]) === 'object') {
        checkTwoMaps(test, actualMap[key], expectedMap[key]);
      } else {
        test.equal(actualMap[key], expectedMap[key], "Mismatch on key: " + key);
      }
    }
  }
}

// ---------------------------------------------------------------------------
// TESTS
// ---------------------------------------------------------------------------
module.exports.testFlush = function(test) {
  var emitter = createEmitter();
  var m = {
    counters: {
      'test.counter': 3,
      'with.a=tag': 4
    },
    gauges: {
      'test.g': 5,
    },
    timers: {
      'test-timer': [10, 11, 12]
    },
    sets: {
      'tea-set': 43
    },
    counter_rates: {
      'test.rate': 239,
      'something.else': 234,
    },
    timer_data: {
      'sometimer': { 'math': 4, 'is': 3, 'fun': 23}
    },
    statsd_metrics: {
      'this.is.ignored': 4
    },
    pctThreshold: 42
  };

  var results = []
  var collector = function(metricsToSend) {
    Array.prototype.push.apply(results, metricsToSend);
  }

  var inst = sfx.init(0, createConfig(), emitter, getLogger());
  inst.post = collector; // override the actual write

  // use the emitter b/c it is what is *really* called
  emitter.flush(123, m);

  // 2 counters, 1 gauge, 3 timers, 1 set, 2 rates, 3 timerdata
  test.equal(results.length, 12);

  // only what we expect explicitly
  for(i = 0; i < results.length; i++) {
    var m = results[i];
    var val = m['value'];
    var dim = m['dimensions'];
    switch(m['metric']) {
      case 'sfx_test.test.counter':
        test.equal(val, 3);
        test.deepEqual(dim, {type:'counter'});
        break;
      case 'sfx_test.with':
        test.equal(val, 4);
        test.deepEqual(dim, {type:'counter', a:'tag'});
        break;
      case 'sfx_test.test.g':
        test.equal(val, 5);
        test.deepEqual(dim, {type:'gauge'});
        break;
      case 'sfx_test.tea-set':
        test.equal(val, 43);
        test.deepEqual(dim, {type:'set'});
        break;
      case 'sfx_test.test-timer':
        test.ok([10, 11, 12].indexOf(val) >= 0, "Timer value wrong: " + val);
        test.deepEqual(dim, {type:'timer'});
        break;
      case 'sfx_test.test.rate':
        test.equal(val, 239);
        test.deepEqual(dim, {type:'rate'});
        break;
      case 'sfx_test.something.else':
        test.equal(val, 234);
        test.deepEqual(dim, {type:'rate'});
        break;
      case 'sfx_test.sometimer.math':
        test.equal(val, 4);
        test.deepEqual(dim, {type:'timerdata'});
        break;
      case 'sfx_test.sometimer.is':
        test.equal(val, 3);
        test.deepEqual(dim, {type:'timerdata'});
        break;
      case 'sfx_test.sometimer.fun':
        test.equal(val, 23);
        test.deepEqual(dim, {type:'timerdata'});
        break;
      default:
        test.ok(false, "Unknown metric found: " + JSON.stringify(m));
    }
  }

  test.done();
}

module.exports.testKeyParsing = function(test) {
  var inst = sfx.init(0, createConfig(), createEmitter(), getLogger());

  var res = inst.parseKey('metricstart.tagN1=tagV1.othermetric part');
  test.equal(res['metricName'], 'metricstart.othermetric_part');
  test.deepEqual(res['tags'], {tagN1: 'tagV1'});

  // can't start with an '='
  res = inst.parseKey('start=with.equals');
  test.equal(res['metricName'], undefined);
  test.equal(res['tags'], undefined);


  // TODO more of these!!!

  test.done();
}

module.exports.testGenericTransformation = function(test) {
  var inst = sfx.init(0, createConfig(), createEmitter(), getLogger());

  var metrics = {
    'this.is.my.metric': 123,
    'this.is.my.rifle=gun': 453
  };

  // what we should get
  var expected = [];
  var result = [];

  expected = [
    buildStat("sfx_test.this.is.my.metric", 123, {type:'gauge'}),
    buildStat('sfx_test.this.is.my', 453, {type:'gauge', rifle:'gun'})
  ];
  result = inst.transformMetrics(metrics, 'gauge');
  checkYourself(test, result, expected);

  expected = [
    buildStat('sfx_test.this.is.my.metric', 123, {type:'set'}),
    buildStat('sfx_test.this.is.my', 453, {type:'set', rifle:'gun'})
  ];
  result = inst.transformMetrics(metrics, 'set');
  checkYourself(test, result, expected);

  expected = [
    buildStat('sfx_test.this.is.my.metric', 123, {type:'counter'}),
    buildStat('sfx_test.this.is.my', 453, {type:'counter', rifle:'gun'})
  ];
  result = inst.transformMetrics(metrics, 'counter');
  checkYourself(test, result, expected);

  expected = [
    buildStat('sfx_test.this.is.my.metric', 123, {type:'rate'}),
    buildStat('sfx_test.this.is.my', 453, {rifle:'gun', type:'rate'})
  ];
  result = inst.transformMetrics(metrics, 'rate');
  checkYourself(test, result, expected);

  test.done();
}

module.exports.testTimerTransformation = function(test) {

  var metrics = {
    'metrics.take.time': [4, 5, 6],
    'hearts.stars.and=rainbows': [7, 8, 9]
  };

  var inst = sfx.init(0, createConfig(), createEmitter(), getLogger());
  var results = inst.transformTimers(metrics);

  test.equal(results.length, 6);

  for(var i = 0; i < results.length; i++) {
    var metric = results[i];
    if(metric['metric'] === 'sfx_test.metrics.take.time') {
      if([4,5,6].indexOf(metric['value']) < 0) {
        test.ok(false, "Metric value isn't ok: " + JSON.stringify(metric));
      }

      test.deepEqual(metric['dimensions'], {type: 'timer'});
    } else if(metric['metric'] === 'sfx_test.hearts.stars') {
      if([7,8,9].indexOf(metric['value']) < 0) {
        test.ok(false, "Metric value isn't ok: " + JSON.stringify(metric));
      }

      test.deepEqual(metric['dimensions'], {type: 'timer', and:'rainbows'});
    } else {
      test.ok(false, "Found an unexpected metric " + JSON.stringify(metric));
    }
  }

  test.done();
}

module.exports.testTimerDataTransformation = function(test) {
  var metrics = {
    'metrics.are.fun': {
       count_90: 1,
       upper_90: 2,
       mean_90: 3,
       sum_90: 4,
       sum_squares_90: 5,
       std: 6,
       upper: 7,
       lower: 8,
       count: 9,
       count_ps: 10,
       sum: 11,
       sum_squares: 12,
       mean: 13,
       median: 14,
       something_new: 15
    }
  };

  var inst = sfx.init(0, createConfig(), createEmitter(), getLogger());
  var results = inst.transformTimerData(metrics);

  test.equal(results.length, 15);
  for(i = 0; i < results.length; i++){
    var m = results[i];
    switch(m['metric']) {
       case 'sfx_test.metrics.are.fun.count_90': test.equal(m['value'], 1); break;
       case 'sfx_test.metrics.are.fun.upper_90': test.equal(m['value'], 2); break;
       case 'sfx_test.metrics.are.fun.mean_90': test.equal(m['value'], 3); break;
       case 'sfx_test.metrics.are.fun.sum_90': test.equal(m['value'], 4); break;
       case 'sfx_test.metrics.are.fun.sum_squares_90': test.equal(m['value'], 5); break;
       case 'sfx_test.metrics.are.fun.std': test.equal(m['value'], 6); break;
       case 'sfx_test.metrics.are.fun.upper': test.equal(m['value'], 7); break;
       case 'sfx_test.metrics.are.fun.lower': test.equal(m['value'], 8); break;
       case 'sfx_test.metrics.are.fun.count': test.equal(m['value'], 9); break;
       case 'sfx_test.metrics.are.fun.count_ps': test.equal(m['value'], 10); break;
       case 'sfx_test.metrics.are.fun.sum': test.equal(m['value'], 11); break;
       case 'sfx_test.metrics.are.fun.sum_squares': test.equal(m['value'], 12); break;
       case 'sfx_test.metrics.are.fun.mean': test.equal(m['value'], 13); break;
       case 'sfx_test.metrics.are.fun.median': test.equal(m['value'], 14); break;
       case 'sfx_test.metrics.are.fun.something_new': test.equal(m['value'], 15); break;
       default: test.ok(false, "Unknown metric: " + JSON.stringify(m));
    }
  }

  test.done();
}

module.exports.testPost = function(test) {

  var expectedRequestsMade = 0;
  var postOptions = undefined;
  var postcb = undefined;
  var fakeRequest = createFakeRequest()

  var fakeHttp = {
    request: function(options, callback) {
      postOptions = options;
      postcb = callback;
      expectedRequestsMade += 1;
      return fakeRequest;
    },
  };

  var metricList = [
    {metric: 'a.metric.name1', value: 4, dimensions: {type: 'hands'}},
    {metric: 'a.metric.name2', value: 5, dimensions: {type: 'shoulders'}},
    {metric: 'a.metric.name3', value: 6, dimensions: {type: 'knees'}},
    {metric: 'a.metric.name4', value: 7, dimensions: {type: 'toes'}}
  ];
  var expectedPostData = {gauge: metricList};

  var config = createConfig();
  var inst = sfx.init(0, config, createEmitter(), getLogger());
  inst.getConfig().http = fakeHttp;
  inst.post(metricList, config.signalfuse);

  test.equal(expectedRequestsMade, 1,
            "Created the wrong number of requests: " + expectedRequestsMade);
  test.equal(fakeRequest.callToCount['end'], 1);
  test.equal(fakeRequest.callToCount['write'], 1);
  test.equal(fakeRequest.callToArgs['write'][0], JSON.stringify(expectedPostData));

  checkTwoMaps(test, postOptions, {
    host: 'sfxhost',
    method: 'POST',
    path: '/v2/datapoint',
    headers: {
      'Content-Type': 'application/json',
      'X-SF-Token': 'there are many tokens, but this one is mine'
    }
  });

  test.done();
}

module.exports.testEndToEnd = function(test) {
  var testport = 123;

  var input = {
    counters: {'metrics': 24},
    sets: {'are.rando=tag':234},
    timers: {'fun':[222]},
    counter_rates: {'yaaaay': 87},
    gauges: {},
    timer_data: {},
    statsd_metrics: {},
    pctThreshold: 42
  };

  var expectedPostData = {gauge:
    [
      {metric: 'sfx_test.metrics', value: 24, dimensions: {type: 'counter'}},
      {metric: 'sfx_test.are', value: 234, dimensions: {type: 'set', rando: 'tag'}},
      {metric: 'sfx_test.fun', value: 222, dimensions: {type: 'timer'}},
      {metric: 'sfx_test.yaaaay', value:87, dimensions: {type: 'rate'}},
    ]
  };

  var emitter = createEmitter();
  var config = createConfig();
  var fakeRequest = createFakeRequest();
  var fakeHttp = {
    request: function(options, handler) {
      return fakeRequest;
    }
  };

  var inst = sfx.init(0, config, emitter, getLogger());
  inst.getConfig().http = fakeHttp;
  emitter['flush'](123, input);

  test.equal(fakeRequest.callToCount['end'], 1);
  test.equal(fakeRequest.callToCount['write'], 1);
//  test.equal(fakeRequest.callToArgs['write'][0], JSON.stringify(expectedPostData));

  test.done();
}

module.exports.testGlobalPrefix = function(test) {
  var config = createConfig();
  config.signalfuse.globalPrefix = "something";

  var newconfig = sfx.init(0, config, createEmitter(), getLogger()).getConfig();
  test.equal(newconfig.globalPrefix, 'something.');

  config.signalfuse.globalPrefix = "";
  newconfig = sfx.init(0, config, createEmitter(), getLogger()).getConfig();
  test.equal(newconfig.globalPrefix, '', 'Got "' + newconfig.globalPrefix + '" expected:' + "''");

  config.signalfuse.globalPrefix = undefined;
  newconfig = sfx.init(0, config, createEmitter(), getLogger()).getConfig();
  test.equal(newconfig.globalPrefix, '', 'Got "' + newconfig.globalPrefix + '" expected:' + "''");
  test.done();
}
