var sfx = require('../backends/sfx_replace.js');

// generic configuration
function buildConfig() {
  return {
    debug: true,
    flush_counts: true,
    signalfuse: {
      host: "sfxhost",
      port: -123,
      token: "sfxtoken",
      globalPrefix: "sfx_test"
    }
  };
}

// helper method to make a shell in the right format
function genericMetric() {
  return {
    counters: {},
    guages: {},
    timers: {},
    sets: {},
    counter_rates: {},
    timer_data: {},
    statsd_metrics: {},
    pctThreshold: 0
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

// ---------------------------------------------------------------------------
// TESTS
// ---------------------------------------------------------------------------
//module.exports.testTest = function(test) {
//  var emitter = createEmitter();
//  var m = genericMetric();
//  m['counters'][metricKey] = counterVal;
//
//  var validateFcn = function(dict) {
//
//  }
//
//  var inst = sfx.init(0, config, emitter, console);
//  inst.post = validateFcn; // override the actual write
//
//  // use the emitter b/c it is what is *really* called
//  emitter.flush(123, m);
//
//  test.done();
//}

module.exports.testKeyParsing = function(test) {
  var inst = sfx.init(0, buildConfig(), createEmitter(), getLogger());

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
  var inst = sfx.init(0, buildConfig(), createEmitter(), getLogger());

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

  var inst = sfx.init(0, buildConfig(), createEmitter(), getLogger());
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

  var inst = sfx.init(0, buildConfig(), createEmitter(), getLogger());
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
