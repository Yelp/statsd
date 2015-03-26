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

function buildStat(name, value, tags) {
  return {
    metric: name,
    value: value,
    dimensions: tags
  }
}

// for use squashing output
var devNullLogger = {
  log: function(msg, level) { ; }
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

//convienence values
var metricKeyWithTags = "this.is.my.rifle=gun";
var metricKey = "this.is.my.metric";

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

module.exports.testNamespaceMunging = function(test) {

  var inst = sfx.init(0, buildConfig(), createEmitter(), console);
  var sfxConfig = inst.getConfig();

  test.notEqual(sfxConfig, {});
  test.notEqual(sfxConfig.namespaces, {});
  test.deepEqual(sfxConfig.namespaces.counter, ['sfx_test', 'counter']);
  test.deepEqual(sfxConfig.namespaces.timer, ['sfx_test', 'timer']);
  test.deepEqual(sfxConfig.namespaces.gauge, ['sfx_test', 'gauge']);
  test.deepEqual(sfxConfig.namespaces.set, ['sfx_test', 'set']);

  // test that if we do set something we keep it
  var updatedConfig = {
    signalfuse: {
      namespaces: {
        counter: ['something different'],
        timer: ['diff timer', 'anotherlayer']
      }
    }
  };

  inst = sfx.init(0, updatedConfig, createEmitter(), console);
  sfxConfig = inst.getConfig();
  // these should be pop'd w/only the type - we didn't put a globalPrefix
  test.deepEqual(sfxConfig.namespaces.gauge, ['gauge']);
  test.deepEqual(sfxConfig.namespaces.set, ['set']);
  // these should match us
  test.deepEqual(sfxConfig.namespaces.counter, ['something different']);
  test.deepEqual(sfxConfig.namespaces.timer, ['diff timer', 'anotherlayer']);

  // test that we always prepend the globalPrefix if it is there
  var withGlobalPrefix = {
    signalfuse: {
      globalPrefix: "globalprefix",
      namespaces: {
        counter: ['notthewordcounter'],
        timer: ['timer', 'special']
      }
    }
  };

  inst = sfx.init(0, withGlobalPrefix, createEmitter(), console);
  sfxConfig = inst.getConfig();
  // auto w/global prefix
  test.deepEqual(sfxConfig.namespaces.gauge, ['globalprefix', 'gauge']);
  test.deepEqual(sfxConfig.namespaces.set, ['globalprefix', 'set']);
  test.deepEqual(sfxConfig.namespaces.counter, ['globalprefix', 'notthewordcounter']);
  test.deepEqual(sfxConfig.namespaces.timer, ['globalprefix', 'timer', 'special']);

  test.done();
}

module.exports.testKeyParsing = function(test) {
  var inst = sfx.init(0, buildConfig(), createEmitter(), console);


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

module.exports.testCounterTransformation = function(test) {
  var inst = sfx.init(0, buildConfig(), createEmitter(), console);
  var counters= {};
  var counterRates= {};

  counters[metricKey] = 123;
  counterRates[metricKey] = 32;

  counters[metricKeyWithTags] = 453;
  counterRates[metricKeyWithTags] = 23;


  // what we should get
  var expected = [];
  expected.push(buildStat('sfx_test.counter.this.is.my.metric.rate', 32, {}));
  expected.push(buildStat("sfx_test.counter.this.is.my.metric.count", 123, {}));
  expected.push(buildStat('sfx_test.counter.this.is.my.count', 453, {rifle:'gun'}));
  expected.push(buildStat('sfx_test.counter.this.is.my.rate', 23, {rifle:'gun'}));

  var result = inst.transformCounters(counters, counterRates);

  checkYourself(test, result, expected);

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
