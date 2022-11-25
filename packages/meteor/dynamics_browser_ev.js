// Simple implementation of dynamic scoping, for use in browsers

var nextSlot = 0;
var currentValues = [];

export function EnvironmentVariable() {
  this.slot = nextSlot++;
}

var EVp = EnvironmentVariable.prototype;

EVp.get = function () {
  return currentValues[this.slot];
};

EVp.getOrNullIfOutsideFiber = function () {
  return this.get();
};

EVp.withValue = function (value, func) {
  var saved = currentValues[this.slot];
  try {
    currentValues[this.slot] = value;
    var ret = func();
  } finally {
    currentValues[this.slot] = saved;
  }
  return ret;
};

export function getEnvironment() {
  return currentValues;
}
export function setEnvironment(newValues) {
  currentValues = newValues;
}
