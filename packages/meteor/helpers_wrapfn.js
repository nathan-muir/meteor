export const Meteor$wrapFn = function (fn) {
  if (!fn || typeof fn !== 'function') {
    throw new Error("Expected to receive function to wrap");
  }

  if (Meteor.isClient) {
    return fn;
  }

  return function() {
    var ret = fn.apply(this, arguments);
    if (ret && typeof ret.then === 'function') {
      return Promise.await(ret);
    }

    return ret;
  }
};
