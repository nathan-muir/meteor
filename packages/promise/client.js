const Promise = globalThis.Promise;
const es6PromiseThen = Promise.prototype.then;

function raise(exception) {
  throw exception;
}

Promise.prototype.then = function (onResolved, onRejected) {
  return es6PromiseThen.call(
    this,
    onResolved && Meteor.bindEnvironment(onResolved, raise),
    onRejected && Meteor.bindEnvironment(onRejected, raise)
  );
};

export { Promise };
