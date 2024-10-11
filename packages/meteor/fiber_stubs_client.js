import { Meteor$bindEnvironment } from "./dynamics_browser.js";
// This file is a partial analogue to fiber_helpers.js, which allows the client
// to use a queue too, and also to call noYieldsAllowed.

// The client has no ability to yield, so noYieldsAllowed is a noop.
//
export const Meteor$_noYieldsAllowed = function (f) {
  return f();
};

// An even simpler queue of tasks than the fiber-enabled one.  This one just
// runs all the tasks when you call runTask or flush, synchronously.
//
export const Meteor$_SynchronousQueue = function () {
  var self = this;
  self._tasks = [];
  self._running = false;
  self._runTimeout = null;
};

var SQp = Meteor$_SynchronousQueue.prototype;

SQp.runTask = function (task) {
  var self = this;
  if (!self.safeToRunTask())
    throw new Error("Could not synchronously run a task from a running task");
  self._tasks.push(task);
  var tasks = self._tasks;
  self._tasks = [];
  self._running = true;

  if (self._runTimeout) {
    // Since we're going to drain the queue, we can forget about the timeout
    // which tries to run it.  (But if one of our tasks queues something else,
    // the timeout will be correctly re-created.)
    clearTimeout(self._runTimeout);
    self._runTimeout = null;
  }

  try {
    while (tasks.length > 0) {
      var t = tasks.shift();
      try {
        t();
      } catch (e) {
        if (tasks.length === 0) {
          // this was the last task, that is, the one we're calling runTask
          // for.
          throw e;
        }
        Meteor._debug("Exception in queued task", e);
      }
    }
  } finally {
    self._running = false;
  }
};

// on the server, `queueTask` is run in an unbound fiber
//  this means it runs without environment variables set
// on the client, it's incorrectly implemented to run under the current
//  environment.
// this causes an issue when LocalCollection._observeQueue.drain() is called
//  during a method call, causing all handlers, and promises launched in those
//  handlers to be executed in the environment of the simulation.
const withoutEnvironment = Meteor$bindEnvironment(function (task) {
  return task();
})
SQp.queueTask = function (task) {
  var self = this;
  self._tasks.push(function() {
    withoutEnvironment(task)
  });
  // Intentionally not using Meteor.setTimeout, because it doesn't like runing
  // in stubs for now.
  if (!self._runTimeout) {
    self._runTimeout = setTimeout(function () {
      return self.flush.apply(self, arguments);
    }, 0);
  }
};

SQp.flush = function () {
  var self = this;
  self.runTask(function () {});
};

SQp.drain = function () {
  var self = this;
  if (!self.safeToRunTask()) {
    return;
  }
  while (self._tasks.length > 0) {
    self.flush();
  }
};

SQp.safeToRunTask = function () {
  var self = this;
  return !self._running;
};
