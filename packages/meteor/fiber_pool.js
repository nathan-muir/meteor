var Fiber = Npm.require('fibers');
var assert = Npm.require('assert');

// Just in case someone tampers with Fiber.yield, don't let that interfere
// with our processing of the callback queue.
var originalYield = Fiber.yield;


function cloneFiberOwnProperties(fiber) {
  if (fiber) {
    var dynamics = {};

    Object.keys(fiber).forEach(function (key) {
      dynamics[key] = shallowClone(fiber[key]);
    });

    return dynamics;
  }
}

function shallowClone(value) {
  if (Array.isArray(value)) {
    return value.slice(0);
  }

  if (value && typeof value === "object") {
    var copy = Object.create(Object.getPrototypeOf(value));
    var keys = Object.keys(value);
    var keyCount = keys.length;

    for (var i = 0; i < keyCount; ++i) {
      var key = keys[i];
      copy[key] = value[key];
    }

    return copy;
  }

  return value;
}

function withoutInvocation(f) {
  if (Package.ddp) {
    var DDP = Package.ddp.DDP;
    var CurrentInvocation =
      DDP._CurrentMethodInvocation ||
      // For backwards compatibility, as explained in this issue:
      // https://github.com/meteor/meteor/issues/8947
      DDP._CurrentInvocation;

    var invocation = CurrentInvocation.get();
    if (invocation && invocation.isSimulation) {
      throw new Error("Can't set timers inside simulations");
    }

    return function () {
      CurrentInvocation.withValue(null, f);
    };
  } else {
    return f;
  }
}

function FiberPool(targetFiberCount) {
  assert.ok(this instanceof FiberPool);
  assert.strictEqual(typeof targetFiberCount, "number");

  var fiberStack = [];

  function makeNewFiber() {
    var fiber = new Fiber(function () {
      while (true) {
        // Call Fiber.yield() to await further instructions.
        var entry = originalYield.call(Fiber);

        if (!(entry &&
          typeof entry.callback === "function" &&
          typeof entry.resolve === "function" &&
          typeof entry.reject === "function")) {
          // If someone retained a reference to this Fiber long enough to
          // call fiber.run(value) with a value that doesn't look like an
          // entry object, return immediately to the top of the loop to
          // continue waiting for the next entry object.
          continue;
        }

        // Ensure this Fiber is no longer in the pool once it begins to
        // execute an entry.
        assert.strictEqual(fiberStack.indexOf(fiber), -1);

        if (entry.dynamics) {
          // Restore the dynamic environment of this fiber as if
          // entry.callback had been wrapped by Meteor.bindEnvironment.
          Object.keys(entry.dynamics).forEach(function (key) {
            fiber[key] = entry.dynamics[key];
          });
        }

        try {
          entry.resolve(entry.callback.apply(
            entry.context || null,
            entry.args || []
          ));
        } catch (error) {
          entry.reject(error);
        }

        // Remove all own properties of the fiber before returning it to
        // the pool.
        Object.keys(fiber).forEach(function (key) {
          delete fiber[key];
        });

        if (fiberStack.length < targetFiberCount) {
          fiberStack.push(fiber);
        } else {
          console.log('destroying fiber');
          // If the pool has already reached the target maximum number of
          // Fibers, don't bother recycling this Fiber.
          break;
        }
      }
    });

    // Run the new Fiber up to the first yield point, so that it will be
    // ready to receive entries.
    fiber.run();

    return fiber;
  }

  // Run the entry.callback function in a Fiber either taken from the pool
  // or created anew if the pool is empty.
  function runEntry(entry) {
    assert.strictEqual(typeof entry, "object");
    assert.strictEqual(typeof entry.callback, "function");
    assert.strictEqual(typeof entry.resolve, "function");
    assert.strictEqual(typeof entry.reject, "function");

    var fiber = fiberStack.pop() || makeNewFiber();
    fiber.run(entry);
  }

  function noop(){}

  this.run = function(func){
    runEntry({
      callback: func,
      resolve: noop,
      reject: noop,
    })
  };

  this.bindEnvironment = function (func, onException, _this) {
    Meteor._nodeCodeMustBeInFiber();

    if (!onException || typeof(onException) === 'string') {
      var description = onException || "callback of async function";
      onException = function (error) {
        Meteor._debug(
          "Exception in " + description + ":",
          error
        );
      };
    } else if (typeof(onException) !== 'function') {
      throw new Error('onException argument must be a function, string or undefined for Meteor.bindEnvironment().');
    }
    return function (/* arguments */) {
      var args = Array.prototype.slice.call(arguments);
      runEntry({
        callback: func,
        context: _this,
        args: args,
        reject: onException,
        dynamics: cloneFiberOwnProperties(Fiber.current)
      })
    }
  };

  this._bindAndCatch = function(context, f){
    return this.bindEnvironment(withoutInvocation(f), context);
  };

  this.setTimeout = function (f, duration) {
    return setTimeout(this._bindAndCatch("setTimeout callback", f), duration);
  };

  this.setInterval = function (f, duration) {
    return setInterval(this._bindAndCatch("setInterval callback", f), duration);
  };

  this.defer = function (f) {
    Meteor._setImmediate(this._bindAndCatch("defer callback", f));
  };

  // Limit the maximum number of idle Fibers that may be kept in the
  // pool. Note that the run method will never refuse to create a new
  // Fiber if the pool is empty; it's just that excess Fibers might be
  // thrown away upon completion, if the pool is full.
  this.setTargetFiberCount = function (limit) {
    assert.strictEqual(typeof limit, "number");

    targetFiberCount = Math.max(limit, 0);

    if (targetFiberCount < fiberStack.length) {
      // If the requested target count is less than the current length of
      // the stack, truncate the stack and terminate any surplus Fibers.
      fiberStack.splice(targetFiberCount).forEach(function (fiber) {
        fiber.reset();
      });
    }

    return this;
  };
}

// Call pool.drain() to terminate all Fibers waiting in the pool and
// signal to any outstanding Fibers that they should exit upon completion,
// instead of reinserting themselves into the pool.
FiberPool.prototype.drain = function () {
  return this.setTargetFiberCount(0);
};

Meteor._makeFiberPool = function (targetFiberCount) {
  return new FiberPool(targetFiberCount || 20);
};

