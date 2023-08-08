import { nextTick, shallowRef, effect, watchEffect, stop, computed } from "@vue/runtime-dom";
import { pauseTracking, resetTracking, effectScope, getCurrentScope, onScopeDispose } from "@vue/reactivity";

/////////////////////////////////////////////////////
// Package docs at http://docs.meteor.com/#tracker //
/////////////////////////////////////////////////////

/**
 * @namespace Tracker
 * @summary The namespace for Tracker-related methods.
 */
export const Tracker = {};

/**
 * @namespace Deps
 * @deprecated
 */
export const Deps = Tracker;

// http://docs.meteor.com/#tracker_active

/**
 * @summary True if there is a current computation, meaning that dependencies on reactive data sources will be tracked and potentially cause the current computation to be rerun.
 * @locus Client
 * @type {Boolean}
 */
Tracker.active = false;

// http://docs.meteor.com/#tracker_currentcomputation

/**
 * @summary The current computation, or `null` if there isn't one.  The current computation is the [`Tracker.Computation`](#tracker_computation) object created by the innermost active call to `Tracker.autorun`, and it's the computation that gains dependencies when reactive data sources are accessed.
 * @locus Client
 * @type {Tracker.Computation}
 */
Tracker.currentComputation = null;

function setCurrentComputation(c) {
  Tracker.currentComputation = c;
  Tracker.active = !! c;
}


var nextId = 1;
// `true` if we are computing a computation now, either first time
// or recompute.  This matches Tracker.active unless we are inside
// Tracker.nonreactive, which nullfies currentComputation even though
// an enclosing computation may still be running.
var inCompute = false;

//
// http://docs.meteor.com/#tracker_computation

/**
 * @summary A Computation object represents code that is repeatedly rerun
 * in response to
 * reactive data changes. Computations don't have return values; they just
 * perform actions, such as rerendering a template on the screen. Computations
 * are created using Tracker.autorun. Use stop to prevent further rerunning of a
 * computation.
 * @instancename computation
 */
Tracker.Computation = class Computation {
  constructor() {
    // http://docs.meteor.com/#computation_stopped

    /**
     * @summary True if this computation has been stopped.
     * @locus Client
     * @memberOf Tracker.Computation
     * @instance
     * @name  stopped
     */
    this.stopped = false;

    // http://docs.meteor.com/#computation_firstrun

    /**
     * @summary True during the initial run of the computation at the time `Tracker.autorun` is called, and false on subsequent reruns and at other times.
     * @locus Client
     * @memberOf Tracker.Computation
     * @instance
     * @name  firstRun
     * @type {Boolean}
     */
    this.firstRun = true;

    this._id = nextId++;
    this._onInvalidateCallbacks = [];
    this._onStopCallbacks = [];
  }

  get invalidated() {
    console.warn('Computation.invalidated is not supported');
    return false;
  }

  // http://docs.meteor.com/#computation_oninvalidate

  /**
   * @summary Registers `callback` to run when this computation is next invalidated, or runs it immediately if the computation is already invalidated.  The callback is run exactly once and not upon future invalidations unless `onInvalidate` is called again after the computation becomes valid again.
   * @locus Client
   * @param {Function} callback Function to be called on invalidation. Receives one argument, the computation that was invalidated.
   */
  onInvalidate(f) {
    if (typeof f !== 'function')
      throw new Error("onInvalidate requires a function");

    if (this.stopped) {
      Tracker.nonreactive(() => {
        f(this);
      });
    } else {
      this._onInvalidateCallbacks.push(f);
    }
  }

  /**
   * @summary Registers `callback` to run when this computation is stopped, or runs it immediately if the computation is already stopped.  The callback is run after any `onInvalidate` callbacks.
   * @locus Client
   * @param {Function} callback Function to be called on stop. Receives one argument, the computation that was stopped.
   */
  onStop(f) {
    if (typeof f !== 'function')
      throw new Error("onStop requires a function");

    if (this.stopped) {
      Tracker.nonreactive(() => {
        f(this);
      });
    } else {
      this._onStopCallbacks.push(f);
    }
  }

  // http://docs.meteor.com/#computation_invalidate

  invalidate() {
    console.warn('Computation.invalidate() is not supported')
  }
  /**
   * @summary Invalidates this computation so that it will be rerun.
   * @locus Client
   */
  _invalidate() {
    for (var i = 0, f; f = this._onInvalidateCallbacks[i]; i++) {
      Tracker.nonreactive(() => {
        f(this);
      });
    }
    this._onInvalidateCallbacks = [];
  }

  // http://docs.meteor.com/#computation_stop

  /**
   * @summary Prevents this computation from rerunning.
   * @locus Client
   */
  stop() {
    this._invalidate();
    if (!this.stopped) {
      this.stopped = true;
      for (var i = 0, f; f = this._onStopCallbacks[i]; i++) {
        Tracker.nonreactive(() => {
          f(this);
        });
      }
      this._onStopCallbacks = [];
    }
  }

  /**
   * @summary Process the reactive updates for this computation immediately
   * and ensure that the computation is rerun. The computation is rerun only
   * if it is invalidated.
   * @locus Client
   */
  flush() {
    console.warn('Tracker.Computation.flush() is not supported.')
  }

  /**
   * @summary Causes the function inside this computation to run and
   * synchronously process all reactive updtes.
   * @locus Client
   */
  run() {
    console.warn('Tracker.Computation.run() is not supported.')
  }
};

//
// http://docs.meteor.com/#tracker_dependency

/**
 * @summary A Dependency represents an atomic unit of reactive data that a
 * computation might depend on. Reactive data sources such as Session or
 * Minimongo internally create different Dependency objects for different
 * pieces of data, each of which may be depended on by multiple computations.
 * When the data changes, the computations are invalidated.
 * @class
 * @instanceName dependency
 */
Tracker.Dependency = class Dependency {
  constructor() {
    this._value = 0;
    this._ref = shallowRef(this._value);
  }

  // http://docs.meteor.com/#dependency_depend
  //
  // Adds `computation` to this set if it is not already
  // present.  Returns true if `computation` is a new member of the set.
  // If no argument, defaults to currentComputation, or does nothing
  // if there is no currentComputation.

  /**
   * @summary Declares that the current computation (or `fromComputation` if given) depends on `dependency`.  The computation will be invalidated the next time `dependency` changes.

   If there is no current computation and `depend()` is called with no arguments, it does nothing and returns false.

   Returns true if the computation is a new dependent of `dependency` rather than an existing one.
   * @locus Client
   * @param {Tracker.Computation} [fromComputation] An optional computation declared to depend on `dependency` instead of the current computation.
   * @returns {Boolean}
   */
  depend(computation) {
    if (computation != null) {
      console.warn('Dependency.depend(computation) not supported')
    }
    return this._ref.value
  }

  // http://docs.meteor.com/#dependency_changed

  /**
   * @summary Invalidate all dependent computations immediately and remove them as dependents.
   * @locus Client
   */
  changed() {
    this._ref.value = ++this._value;
  }

  // http://docs.meteor.com/#dependency_hasdependents

  /**
   * @summary True if this Dependency has one or more dependent Computations, which would be invalidated if this Dependency were to change.
   * @locus Client
   * @returns {Boolean}
   */
  hasDependents() {
    console.warn('Dependency.hasDependants() not supported')
    return true;
  }
};

// http://docs.meteor.com/#tracker_flush

/**
 * @summary Process all reactive updates immediately and ensure that all invalidated computations are rerun.
 * @locus Client
 */
Tracker.flush = function (options) {
  console.warn('Tracker.flush() is not supported.')
};

/**
 * @summary True if we are computing a computation now, either first time or recompute.  This matches Tracker.active unless we are inside Tracker.nonreactive, which nullfies currentComputation even though an enclosing computation may still be running.
 * @locus Client
 * @returns {Boolean}
 */
Tracker.inFlush = function () {
  console.warn('Tracker.inFlush() is not supported.')
  return false;
}

// http://docs.meteor.com/#tracker_autorun
//
// Run f(). Record its dependencies. Rerun it whenever the
// dependencies change.
//
// Returns a new Computation, which is also passed to f.
//
// Links the computation to the current computation
// so that it is stopped if the current computation is invalidated.

/**
 * @callback Tracker.ComputationFunction
 * @param {Tracker.Computation}
 */
/**
 * @summary Run a function now and rerun it later whenever its dependencies
 * change. Returns a Computation object that can be used to stop or observe the
 * rerunning.
 * @locus Client
 * @param {Tracker.ComputationFunction} runFunc The function to run. It receives
 * one argument: the Computation object that will be returned.
 * @param {Object} [options]
 * @param {Function} options.onError Optional. The function to run when an error
 * happens in the Computation. The only argument it receives is the Error
 * thrown. Defaults to the error being logged to the console.
 * @returns {Tracker.Computation}
 */
Tracker.autorun = function (f) {
  // detached scope when tracker is active
  const scope = effectScope(Tracker.active);
  const computation = new Tracker.Computation();

  let scopeActive = true;
  scope.cleanups.push(() => {
    scopeActive = false;
    computation.stop();
  })
  computation.onStop(() => {
    // we don't want to call scope.stop() twice
    if (scopeActive) {
      scopeActive = false;
      scope.stop()
    }
  });

  scope.run(function () {
    watchEffect(function () {
      if (!computation.firstRun) {
        computation._invalidate();
      }
      Tracker._runCapture(computation, f);
    });
  })
  if (Tracker.active) {
    // when parent stops/invalidates, recursively stop all handles
    Tracker.onInvalidate(function () {
      computation.stop();
    });
  }
  return computation;
};

Tracker.computed = function(getterOrOptions) {
  let getter, setter;
  if (typeof getterOrOptions === 'function') {
    getter = getterOrOptions;
  } else {
    getter = getterOrOptions.get;
    setter = getterOrOptions.set;
  }
  const computation = new Tracker.Computation();
  const cref = computed({
    get () {
      const previous = Tracker.currentComputation;
      setCurrentComputation(computation);
      const previousInCompute = inCompute;
      inCompute = true;
      try {
        return getter(computation);
      } finally {
        setCurrentComputation(previous);
        inCompute = previousInCompute;
        computation.firstRun = false;
      }
    },
    set: setter,
  });
  cref.effect.onStop = () => computation.stop();
  cref.__dirty = cref._dirty;
  Object.defineProperty(cref, '_dirty', {
    get() {
      return this.__dirty;
    },
    set(value) {
      this.__dirty = value;
      if (value)
        computation._invalidate(); // stop handles
    }
  });
  return cref;
};

Tracker.capture = function(f) {
  if (typeof f !== 'function')
    throw new Error('Tracker.autorun requires a function argument');
  const computation = new Tracker.Computation();
  Tracker._runCapture(computation, f);
  if (Tracker.active) {
    // when parent stops/invalidates, recursively stop all handles
    Tracker.onInvalidate(function () {
      computation.stop();
    });
  }
  return computation;
};


Tracker._runCapture = function(computation, f) {
  if (typeof f !== 'function')
    throw new Error('Tracker.autorun requires a function argument');

  let errored = true;
  try {
    const previous = Tracker.currentComputation;
    setCurrentComputation(computation);
    const previousInCompute = inCompute;
    inCompute = true;
    try {
      f(computation);
    } finally {
      setCurrentComputation(previous);
      inCompute = previousInCompute;
    }
    errored = false;
  } finally {
    computation.firstRun = false;
    if (errored)
      computation.stop();
  }
};

// http://docs.meteor.com/#tracker_nonreactive
//
// Run `f` with no current computation, returning the return value
// of `f`.  Used to turn off reactivity for the duration of `f`,
// so that reactive data sources accessed by `f` will not result in any
// computations being invalidated.

/**
 * @summary Run a function without tracking dependencies.
 * @locus Client
 * @param {Function} func A function to call immediately.
 */
Tracker.nonreactive = function (f) {
  var previous = Tracker.currentComputation;
  pauseTracking()
  setCurrentComputation(null);
  try {
    return f();
  } finally {
    setCurrentComputation(previous);
    resetTracking();
  }
};

Tracker.withComputation = function (computation, f) {
  var previousComputation = Tracker.currentComputation;

  Tracker.currentComputation = computation;
  Tracker.active = !!computation;

  try {
    return f();
  } finally {
    Tracker.currentComputation = previousComputation;
    Tracker.active = !!previousComputation;
  }
};

// http://docs.meteor.com/#tracker_oninvalidate

/**
 * @summary Registers a new [`onInvalidate`](#computation_oninvalidate) callback on the current computation (which must exist), to be called immediately when the current computation is invalidated or stopped.
 * @locus Client
 * @param {Function} callback A callback function that will be invoked as `func(c)`, where `c` is the computation on which the callback is registered.
 */
Tracker.onInvalidate = function (f) {
  if (! Tracker.active)
    throw new Error("Tracker.onInvalidate requires a currentComputation");

  Tracker.currentComputation.onInvalidate(f);
};

// http://docs.meteor.com/#tracker_afterflush

/**
 * @summary Schedules a function to be called during the next flush, or later in the current flush if one is in progress, after all invalidated computations have been rerun.  The function will be run once and not on subsequent flushes unless `afterFlush` is called again.
 * @locus Client
 * @param {Function} callback A function to call at flush time.
 */
Tracker.afterFlush = function (f) {
  nextTick(f);
};
