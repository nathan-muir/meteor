import { Meteor } from './client_environment.js';
import { EnvironmentVariable, getEnvironment, setEnvironment } from './dynamics_browser_ev.js';
// Simple implementation of dynamic scoping, for use in browsers


Meteor.EnvironmentVariable = EnvironmentVariable;

Meteor.bindEnvironment = function (func, onException, _this) {
  // needed in order to be able to create closures inside func and
  // have the closed variables not change back to their original
  // values
  var boundValues = getEnvironment().slice();

  if (!onException || typeof(onException) === 'string') {
    var description = onException || "callback of async function";
    onException = function (error) {
      Meteor._debug(
        "Exception in " + description + ":",
        error
      );
    };
  }

  return function (/* arguments */) {
    var savedValues = getEnvironment();
    try {
      setEnvironment(boundValues);
      var ret = func.apply(_this, arguments);
    } catch (e) {
      // note: callback-hook currently relies on the fact that if onException
      // throws in the browser, the wrapped call throws.
      onException(e);
    } finally {
      setEnvironment(savedValues);
    }
    return ret;
  };
};

Meteor._nodeCodeMustBeInFiber = function () {
  // no-op on browser
};
