import { shallowReactive, toRaw } from '@vue/runtime-dom';

const hasOwn = Object.prototype.hasOwnProperty;

// XXX come up with a serialization method which canonicalizes object key
// order, which would allow us to use objects as values for equals.
function stringify(value) {
  if (value === undefined) {
    return 'undefined';
  }
  return EJSON.stringify(value);
}

function parse(serialized) {
  if (serialized === undefined || serialized === 'undefined') {
    return undefined;
  }
  return EJSON.parse(serialized);
}

function changed(v) {
  v && v.changed();
}

// XXX COMPAT WITH 0.9.1 : accept migrationData instead of dictName
/**
 * @class
 * @instanceName ReactiveDict
 * @summary Constructor for a ReactiveDict, which represents a reactive dictionary of key/value pairs.
 * @locus Client
 * @param {String} [name] Optional.  When a name is passed, preserves contents across Hot Code Pushes
 * @param {Object} [initialValue] Optional.  The default values for the dictionary
 */
export class ReactiveDict {
  constructor(dictName, dictData) {
    // this.keys: key -> value
    this.keys = shallowReactive(new Map());

    if (dictName) {
      // name given; migration will be performed
      if (typeof dictName === 'string') {
        // the normal case, argument is a string name.

        // Only run migration logic on client, it will cause
        // duplicate name errors on server during reloads.
        // _registerDictForMigrate will throw an error on duplicate name.
        Meteor.isClient && ReactiveDict._registerDictForMigrate(dictName, this);
        const migratedData = Meteor.isClient && ReactiveDict._loadMigratedDict(dictName);

        if (migratedData) {
          // Don't stringify migrated data
          Object.entries(migratedData).forEach(([key, value]) => {
            this.keys.set(key, value);
          });
        } else {
          // Use _setObject to make sure values are stringified
          this._setObject(dictData || {});
        }
        this.name = dictName;
      } else if (typeof dictName === 'object') {
        // back-compat case: dictName is actually migrationData
        // Use _setObject to make sure values are stringified
        this._setObject(dictName);
      } else {
        throw new Error("Invalid ReactiveDict argument: " + dictName);
      }
    } else if (typeof dictData === 'object') {
      this._setObject(dictData);
    }

  }

  // set() began as a key/value method, but we are now overloading it
  // to take an object of key/value pairs, similar to backbone
  // http://backbonejs.org/#Model-set
  /**
   * @summary Set a value for a key in the ReactiveDict. Notify any listeners
   * that the value has changed (eg: redraw templates, and rerun any
   * [`Tracker.autorun`](#tracker_autorun) computations, that called
   * [`ReactiveDict.get`](#ReactiveDict_get) on this `key`.)
   * @locus Client
   * @param {String} key The key to set, eg, `selectedItem`
   * @param {EJSONable | undefined} value The new value for `key`
   */
  set(keyOrObject, value) {
    if ((typeof keyOrObject === 'object') && (value === undefined)) {
      // Called as `dict.set({...})`
      this._setObject(keyOrObject);
      return;
    }
    // the input isn't an object, so it must be a key
    // and we resume with the rest of the function
    this.keys.set(keyOrObject, stringify(value));
  }

  /**
   * @summary Set a value for a key if it hasn't been set before.
   * Otherwise works exactly the same as [`ReactiveDict.set`](#ReactiveDict-set).
   * @locus Client
   * @param {String} key The key to set, eg, `selectedItem`
   * @param {EJSONable | undefined} value The new value for `key`
   */
  setDefault(keyOrObject, value) {
    if ((typeof keyOrObject === 'object') && (value === undefined)) {
      // Called as `dict.setDefault({...})`
      this._setDefaultObject(keyOrObject);
      return;
    }
    // the input isn't an object, so it must be a key
    // and we resume with the rest of the function
    const key = keyOrObject;

    if (!toRaw(this.keys).has(key)) {
      this.set(key, value);
    }
  }

  /**
   * @summary Get the value assiciated with a key. If inside a [reactive
   * computation](#reactivity), invalidate the computation the next time the
   * value associated with this key is changed by
   * [`ReactiveDict.set`](#ReactiveDict-set).
   * This returns a clone of the value, so if it's an object or an array,
   * mutating the returned value has no effect on the value stored in the
   * ReactiveDict.
   * @locus Client
   * @param {String} key The key of the element to return
   */
  get(key) {
    return parse(this.keys.get(key));
  }

  /**
   * @summary Test if the stored entry for a key is equal to a value. If inside a
   * [reactive computation](#reactivity), invalidate the computation the next
   * time the variable changes to or from the value.
   * @locus Client
   * @param {String} key The name of the session variable to test
   * @param {String | Number | Boolean | null | undefined} value The value to
   * test against
   */
  equals(key, value) {
    // Mongo.ObjectID is in the 'mongo' package
    let ObjectID = null;
    if (Package.mongo) {
      ObjectID = Package.mongo.Mongo.ObjectID;
    }
    // We don't allow objects (or arrays that might include objects) for
    // .equals, because JSON.stringify doesn't canonicalize object key
    // order. (We can make equals have the right return value by parsing the
    // current value and using EJSON.equals, but we won't have a canonical
    // element of keyValueDeps[key] to store the dependency.) You can still use
    // "EJSON.equals(reactiveDict.get(key), value)".
    //
    // XXX we could allow arrays as long as we recursively check that there
    // are no objects
    if (typeof value !== 'string' &&
        typeof value !== 'number' &&
        typeof value !== 'boolean' &&
        typeof value !== 'undefined' &&
        !(value instanceof Date) &&
        !(ObjectID && value instanceof ObjectID) &&
        value !== null) {
      throw new Error("ReactiveDict.equals: value must be scalar");
    }
    let oldValue = undefined;
    if (this.keys.has(key)) {
      oldValue = parse(this.keys.get(key));
    }
    return EJSON.equals(oldValue, value);
  }

  /**
   * @summary Get all key-value pairs as a plain object. If inside a [reactive
   * computation](#reactivity), invalidate the computation the next time the
   * value associated with any key is changed by
   * [`ReactiveDict.set`](#ReactiveDict-set).
   * This returns a clone of each value, so if it's an object or an array,
   * mutating the returned value has no effect on the value stored in the
   * ReactiveDict.
   * @locus Client
   */
  all() {
    let ret = {};
    this.keys.forEach((value, key) => {
      ret[key] = parse(value);
    });
    return ret;
  }

  /**
   * @summary remove all key-value pairs from the ReactiveDict. Notify any
   * listeners that the value has changed (eg: redraw templates, and rerun any
   * [`Tracker.autorun`](#tracker_autorun) computations, that called
   * [`ReactiveDict.get`](#ReactiveDict_get) on this `key`.)
   * @locus Client
   */
  clear() {
    this.keys.clear();
  }

  /**
   * @summary remove a key-value pair from the ReactiveDict. Notify any listeners
   * that the value has changed (eg: redraw templates, and rerun any
   * [`Tracker.autorun`](#tracker_autorun) computations, that called
   * [`ReactiveDict.get`](#ReactiveDict_get) on this `key`.)
   * @locus Client
   * @param {String} key The key to delete, eg, `selectedItem`
   */
  delete(key) {
    return this.keys.delete(key);
  }

  /**
   * @summary Clear all values from the reactiveDict and prevent it from being
   * migrated on a Hot Code Pushes. Notify any listeners
   * that the value has changed (eg: redraw templates, and rerun any
   * [`Tracker.autorun`](#tracker_autorun) computations, that called
   * [`ReactiveDict.get`](#ReactiveDict_get) on this `key`.)
   * @locus Client
   */
  destroy() {
    this.clear();
    if (this.name && hasOwn.call(ReactiveDict._dictsToMigrate, this.name)) {
      delete ReactiveDict._dictsToMigrate[this.name];
    }
  }

  _setObject(object) {
    Object.keys(object).forEach(key => {
      this.set(key, object[key]);
    });
  }

  _setDefaultObject(object) {
    Object.keys(object).forEach(key => {
      this.setDefault(key, object[key]);
    });
  }

  // Get a JSON value that can be passed to the constructor to
  // create a new ReactiveDict with the same contents as this one
  _getMigrationData() {
    // XXX sanitize and make sure it's JSONible?
    let ret = {};
    toRaw(this.keys).forEach((value, key) => {
      ret[key] = value;
    });
    return ret;
  }
}
