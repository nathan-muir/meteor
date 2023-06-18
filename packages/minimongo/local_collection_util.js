// checks if all field names in an object are valid
import { MinimongoError } from "./error.js";
import * as fieldHelpers from "./field-helpers.js";


// Make sure field names do not contain Mongo restricted
// characters ('.', '$', '\0').
// https://docs.mongodb.com/manual/reference/limits/#Restrictions-on-Field-Names
const invalidCharMsg = {
  $: 'start with \'$\'',
  '.': 'contain \'.\'',
  '\0': 'contain null bytes'
};

export function assertHasValidFieldNames(doc) {
  if (doc && typeof doc === 'object') {
    JSON.stringify(doc, (key, value) => {
      assertIsValidFieldName(key);
      return value;
    });
  }
}

export function assertIsValidFieldName(key) {
  let match;
  if (typeof key === 'string' && (match = key.match(/^\$|\.|\0/))) {
    throw MinimongoError(`Key ${key} must not ${invalidCharMsg[match[0]]}`);
  }
}

// XXX maybe this should be EJSON.isObject, though EJSON doesn't know about
// RegExp
// XXX note that _type(undefined) === 3!!!!
export const _isPlainObject =x => {
  return x && fieldHelpers._type(x) === 3;
};

export const _selectorIsId =selector =>
  typeof selector === 'number' ||
  typeof selector === 'string' ||
  selector instanceof MongoID.ObjectID
;

// Is the selector just lookup by _id (shorthand or not)?
export const _selectorIsIdPerhapsAsObject =selector =>
  _selectorIsId(selector) ||
  _selectorIsId(selector && selector._id) &&
  Object.keys(selector).length === 1
;
