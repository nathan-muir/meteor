// _CachingChangeObserver is an object which receives observeChanges callbacks
// and keeps a cache of the current cursor state up to date in this.docs. Users
// of this class should read the docs field but not modify it. You should pass
// the "applyChange" field as the callbacks to the underlying observeChanges
// call. Optionally, you can specify your own observeChanges callbacks which are
// invoked immediately before the docs field is updated; this object is made
// available as `this` to those callbacks.
import Matcher from './matcher.js';
import * as fieldHelpers from "./field-helpers.js";
import Sorter from './sorter.js';
import {
  hasOwn,
  isIndexable,
  isNumericKey,
  isOperatorObject,
  populateDocumentWithQueryFields,
  projectionDetails,
} from './common.js';
import { MinimongoError } from "./error.js";
import { assertHasValidFieldNames, assertIsValidFieldName } from "./local_collection_util.js";
import { _isPlainObject, _selectorIsId, _selectorIsIdPerhapsAsObject } from "./local_collection_util.js";


export class _CachingChangeObserver {
  constructor(options = {}) {
    const orderedFromCallbacks = (
      options.callbacks &&
      _observeChangesCallbacksAreOrdered(options.callbacks)
    );

    if (hasOwn.call(options, 'ordered')) {
      this.ordered = options.ordered;

      if (options.callbacks && options.ordered !== orderedFromCallbacks) {
        throw Error('ordered option doesn\'t match callbacks');
      }
    } else if (options.callbacks) {
      this.ordered = orderedFromCallbacks;
    } else {
      throw Error('must provide ordered or callbacks');
    }

    const callbacks = options.callbacks || {};

    if (this.ordered) {
      this.docs = new OrderedDict(MongoID.idStringify);
      this.applyChange = {
        addedBefore: (id, fields, before) => {
          // Take a shallow copy since the top-level properties can be changed
          const doc = { ...fields };

          doc._id = id;

          if (callbacks.addedBefore) {
            callbacks.addedBefore.call(this, id, EJSON.clone(fields), before);
          }

          // This line triggers if we provide added with movedBefore.
          if (callbacks.added) {
            callbacks.added.call(this, id, EJSON.clone(fields));
          }

          // XXX could `before` be a falsy ID?  Technically
          // idStringify seems to allow for them -- though
          // OrderedDict won't call stringify on a falsy arg.
          this.docs.putBefore(id, doc, before || null);
        },
        movedBefore: (id, before) => {
          const doc = this.docs.get(id);

          if (callbacks.movedBefore) {
            callbacks.movedBefore.call(this, id, before);
          }

          this.docs.moveBefore(id, before || null);
        },
      };
    } else {
      this.docs = new _IdMap;
      this.applyChange = {
        added: (id, fields) => {
          // Take a shallow copy since the top-level properties can be changed
          const doc = { ...fields };

          if (callbacks.added) {
            callbacks.added.call(this, id, EJSON.clone(fields));
          }

          doc._id = id;

          this.docs.set(id,  doc);
        },
      };
    }

    // The methods in _IdMap and OrderedDict used by these callbacks are
    // identical.
    this.applyChange.changed = (id, fields) => {
      const doc = this.docs.get(id);

      if (!doc) {
        throw new Error(`Unknown id for changed: ${id}`);
      }

      if (callbacks.changed) {
        callbacks.changed.call(this, id, EJSON.clone(fields));
      }

      DiffSequence.applyChanges(doc, fields);
    };

    this.applyChange.removed = id => {
      if (callbacks.removed) {
        callbacks.removed.call(this, id);
      }

      this.docs.remove(id);
    };
  }
}


export class _IdMap extends IdMap {
  constructor() {
    super(MongoID.idStringify, MongoID.idParse);
  }
}

// Wrap a transform function to return objects that have the _id field
// of the untransformed document. This ensures that subsystems such as
// the observe-sequence package that call `observe` can keep track of
// the documents identities.
//
// - Require that it returns objects
// - If the return value has an _id field, verify that it matches the
//   original _id field
// - If the return value doesn't have an _id field, add it back.
export const wrapTransform =transform => {
  if (!transform) {
    return null;
  }

  // No need to doubly-wrap transforms.
  if (transform.__wrappedTransform__) {
    return transform;
  }

  const wrapped = doc => {
    if (!hasOwn.call(doc, '_id')) {
      // XXX do we ever have a transform on the oplog's collection? because that
      // collection has no _id.
      throw new Error('can only transform documents with _id');
    }

    const id = doc._id;

    // XXX consider making tracker a weak dependency and checking
    // Package.tracker here
    const transformed = Tracker.nonreactive(() => transform(doc));

    if (!_isPlainObject(transformed)) {
      throw new Error('transform must return object');
    }

    if (hasOwn.call(transformed, '_id')) {
      if (!EJSON.equals(transformed._id, id)) {
        throw new Error('transformed document can\'t have different _id');
      }
    } else {
      transformed._id = id;
    }

    return transformed;
  };

  wrapped.__wrappedTransform__ = true;

  return wrapped;
};

// XXX the sorted-query logic below is laughably inefficient. we'll
// need to come up with a better datastructure for this.
//
// XXX the logic for observing with a skip or a limit is even more
// laughably inefficient. we recompute the whole results every time!

// This binary search puts a value between any equal values, and the first
// lesser value.
export const _binarySearch =(cmp, array, value) => {
  let first = 0;
  let range = array.length;

  while (range > 0) {
    const halfRange = Math.floor(range / 2);

    if (cmp(value, array[first + halfRange]) >= 0) {
      first += halfRange + 1;
      range -= halfRange + 1;
    } else {
      range = halfRange;
    }
  }

  return first;
};

export const _checkSupportedProjection =fields => {
  if (fields !== Object(fields) || Array.isArray(fields)) {
    throw MinimongoError('fields option must be an object');
  }

  Object.keys(fields).forEach(keyPath => {
    if (keyPath.split('.').includes('$')) {
      throw MinimongoError(
        'Minimongo doesn\'t support $ operator in projections yet.'
      );
    }

    const value = fields[keyPath];

    if (typeof value === 'object' &&
        ['$elemMatch', '$meta', '$slice'].some(key =>
          hasOwn.call(value, key)
        )) {
      throw MinimongoError(
        'Minimongo doesn\'t support operators in projections yet.'
      );
    }

    if (![1, 0, true, false].includes(value)) {
      throw MinimongoError(
        'Projection values should be one of 1, 0, true, or false'
      );
    }
  });
};

// Knows how to compile a fields projection to a predicate function.
// @returns - Function: a closure that filters out an object according to the
//            fields projection rules:
//            @param obj - Object: MongoDB-styled document
//            @returns - Object: a document with the fields filtered out
//                       according to projection rules. Doesn't retain subfields
//                       of passed argument.
export const _compileProjection =fields => {
  _checkSupportedProjection(fields);

  const _idProjection = fields._id === undefined ? true : fields._id;
  const details = projectionDetails(fields);

  // returns transformed doc according to ruleTree
  const transform = (doc, ruleTree) => {
    // Special case for "sets"
    if (Array.isArray(doc)) {
      return doc.map(subdoc => transform(subdoc, ruleTree));
    }

    const result = details.including ? {} : EJSON.clone(doc);

    Object.keys(ruleTree).forEach(key => {
      if (doc == null || !hasOwn.call(doc, key)) {
        return;
      }

      const rule = ruleTree[key];

      if (rule === Object(rule)) {
        // For sub-objects/subsets we branch
        if (doc[key] === Object(doc[key])) {
          result[key] = transform(doc[key], rule);
        }
      } else if (details.including) {
        // Otherwise we don't even touch this subfield
        result[key] = EJSON.clone(doc[key]);
      } else {
        delete result[key];
      }
    });

    return doc != null ? result : doc;
  };

  return doc => {
    const result = transform(doc, details.tree);

    if (_idProjection && hasOwn.call(doc, '_id')) {
      result._id = doc._id;
    }

    if (!_idProjection && hasOwn.call(result, '_id')) {
      delete result._id;
    }

    return result;
  };
};

// Calculates the document to insert in case we're doing an upsert and the
// selector does not match any elements
export const _createUpsertDocument =(selector, modifier) => {
  const selectorDocument = populateDocumentWithQueryFields(selector);
  const isModify = _isModificationMod(modifier);

  const newDoc = {};

  if (selectorDocument._id) {
    newDoc._id = selectorDocument._id;
    delete selectorDocument._id;
  }

  // This double _modify call is made to help with nested properties (see issue
  // #8631). We do this even if it's a replacement for validation purposes (e.g.
  // ambiguous id's)
  _modify(newDoc, {$set: selectorDocument});
  _modify(newDoc, modifier, {isInsert: true});

  if (isModify) {
    return newDoc;
  }

  // Replacement can take _id from query document
  const replacement = Object.assign({}, modifier);
  if (newDoc._id) {
    replacement._id = newDoc._id;
  }

  return replacement;
};

export const _diffObjects =(left, right, callbacks) => {
  return DiffSequence.diffObjects(left, right, callbacks);
};

// ordered: bool.
// old_results and new_results: collections of documents.
//    if ordered, they are arrays.
//    if unordered, they are IdMaps
export const _diffQueryChanges =(ordered, oldResults, newResults, observer, options) =>
  DiffSequence.diffQueryChanges(ordered, oldResults, newResults, observer, options)
;

export const _diffQueryOrderedChanges =(oldResults, newResults, observer, options) =>
  DiffSequence.diffQueryOrderedChanges(oldResults, newResults, observer, options)
;

export const _diffQueryUnorderedChanges =(oldResults, newResults, observer, options) =>
  DiffSequence.diffQueryUnorderedChanges(oldResults, newResults, observer, options)
;

export const _findInOrderedResults =(query, doc) => {
  if (!query.ordered) {
    throw new Error('Can\'t call _findInOrderedResults on unordered query');
  }

  for (let i = 0; i < query.results.length; i++) {
    if (query.results[i] === doc) {
      return i;
    }
  }

  throw Error('object missing from query');
};

// If this is a selector which explicitly constrains the match by ID to a finite
// number of documents, returns a list of their IDs.  Otherwise returns
// null. Note that the selector may have other restrictions so it may not even
// match those document!  We care about $in and $and since those are generated
// access-controlled update and remove.
export const _idsMatchedBySelector =selector => {
  // Is the selector just an ID?
  if (_selectorIsId(selector)) {
    return [selector];
  }

  if (!selector) {
    return null;
  }

  // Do we have an _id clause?
  if (hasOwn.call(selector, '_id')) {
    // Is the _id clause just an ID?
    if (_selectorIsId(selector._id)) {
      return [selector._id];
    }

    // Is the _id clause {_id: {$in: ["x", "y", "z"]}}?
    if (selector._id
        && Array.isArray(selector._id.$in)
        && selector._id.$in.length
        && selector._id.$in.every(_selectorIsId)) {
      return selector._id.$in;
    }

    return null;
  }

  // If this is a top-level $and, and any of the clauses constrain their
  // documents, then the whole selector is constrained by any one clause's
  // constraint. (Well, by their intersection, but that seems unlikely.)
  if (Array.isArray(selector.$and)) {
    for (let i = 0; i < selector.$and.length; ++i) {
      const subIds = _idsMatchedBySelector(selector.$and[i]);

      if (subIds) {
        return subIds;
      }
    }
  }

  return null;
};

export const _insertInResults =(query, doc) => {
  const fields = EJSON.clone(doc);

  delete fields._id;

  if (query.ordered) {
    if (!query.sorter) {
      query.addedBefore(doc._id, query.projectionFn(fields), null);
      query.results.push(doc);
    } else {
      const i = _insertInSortedList(
        query.sorter.getComparator({distances: query.distances}),
        query.results,
        doc
      );

      let next = query.results[i + 1];
      if (next) {
        next = next._id;
      } else {
        next = null;
      }

      query.addedBefore(doc._id, query.projectionFn(fields), next);
    }

    query.added(doc._id, query.projectionFn(fields));
  } else {
    query.added(doc._id, query.projectionFn(fields));
    query.results.set(doc._id, doc);
  }
};

export const _insertInSortedList =(cmp, array, value) => {
  if (array.length === 0) {
    array.push(value);
    return 0;
  }

  const i = _binarySearch(cmp, array, value);

  array.splice(i, 0, value);

  return i;
};

export const _isModificationMod =mod => {
  let isModify = false;
  let isReplace = false;

  Object.keys(mod).forEach(key => {
    if (key.substr(0, 1) === '$') {
      isModify = true;
    } else {
      isReplace = true;
    }
  });

  if (isModify && isReplace) {
    throw new Error(
      'Update parameter cannot have both modifier and non-modifier fields.'
    );
  }

  return isModify;
};


// XXX need a strategy for passing the binding of $ into this
// function, from the compiled selector
//
// maybe just {key.up.to.just.before.dollarsign: array_index}
//
// XXX atomicity: if one modification fails, do we roll back the whole
// change?
//
// options:
//   - isInsert is set when _modify is being called to compute the document to
//     insert as part of an upsert operation. We use this primarily to figure
//     out when to set the fields in $setOnInsert, if present.
export const _modify =(doc, modifier, options = {}) => {
  if (!_isPlainObject(modifier)) {
    throw MinimongoError('Modifier must be an object');
  }

  // Make sure the caller can't mutate our data structures.
  modifier = EJSON.clone(modifier);

  const isModifier = isOperatorObject(modifier);
  const newDoc = isModifier ? EJSON.clone(doc) : modifier;

  if (isModifier) {
    // apply modifiers to the doc.
    Object.keys(modifier).forEach(operator => {
      // Treat $setOnInsert as $set if this is an insert.
      const setOnInsert = options.isInsert && operator === '$setOnInsert';
      const modFunc = MODIFIERS[setOnInsert ? '$set' : operator];
      const operand = modifier[operator];

      if (!modFunc) {
        throw MinimongoError(`Invalid modifier specified ${operator}`);
      }

      Object.keys(operand).forEach(keypath => {
        const arg = operand[keypath];

        if (keypath === '') {
          throw MinimongoError('An empty update path is not valid.');
        }

        const keyparts = keypath.split('.');

        if (!keyparts.every(Boolean)) {
          throw MinimongoError(
            `The update path '${keypath}' contains an empty field name, ` +
            'which is not allowed.'
          );
        }

        const target = findModTarget(newDoc, keyparts, {
          arrayIndices: options.arrayIndices,
          forbidArray: operator === '$rename',
          noCreate: NO_CREATE_MODIFIERS[operator]
        });

        modFunc(target, keyparts.pop(), arg, keypath, newDoc);
      });
    });

    if (doc._id && !EJSON.equals(doc._id, newDoc._id)) {
      throw MinimongoError(
        `After applying the update to the document {_id: "${doc._id}", ...},` +
        ' the (immutable) field \'_id\' was found to have been altered to ' +
        `_id: "${newDoc._id}"`
      );
    }
  } else {
    if (doc._id && modifier._id && !EJSON.equals(doc._id, modifier._id)) {
      throw MinimongoError(
        `The _id field cannot be changed from {_id: "${doc._id}"} to ` +
        `{_id: "${modifier._id}"}`
      );
    }

    // replace the whole document
    assertHasValidFieldNames(modifier);
  }

  // move new document into place.
  Object.keys(doc).forEach(key => {
    // Note: this used to be for (var key in doc) however, this does not
    // work right in Opera. Deleting from a doc while iterating over it
    // would sometimes cause opera to skip some keys.
    if (key !== '_id') {
      delete doc[key];
    }
  });

  Object.keys(newDoc).forEach(key => {
    doc[key] = newDoc[key];
  });
};

export const _observeFromObserveChanges =(cursor, observeCallbacks) => {
  const transform = cursor.getTransform() || (doc => doc);
  let suppressed = !!observeCallbacks._suppress_initial;

  let observeChangesCallbacks;
  if (_observeCallbacksAreOrdered(observeCallbacks)) {
    // The "_no_indices" option sets all index arguments to -1 and skips the
    // linear scans required to generate them.  This lets observers that don't
    // need absolute indices benefit from the other features of this API --
    // relative order, transforms, and applyChanges -- without the speed hit.
    const indices = !observeCallbacks._no_indices;

    observeChangesCallbacks = {
      addedBefore(id, fields, before) {
        if (suppressed || !(observeCallbacks.addedAt || observeCallbacks.added)) {
          return;
        }

        const doc = transform(Object.assign(fields, {_id: id}));

        if (observeCallbacks.addedAt) {
          observeCallbacks.addedAt(
            doc,
            indices
              ? before
                ? this.docs.indexOf(before)
                : this.docs.size()
              : -1,
            before
          );
        } else {
          observeCallbacks.added(doc);
        }
      },
      changed(id, fields) {
        if (!(observeCallbacks.changedAt || observeCallbacks.changed)) {
          return;
        }

        let doc = EJSON.clone(this.docs.get(id));
        if (!doc) {
          throw new Error(`Unknown id for changed: ${id}`);
        }

        const oldDoc = transform(EJSON.clone(doc));

        DiffSequence.applyChanges(doc, fields);

        if (observeCallbacks.changedAt) {
          observeCallbacks.changedAt(
            transform(doc),
            oldDoc,
            indices ? this.docs.indexOf(id) : -1
          );
        } else {
          observeCallbacks.changed(transform(doc), oldDoc);
        }
      },
      movedBefore(id, before) {
        if (!observeCallbacks.movedTo) {
          return;
        }

        const from = indices ? this.docs.indexOf(id) : -1;
        let to = indices
          ? before
            ? this.docs.indexOf(before)
            : this.docs.size()
          : -1;

        // When not moving backwards, adjust for the fact that removing the
        // document slides everything back one slot.
        if (to > from) {
          --to;
        }

        observeCallbacks.movedTo(
          transform(EJSON.clone(this.docs.get(id))),
          from,
          to,
          before || null
        );
      },
      removed(id) {
        if (!(observeCallbacks.removedAt || observeCallbacks.removed)) {
          return;
        }

        // technically maybe there should be an EJSON.clone here, but it's about
        // to be removed from this.docs!
        const doc = transform(this.docs.get(id));

        if (observeCallbacks.removedAt) {
          observeCallbacks.removedAt(doc, indices ? this.docs.indexOf(id) : -1);
        } else {
          observeCallbacks.removed(doc);
        }
      },
    };
  } else {
    observeChangesCallbacks = {
      added(id, fields) {
        if (!suppressed && observeCallbacks.added) {
          observeCallbacks.added(transform(Object.assign(fields, {_id: id})));
        }
      },
      changed(id, fields) {
        if (observeCallbacks.changed) {
          const oldDoc = this.docs.get(id);
          const doc = EJSON.clone(oldDoc);

          DiffSequence.applyChanges(doc, fields);

          observeCallbacks.changed(
            transform(doc),
            transform(EJSON.clone(oldDoc))
          );
        }
      },
      removed(id) {
        if (observeCallbacks.removed) {
          observeCallbacks.removed(transform(this.docs.get(id)));
        }
      },
    };
  }

  const changeObserver = new _CachingChangeObserver({
    callbacks: observeChangesCallbacks
  });

  // CachingChangeObserver clones all received input on its callbacks
  // So we can mark it as safe to reduce the ejson clones.
  // This is tested by the `mongo-livedata - (extended) scribbling` tests
  changeObserver.applyChange._fromObserve = true;
  const handle = cursor.observeChanges(changeObserver.applyChange,
    { nonMutatingCallbacks: true });

  suppressed = false;

  return handle;
};

export const _observeCallbacksAreOrdered =callbacks => {
  if (callbacks.added && callbacks.addedAt) {
    throw new Error('Please specify only one of added() and addedAt()');
  }

  if (callbacks.changed && callbacks.changedAt) {
    throw new Error('Please specify only one of changed() and changedAt()');
  }

  if (callbacks.removed && callbacks.removedAt) {
    throw new Error('Please specify only one of removed() and removedAt()');
  }

  return !!(
    callbacks.addedAt ||
    callbacks.changedAt ||
    callbacks.movedTo ||
    callbacks.removedAt
  );
};

export const _observeChangesCallbacksAreOrdered =callbacks => {
  if (callbacks.added && callbacks.addedBefore) {
    throw new Error('Please specify only one of added() and addedBefore()');
  }

  return !!(callbacks.addedBefore || callbacks.movedBefore);
};

export const _removeFromResults =(query, doc) => {
  if (query.ordered) {
    const i = _findInOrderedResults(query, doc);

    query.removed(doc._id);
    query.results.splice(i, 1);
  } else {
    const id = doc._id;  // in case callback mutates doc

    query.removed(doc._id);
    query.results.remove(id);
  }
};

// Is this selector just shorthand for lookup by _id?


export const _updateInResults =(query, doc, old_doc) => {
  if (!EJSON.equals(doc._id, old_doc._id)) {
    throw new Error('Can\'t change a doc\'s _id while updating');
  }

  const projectionFn = query.projectionFn;
  const changedFields = DiffSequence.makeChangedFields(
    projectionFn(doc),
    projectionFn(old_doc)
  );

  if (!query.ordered) {
    if (Object.keys(changedFields).length) {
      query.changed(doc._id, changedFields);
      query.results.set(doc._id, doc);
    }

    return;
  }

  const old_idx = _findInOrderedResults(query, doc);

  if (Object.keys(changedFields).length) {
    query.changed(doc._id, changedFields);
  }

  if (!query.sorter) {
    return;
  }

  // just take it out and put it back in again, and see if the index changes
  query.results.splice(old_idx, 1);

  const new_idx = _insertInSortedList(
    query.sorter.getComparator({distances: query.distances}),
    query.results,
    doc
  );

  if (old_idx !== new_idx) {
    let next = query.results[new_idx + 1];
    if (next) {
      next = next._id;
    } else {
      next = null;
    }

    query.movedBefore && query.movedBefore(doc._id, next);
  }
};

const MODIFIERS = {
  $currentDate(target, field, arg) {
    if (typeof arg === 'object' && hasOwn.call(arg, '$type')) {
      if (arg.$type !== 'date') {
        throw MinimongoError(
          'Minimongo does currently only support the date type in ' +
          '$currentDate modifiers',
          {field}
        );
      }
    } else if (arg !== true) {
      throw MinimongoError('Invalid $currentDate modifier', {field});
    }

    target[field] = new Date();
  },
  $inc(target, field, arg) {
    if (typeof arg !== 'number') {
      throw MinimongoError('Modifier $inc allowed for numbers only', {field});
    }

    if (field in target) {
      if (typeof target[field] !== 'number') {
        throw MinimongoError(
          'Cannot apply $inc modifier to non-number',
          {field}
        );
      }

      target[field] += arg;
    } else {
      target[field] = arg;
    }
  },
  $min(target, field, arg) {
    if (typeof arg !== 'number') {
      throw MinimongoError('Modifier $min allowed for numbers only', {field});
    }

    if (field in target) {
      if (typeof target[field] !== 'number') {
        throw MinimongoError(
          'Cannot apply $min modifier to non-number',
          {field}
        );
      }

      if (target[field] > arg) {
        target[field] = arg;
      }
    } else {
      target[field] = arg;
    }
  },
  $max(target, field, arg) {
    if (typeof arg !== 'number') {
      throw MinimongoError('Modifier $max allowed for numbers only', {field});
    }

    if (field in target) {
      if (typeof target[field] !== 'number') {
        throw MinimongoError(
          'Cannot apply $max modifier to non-number',
          {field}
        );
      }

      if (target[field] < arg) {
        target[field] = arg;
      }
    } else {
      target[field] = arg;
    }
  },
  $mul(target, field, arg) {
    if (typeof arg !== 'number') {
      throw MinimongoError('Modifier $mul allowed for numbers only', {field});
    }

    if (field in target) {
      if (typeof target[field] !== 'number') {
        throw MinimongoError(
          'Cannot apply $mul modifier to non-number',
          {field}
        );
      }

      target[field] *= arg;
    } else {
      target[field] = 0;
    }
  },
  $rename(target, field, arg, keypath, doc) {
    // no idea why mongo has this restriction..
    if (keypath === arg) {
      throw MinimongoError('$rename source must differ from target', {field});
    }

    if (target === null) {
      throw MinimongoError('$rename source field invalid', {field});
    }

    if (typeof arg !== 'string') {
      throw MinimongoError('$rename target must be a string', {field});
    }

    if (arg.includes('\0')) {
      // Null bytes are not allowed in Mongo field names
      // https://docs.mongodb.com/manual/reference/limits/#Restrictions-on-Field-Names
      throw MinimongoError(
        'The \'to\' field for $rename cannot contain an embedded null byte',
        {field}
      );
    }

    if (target === undefined) {
      return;
    }

    const object = target[field];

    delete target[field];

    const keyparts = arg.split('.');
    const target2 = findModTarget(doc, keyparts, {forbidArray: true});

    if (target2 === null) {
      throw MinimongoError('$rename target field invalid', {field});
    }

    target2[keyparts.pop()] = object;
  },
  $set(target, field, arg) {
    if (target !== Object(target)) { // not an array or an object
      const error = MinimongoError(
        'Cannot set property on non-object field',
        {field}
      );
      error.setPropertyError = true;
      throw error;
    }

    if (target === null) {
      const error = MinimongoError('Cannot set property on null', {field});
      error.setPropertyError = true;
      throw error;
    }

    assertHasValidFieldNames(arg);

    target[field] = arg;
  },
  $setOnInsert(target, field, arg) {
    // converted to `$set` in `_modify`
  },
  $unset(target, field, arg) {
    if (target !== undefined) {
      if (target instanceof Array) {
        if (field in target) {
          target[field] = null;
        }
      } else {
        delete target[field];
      }
    }
  },
  $push(target, field, arg) {
    if (target[field] === undefined) {
      target[field] = [];
    }

    if (!(target[field] instanceof Array)) {
      throw MinimongoError('Cannot apply $push modifier to non-array', {field});
    }

    if (!(arg && arg.$each)) {
      // Simple mode: not $each
      assertHasValidFieldNames(arg);

      target[field].push(arg);

      return;
    }

    // Fancy mode: $each (and maybe $slice and $sort and $position)
    const toPush = arg.$each;
    if (!(toPush instanceof Array)) {
      throw MinimongoError('$each must be an array', {field});
    }

    assertHasValidFieldNames(toPush);

    // Parse $position
    let position = undefined;
    if ('$position' in arg) {
      if (typeof arg.$position !== 'number') {
        throw MinimongoError('$position must be a numeric value', {field});
      }

      // XXX should check to make sure integer
      if (arg.$position < 0) {
        throw MinimongoError(
          '$position in $push must be zero or positive',
          {field}
        );
      }

      position = arg.$position;
    }

    // Parse $slice.
    let slice = undefined;
    if ('$slice' in arg) {
      if (typeof arg.$slice !== 'number') {
        throw MinimongoError('$slice must be a numeric value', {field});
      }

      // XXX should check to make sure integer
      slice = arg.$slice;
    }

    // Parse $sort.
    let sortFunction = undefined;
    if (arg.$sort) {
      if (slice === undefined) {
        throw MinimongoError('$sort requires $slice to be present', {field});
      }

      // XXX this allows us to use a $sort whose value is an array, but that's
      // actually an extension of the Node driver, so it won't work
      // server-side. Could be confusing!
      // XXX is it correct that we don't do geo-stuff here?
      sortFunction = new Sorter(arg.$sort).getComparator();

      toPush.forEach(element => {
        if (fieldHelpers._type(element) !== 3) {
          throw MinimongoError(
            '$push like modifiers using $sort require all elements to be ' +
            'objects',
            {field}
          );
        }
      });
    }

    // Actually push.
    if (position === undefined) {
      toPush.forEach(element => {
        target[field].push(element);
      });
    } else {
      const spliceArguments = [position, 0];

      toPush.forEach(element => {
        spliceArguments.push(element);
      });

      target[field].splice(...spliceArguments);
    }

    // Actually sort.
    if (sortFunction) {
      target[field].sort(sortFunction);
    }

    // Actually slice.
    if (slice !== undefined) {
      if (slice === 0) {
        target[field] = []; // differs from Array.slice!
      } else if (slice < 0) {
        target[field] = target[field].slice(slice);
      } else {
        target[field] = target[field].slice(0, slice);
      }
    }
  },
  $pushAll(target, field, arg) {
    if (!(typeof arg === 'object' && arg instanceof Array)) {
      throw MinimongoError('Modifier $pushAll/pullAll allowed for arrays only');
    }

    assertHasValidFieldNames(arg);

    const toPush = target[field];

    if (toPush === undefined) {
      target[field] = arg;
    } else if (!(toPush instanceof Array)) {
      throw MinimongoError(
        'Cannot apply $pushAll modifier to non-array',
        {field}
      );
    } else {
      toPush.push(...arg);
    }
  },
  $addToSet(target, field, arg) {
    let isEach = false;

    if (typeof arg === 'object') {
      // check if first key is '$each'
      const keys = Object.keys(arg);
      if (keys[0] === '$each') {
        isEach = true;
      }
    }

    const values = isEach ? arg.$each : [arg];

    assertHasValidFieldNames(values);

    const toAdd = target[field];
    if (toAdd === undefined) {
      target[field] = values;
    } else if (!(toAdd instanceof Array)) {
      throw MinimongoError(
        'Cannot apply $addToSet modifier to non-array',
        {field}
      );
    } else {
      values.forEach(value => {
        if (toAdd.some(element => fieldHelpers._equal(value, element))) {
          return;
        }

        toAdd.push(value);
      });
    }
  },
  $pop(target, field, arg) {
    if (target === undefined) {
      return;
    }

    const toPop = target[field];

    if (toPop === undefined) {
      return;
    }

    if (!(toPop instanceof Array)) {
      throw MinimongoError('Cannot apply $pop modifier to non-array', {field});
    }

    if (typeof arg === 'number' && arg < 0) {
      toPop.splice(0, 1);
    } else {
      toPop.pop();
    }
  },
  $pull(target, field, arg) {
    if (target === undefined) {
      return;
    }

    const toPull = target[field];
    if (toPull === undefined) {
      return;
    }

    if (!(toPull instanceof Array)) {
      throw MinimongoError(
        'Cannot apply $pull/pullAll modifier to non-array',
        {field}
      );
    }

    let out;
    if (arg != null && typeof arg === 'object' && !(arg instanceof Array)) {
      // XXX would be much nicer to compile this once, rather than
      // for each document we modify.. but usually we're not
      // modifying that many documents, so we'll let it slide for
      // now

      // XXX Minimongo.Matcher isn't up for the job, because we need
      // to permit stuff like {$pull: {a: {$gt: 4}}}.. something
      // like {$gt: 4} is not normally a complete selector.
      // same issue as $elemMatch possibly?
      const matcher = new Matcher(arg);

      out = toPull.filter(element => !matcher.documentMatches(element).result);
    } else {
      out = toPull.filter(element => !fieldHelpers._equal(element, arg));
    }

    target[field] = out;
  },
  $pullAll(target, field, arg) {
    if (!(typeof arg === 'object' && arg instanceof Array)) {
      throw MinimongoError(
        'Modifier $pushAll/pullAll allowed for arrays only',
        {field}
      );
    }

    if (target === undefined) {
      return;
    }

    const toPull = target[field];

    if (toPull === undefined) {
      return;
    }

    if (!(toPull instanceof Array)) {
      throw MinimongoError(
        'Cannot apply $pull/pullAll modifier to non-array',
        {field}
      );
    }

    target[field] = toPull.filter(object =>
      !arg.some(element => fieldHelpers._equal(object, element))
    );
  },
  $bit(target, field, arg) {
    // XXX mongo only supports $bit on integers, and we only support
    // native javascript numbers (doubles) so far, so we can't support $bit
    throw MinimongoError('$bit is not supported', {field});
  },
  $v() {
    // As discussed in https://github.com/meteor/meteor/issues/9623,
    // the `$v` operator is not needed by Meteor, but problems can occur if
    // it's not at least callable (as of Mongo >= 3.6). It's defined here as
    // a no-op to work around these problems.
  }
};

const NO_CREATE_MODIFIERS = {
  $pop: true,
  $pull: true,
  $pullAll: true,
  $rename: true,
  $unset: true
};


// for a.b.c.2.d.e, keyparts should be ['a', 'b', 'c', '2', 'd', 'e'],
// and then you would operate on the 'e' property of the returned
// object.
//
// if options.noCreate is falsey, creates intermediate levels of
// structure as necessary, like mkdir -p (and raises an exception if
// that would mean giving a non-numeric property to an array.) if
// options.noCreate is true, return undefined instead.
//
// may modify the last element of keyparts to signal to the caller that it needs
// to use a different value to index into the returned object (for example,
// ['a', '01'] -> ['a', 1]).
//
// if forbidArray is true, return null if the keypath goes through an array.
//
// if options.arrayIndices is set, use its first element for the (first) '$' in
// the path.
function findModTarget(doc, keyparts, options = {}) {
  let usedArrayIndex = false;

  for (let i = 0; i < keyparts.length; i++) {
    const last = i === keyparts.length - 1;
    let keypart = keyparts[i];

    if (!isIndexable(doc)) {
      if (options.noCreate) {
        return undefined;
      }

      const error = MinimongoError(
        `cannot use the part '${keypart}' to traverse ${doc}`
      );
      error.setPropertyError = true;
      throw error;
    }

    if (doc instanceof Array) {
      if (options.forbidArray) {
        return null;
      }

      if (keypart === '$') {
        if (usedArrayIndex) {
          throw MinimongoError('Too many positional (i.e. \'$\') elements');
        }

        if (!options.arrayIndices || !options.arrayIndices.length) {
          throw MinimongoError(
            'The positional operator did not find the match needed from the ' +
            'query'
          );
        }

        keypart = options.arrayIndices[0];
        usedArrayIndex = true;
      } else if (isNumericKey(keypart)) {
        keypart = parseInt(keypart);
      } else {
        if (options.noCreate) {
          return undefined;
        }

        throw MinimongoError(
          `can't append to array using string field name [${keypart}]`
        );
      }

      if (last) {
        keyparts[i] = keypart; // handle 'a.01'
      }

      if (options.noCreate && keypart >= doc.length) {
        return undefined;
      }

      while (doc.length < keypart) {
        doc.push(null);
      }

      if (!last) {
        if (doc.length === keypart) {
          doc.push({});
        } else if (typeof doc[keypart] !== 'object') {
          throw MinimongoError(
            `can't modify field '${keyparts[i + 1]}' of list value ` +
            JSON.stringify(doc[keypart])
          );
        }
      }
    } else {
      assertIsValidFieldName(keypart);

      if (!(keypart in doc)) {
        if (options.noCreate) {
          return undefined;
        }

        if (!last) {
          doc[keypart] = {};
        }
      }
    }

    if (last) {
      return doc;
    }

    doc = doc[keypart];
  }

  // notreached
}

export { _isPlainObject, _selectorIsId, _selectorIsIdPerhapsAsObject };
