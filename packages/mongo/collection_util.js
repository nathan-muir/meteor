export function publishCursor(cursor, sub, collection) {
  var observeHandle = cursor.observeChanges(
    {
      added: function (id, fields) {
        sub.added(collection, id, fields);
      },
      changed: function (id, fields) {
        sub.changed(collection, id, fields);
      },
      removed: function (id) {
        sub.removed(collection, id);
      },
    },
    // Publications don't mutate the documents
    // This is tested by the `livedata - publish callbacks clone` test
    { nonMutatingCallbacks: true }
  );

  // We don't call sub.ready() here: it gets called in livedata_server, after
  // possibly calling _publishCursor on multiple returned cursors.

  // register stop callback (expects lambda w/ no args).
  sub.onStop(function () {
    observeHandle.stop();
  });

  // return the observeHandle in case it needs to be stopped early
  return observeHandle;
}

// protect against dangerous selectors.  falsey and {_id: falsey} are both
// likely programmer error, and not what you want, particularly for destructive
// operations. If a falsey _id is sent in, a new string _id will be
// generated and returned; if a fallbackId is provided, it will be returned
// instead.
export function rewriteSelector(selector, { fallbackId } = {}) {
  // shorthand -- scalars match _id
  if (LocalCollection._selectorIsId(selector)) selector = { _id: selector };

  if (Array.isArray(selector)) {
    // This is consistent with the Mongo console itself; if we don't do this
    // check passing an empty array ends up selecting all items
    throw new Error("Mongo selector can't be an array.");
  }

  if (!selector || ('_id' in selector && !selector._id)) {
    // can't match anything
    return { _id: fallbackId || Random.id() };
  }

  return selector;
}
