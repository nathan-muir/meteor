import Matcher from './matcher.js';
import Sorter from './sorter.js';
import LocalCollection_ from "./local_collection.js";
import Cursor from './cursor.js';
import ObserveHandle from "./observe_handle.js";
import * as LCS from './local_collection_static.js';
import * as _f from "./field-helpers.js";

Object.assign(LocalCollection_, {
    Cursor,
    ObserveHandle,
    ...LCS,
    _f: {..._f},
});

export const LocalCollection = LocalCollection_;
export const Minimongo = {
    LocalCollection,
    Matcher,
    Sorter
};
