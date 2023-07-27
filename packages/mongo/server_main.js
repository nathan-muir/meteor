import MongoDB from "mongodb"
import packageJson from "mongodb/package.json";
import { setConnectionOptions } from "./connection_options.js";
import { Collection } from "./collection.js";
import { defaultRemoteCollectionDriver, RemoteCollectionDriver } from "./remote_collection_driver.js";


export const Mongo = /*#__PURE__*/Object.freeze({
  Collection,
  Cursor: LocalCollection.Cursor,
  ObjectID: MongoID.ObjectID,
  setConnectionOptions,
});


export const MongoInternals = /*#__PURE__*/Object.freeze({
  defaultRemoteCollectionDriver,
  RemoteCollectionDriver,
  NpmModules: {
    mongodb: {
      version: packageJson.version,
      module: MongoDB,
    },
  },
});
