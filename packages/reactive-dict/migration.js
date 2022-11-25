import { Reload } from 'meteor/reload';
import { ReactiveDict } from './reactive-dict';

const hasOwn = Object.prototype.hasOwnProperty;

ReactiveDict._migratedDictData = {}; // name -> data
ReactiveDict._dictsToMigrate = {}; // name -> ReactiveDict

ReactiveDict._loadMigratedDict = function (dictName) {
  if (hasOwn.call(ReactiveDict._migratedDictData, dictName)) {
    const data = ReactiveDict._migratedDictData[dictName];
    delete ReactiveDict._migratedDictData[dictName];
    return data;
  }

  return null;
};

ReactiveDict._registerDictForMigrate = function (dictName, dict) {
  if (hasOwn.call(ReactiveDict._dictsToMigrate, dictName))
    throw new Error("Duplicate ReactiveDict name: " + dictName);

  ReactiveDict._dictsToMigrate[dictName] = dict;
};

// Put old migrated data into ReactiveDict._migratedDictData,
// where it can be accessed by ReactiveDict._loadMigratedDict.
var migrationData = Reload._migrationData('reactive-dict');
if (migrationData && migrationData.dicts)
  ReactiveDict._migratedDictData = migrationData.dicts;

// On migration, assemble the data from all the dicts that have been
// registered.
Reload._onMigrate('reactive-dict', function () {
  var dictsToMigrate = ReactiveDict._dictsToMigrate;
  var dataToMigrate = {};

  for (var dictName in dictsToMigrate)
    dataToMigrate[dictName] = dictsToMigrate[dictName]._getMigrationData();

  return [true, {dicts: dataToMigrate}];
});

export { ReactiveDict };
