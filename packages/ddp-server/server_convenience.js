import { Server } from './livedata_server.js';
import { InvalidationCrossbar } from './crossbar.js';
export { DDPServer } from './livedata_server.js';

export const Meteor$server = new Server;

export const Meteor$refresh = function (notification) {
  InvalidationCrossbar.fire(notification);
};

// Proxy the public methods of Meteor.server so they can
// be called directly on Meteor.
export const Meteor$publish = (...args) => Meteor$server.publish(...args);
export const Meteor$methods = (...args) => Meteor$server.methods(...args);
export const Meteor$call = (...args) => Meteor$server.call(...args);
export const Meteor$callAsync = (...args) => Meteor$server.callAsync(...args);
export const Meteor$apply = (...args) => Meteor$server.apply(...args);
export const Meteor$applyAsync = (...args) => Meteor$server.applyAsync(...args);
export const Meteor$onConnection = (...args) => Meteor$server.onConnection(...args);
export const Meteor$onMessage = (...args) => Meteor$server.onMessage(...args);
