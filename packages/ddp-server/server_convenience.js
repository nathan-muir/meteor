import { Server } from './livedata_server.js';
import { InvalidationCrossbar } from './crossbar.js';
export { DDPServer } from './livedata_server.js';

if (process.env.DDP_DEFAULT_CONNECTION_URL) {
  __meteor_runtime_config__.DDP_DEFAULT_CONNECTION_URL =
    process.env.DDP_DEFAULT_CONNECTION_URL;
}

export const Meteor$server = new Server;

export const Meteor$refresh = function (notification) {
  InvalidationCrossbar.fire(notification);
};

// Proxy the public methods of Meteor.server so they can
// be called directly on Meteor.
export const Meteor$publish = Meteor$server.publish.bind(Meteor$server);
export const Meteor$methods = Meteor$server.methods.bind(Meteor$server);
export const Meteor$call = Meteor$server.call.bind(Meteor$server);
export const Meteor$callAsync = Meteor$server.callAsync.bind(Meteor$server);
export const Meteor$apply = Meteor$server.apply.bind(Meteor$server);
export const Meteor$applyAsync = Meteor$server.applyAsync.bind(Meteor$server);
export const Meteor$onConnection = Meteor$server.onConnection.bind(Meteor$server);
export const Meteor$onMessage = Meteor$server.onMessage.bind(Meteor$server);

