function parseMeteorSettings() {
  let settings = {};

  if (process.env.METEOR_SETTINGS) {
    try {
      settings = JSON.parse(process.env.METEOR_SETTINGS);
    } catch (e) {
      throw new Error("METEOR_SETTINGS are not valid JSON.");
    }
  }

  // Make sure that there is always a public attribute
  // to enable Meteor.settings.public on client
  if (! settings.public) {
      settings.public = {};
  }
  return settings
}

export const Meteor$settings = parseMeteorSettings();
