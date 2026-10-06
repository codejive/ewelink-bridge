'use strict';

const sanitizeTopicToken = value => String(value).replace(/[+#/\u0000]/g, '_');
const sanitizeSubjectToken = value => String(value).replace(/[.\s*>\u0000]/g, '_') || '_';

function devicePublications(action) {
  return Object.entries(action.params).map(([key, value]) => ({
    deviceId: String(action.deviceid), key,
    payload: value == null ? '' : typeof value === 'object' ? JSON.stringify(value) : String(value)
  }));
}

function mqttTopic(publication) {
  return `${sanitizeTopicToken(publication.deviceId)}/state/${sanitizeTopicToken(publication.key)}`;
}

function natsSubject(publication) {
  return `${sanitizeSubjectToken(publication.deviceId)}.state.${sanitizeSubjectToken(publication.key)}`;
}

module.exports = { devicePublications, mqttTopic, natsSubject };
