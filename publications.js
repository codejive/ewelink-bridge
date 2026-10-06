'use strict';

const sanitize = value => String(value).replace(/[+#/\u0000]/g, '_');

function devicePublications(action) {
  const base = `${sanitize(action.deviceid)}/state`;
  return Object.entries(action.params).map(([key, value]) => ({
    topic: `${base}/${sanitize(key)}`,
    payload: value == null ? '' : typeof value === 'object' ? JSON.stringify(value) : String(value)
  }));
}

module.exports = { devicePublications };
