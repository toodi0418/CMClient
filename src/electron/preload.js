'use strict';

const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('meshtastic', {
  connect: (options) => ipcRenderer.invoke('meshtastic:connect', options),
  disconnect: () => ipcRenderer.invoke('meshtastic:disconnect'),
  discover: (options) => ipcRenderer.invoke('meshtastic:discover', options),
  saveCallmeshKey: (key) => ipcRenderer.invoke('callmesh:save-key', key),
  getCallMeshStatus: () => ipcRenderer.invoke('callmesh:get-status'),
  onSummary: (callback) => {
    const listener = (_event, data) => callback(data);
    ipcRenderer.on('meshtastic:summary', listener);
    return () => ipcRenderer.removeListener('meshtastic:summary', listener);
  },
  onFromRadio: (callback) => {
    const listener = (_event, data) => callback(data);
    ipcRenderer.on('meshtastic:fromRadio', listener);
    return () => ipcRenderer.removeListener('meshtastic:fromRadio', listener);
  },
  onStatus: (callback) => {
    const listener = (_event, data) => callback(data);
    ipcRenderer.on('meshtastic:status', listener);
    return () => ipcRenderer.removeListener('meshtastic:status', listener);
  },
  onCallMeshStatus: (callback) => {
    const listener = (_event, data) => callback(data);
    ipcRenderer.on('callmesh:status', listener);
    return () => ipcRenderer.removeListener('callmesh:status', listener);
  },
  onMyInfo: (callback) => {
    const listener = (_event, data) => callback(data);
    ipcRenderer.on('meshtastic:myInfo', listener);
    return () => ipcRenderer.removeListener('meshtastic:myInfo', listener);
  },
  onCallMeshLog: (callback) => {
    const listener = (_event, data) => callback(data);
    ipcRenderer.on('callmesh:log', listener);
    return () => ipcRenderer.removeListener('callmesh:log', listener);
  },
  getAppInfo: () => ipcRenderer.invoke('app:get-info'),
  getClientPreferences: () => ipcRenderer.invoke('app:get-preferences'),
  updateClientPreferences: (preferences) => ipcRenderer.invoke('app:update-preferences', preferences),
  resetCallMeshData: () => ipcRenderer.invoke('callmesh:reset'),
  setAprsServer: (server) => ipcRenderer.invoke('aprs:set-server', server),
  setAprsBeaconInterval: (minutes) => ipcRenderer.invoke('aprs:set-beacon-interval', minutes),
  shouldAutoValidateKey: () => ipcRenderer.invoke('callmesh:should-auto-validate'),
  onAprsUplink: (callback) => {
    const listener = (_event, data) => callback(data);
    ipcRenderer.on('meshtastic:aprs-uplink', listener);
    return () => ipcRenderer.removeListener('meshtastic:aprs-uplink', listener);
  }
});
