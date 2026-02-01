'use strict';

const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('tmagSettings', {
  get: () => ipcRenderer.invoke('settings:get'),
  save: (payload) => ipcRenderer.invoke('settings:save', payload),
  restartBackend: () => ipcRenderer.invoke('settings:restart-backend'),
  openWeb: () => ipcRenderer.invoke('settings:open-web'),
  listSerialPorts: () => ipcRenderer.invoke('settings:list-serial'),
  discover: (options) => ipcRenderer.invoke('settings:discover', options),
  resetData: () => ipcRenderer.invoke('settings:reset-data')
});

contextBridge.exposeInMainWorld('tmagSettingsEvents', {
  onStatus: (handler) => {
    if (typeof handler !== 'function') return () => {};
    const listener = (_event, message) => handler(message);
    ipcRenderer.on('settings:status', listener);
    return () => ipcRenderer.removeListener('settings:status', listener);
  }
});
