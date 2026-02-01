'use strict';

const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('tmagStatus', {
  get: () => ipcRenderer.invoke('status:get'),
  getSelf: () => ipcRenderer.invoke('status:get-self'),
  openSettings: () => ipcRenderer.invoke('status:open-settings'),
  openWeb: () => ipcRenderer.invoke('status:open-web'),
  minimize: () => ipcRenderer.invoke('status:window-minimize'),
  close: () => ipcRenderer.invoke('status:window-close'),
  resize: (height) => ipcRenderer.invoke('status:resize', { height }),
  onRx: (handler) => {
    if (typeof handler !== 'function') return () => {};
    const listener = (_event, payload) => handler(payload);
    ipcRenderer.on('status:rx', listener);
    return () => ipcRenderer.removeListener('status:rx', listener);
  },
  onAprs: (handler) => {
    if (typeof handler !== 'function') return () => {};
    const listener = (_event, payload) => handler(payload);
    ipcRenderer.on('status:aprs-rx', listener);
    return () => ipcRenderer.removeListener('status:aprs-rx', listener);
  },
  onMessage: (handler) => {
    if (typeof handler !== 'function') return () => {};
    const listener = (_event, message) => handler(message);
    ipcRenderer.on('status:message', listener);
    return () => ipcRenderer.removeListener('status:message', listener);
  }
});
