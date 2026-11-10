let watchedMqttTopic = 'tronsharing/v8/tronsharing_club_v8_master';
let myUserId = '';
let roomName = 'Главная кабинка';
let swSockets = [];
let seenLogIds = new Set();
let seenUserIds = new Set();

function encodeMqttVarint(len) {
  const out = [];
  do {
    let byte = len % 128;
    len = Math.floor(len / 128);
    if (len > 0) byte |= 0x80;
    out.push(byte);
  } while (len > 0);
  return out;
}

function buildMqttConnectPacket(clientId) {
  const cidBytes = new TextEncoder().encode(clientId);
  const varHeader = [0x00, 0x04, 0x4d, 0x51, 0x54, 0x54, 0x04, 0x02, 0x00, 0x3c];
  const payload = [(cidBytes.length >> 8) & 0xff, cidBytes.length & 0xff, ...cidBytes];
  const rem = varHeader.length + payload.length;
  return new Uint8Array([0x10, ...encodeMqttVarint(rem), ...varHeader, ...payload]);
}

function buildMqttSubscribePacket(topic) {
  const topBytes = new TextEncoder().encode(topic);
  const body = [0x00, 0x01, (topBytes.length >> 8) & 0xff, topBytes.length & 0xff, ...topBytes, 0x00];
  return new Uint8Array([0x82, ...encodeMqttVarint(body.length), ...body]);
}

function parseMqttIncomingPublish(arrayBuffer) {
  const buf = new Uint8Array(arrayBuffer);
  if (buf.length < 4 || (buf[0] & 0xf0) !== 0x30) return null;
  let multiplier = 1, remLen = 0, idx = 1;
  while (idx < buf.length) {
    const b = buf[idx++];
    remLen += (b & 127) * multiplier;
    if ((b & 128) === 0) break;
    multiplier *= 128;
  }
  const topicLen = (buf[idx] << 8) | buf[idx + 1];
  let payloadStart = idx + 2 + topicLen;
  if (((buf[0] >> 1) & 0x03) > 0) payloadStart += 2;
  const payloadEnd = Math.min(buf.length, idx + remLen);
  if (payloadStart >= payloadEnd) return null;
  let str = new TextDecoder().decode(buf.subarray(payloadStart, payloadEnd));
  if (str.startsWith('TS8:')) str = str.slice(4);
  return JSON.parse(str);
}

function handleIncomingPayloadInSw(data, isInitial) {
  if (!data || data.type === 'REQ_SYNC') return;
  if (Array.isArray(data.users)) {
    data.users.forEach(u => {
      if (!u || !u.id) return;
      if (!seenUserIds.has(u.id)) {
        seenUserIds.add(u.id);
        if (!isInitial && u.id !== myUserId) {
          self.registration.showNotification('👋 Новый участник в комнате!', {
            body: `${u.name} подключился к комнате «${roomName}»`,
            icon: 'icon-192.png',
            badge: 'icon-192.png',
            vibrate: [180, 80, 220],
            tag: 'sw-join-' + u.id
          }).catch(() => {});
        }
      }
    });
  }
  if (Array.isArray(data.logs)) {
    data.logs.forEach(l => {
      if (!l || !l.id) return;
      if (!seenLogIds.has(l.id)) {
        seenLogIds.add(l.id);
        if (!isInitial && l.userId !== myUserId && (Date.now() - (l.timestamp || 0) < 120000)) {
          const sender = (data.users || []).find(x => x.id === l.userId) || { name: l.userName || 'Участник' };
          self.registration.showNotification(`🚽 ${sender.name} сходил на трон!`, {
            body: `Оценка: ${l.rating || 5}★${l.note ? ' — «' + l.note + '»' : ''}`,
            icon: 'icon-192.png',
            badge: 'icon-192.png',
            vibrate: [200, 100, 250],
            tag: 'sw-log-' + l.id
          }).catch(() => {});
        }
      }
    });
  }
}

function connectSwBrokers() {
  if (!watchedMqttTopic) return;
  swSockets.forEach(w => { try { w.close(); } catch (e) {} });
  swSockets = [];
  ['wss://broker.emqx.io:8084/mqtt', 'wss://broker.hivemq.com:8884/mqtt'].forEach(url => {
    try {
      const ws = new WebSocket(url, 'mqtt');
      ws.binaryType = 'arraybuffer';
      let firstMsg = true;
      let connAcked = false;
      ws.onopen = () => {
        const cid = 'sw8_' + Math.random().toString(36).slice(2, 9);
        ws.send(buildMqttConnectPacket(cid));
      };
      ws.onmessage = (evt) => {
        try {
          const buf = new Uint8Array(evt.data);
          if (!buf || buf.length < 2) return;
          if ((buf[0] & 0xf0) === 0x20 && !connAcked) {
            connAcked = true;
            ws.send(buildMqttSubscribePacket(watchedMqttTopic));
            return;
          }
          if ((buf[0] & 0xf0) === 0x30) {
            const data = parseMqttIncomingPublish(evt.data);
            if (data) {
              handleIncomingPayloadInSw(data, firstMsg);
              firstMsg = false;
            }
          }
        } catch (e) {}
      };
      swSockets.push(ws);
    } catch (e) {}
  });
}

self.addEventListener('message', (event) => {
  const msg = event.data;
  if (msg && msg.type === 'WATCH_ROOM') {
    myUserId = msg.myUserId || myUserId;
    roomName = msg.roomName || roomName;
    if (msg.mqttTopic && msg.mqttTopic !== watchedMqttTopic) {
      watchedMqttTopic = msg.mqttTopic;
      connectSwBrokers();
    } else if (swSockets.length === 0) {
      connectSwBrokers();
    }
  }
});

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
  connectSwBrokers();
});
self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clientList) => {
      for (const client of clientList) {
        if ('focus' in client) return client.focus();
      }
      if (self.clients.openWindow) return self.clients.openWindow('./');
    })
  );
});
