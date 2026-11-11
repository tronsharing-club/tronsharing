let watchedMqttTopic = 'tronsharing/v11/club/#';
let myUserId = '';
let myUserName = '';
let roomName = 'Главная кабинка';
let swSockets = [];
let seenLogIds = new Set();
let seenUserNames = new Set();
let swPingTimer = null;

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

function parseMqttPacketsFromBuffer(arrayBuffer) {
  const buf = new Uint8Array(arrayBuffer);
  const results = [];
  let offset = 0;
  while (offset < buf.length) {
    if (offset + 2 > buf.length) break;
    const headerByte = buf[offset];
    const pktType = headerByte & 0xf0;
    let multiplier = 1, remLen = 0, idx = offset + 1;
    while (idx < buf.length) {
      const b = buf[idx++];
      remLen += (b & 127) * multiplier;
      if ((b & 128) === 0) break;
      multiplier *= 128;
    }
    const pktEnd = idx + remLen;
    if (pktEnd > buf.length) break;
    if (pktType === 0x20) {
      results.push({ type: 'CONNACK' });
    } else if (pktType === 0x30 && remLen >= 2) {
      const topicLen = (buf[idx] << 8) | buf[idx + 1];
      let payloadStart = idx + 2 + topicLen;
      const qos = (headerByte >> 1) & 0x03;
      if (qos > 0) payloadStart += 2;
      if (payloadStart < pktEnd) {
        let str = new TextDecoder().decode(buf.subarray(payloadStart, pktEnd));
        if (str.startsWith('TS10:')) str = str.slice(5);
        else if (str.startsWith('TS8:')) str = str.slice(4);
        try {
          results.push({ type: 'PUBLISH', data: JSON.parse(str) });
        } catch (e) {}
      }
    }
    offset = pktEnd;
  }
  return results;
}

function handleIncomingPayloadInSw(data, isInitialGrace) {
  if (!data || data.type === 'REQ_SYNC') return;
  const myNameLower = (myUserName || '').trim().toLowerCase();

  const usersList = [];
  if (data.singleUser) usersList.push(data.singleUser);
  if (Array.isArray(data.users)) data.users.forEach(u => usersList.push(u));

  usersList.forEach(u => {
    if (!u || !u.name || u.name === 'Игрок' || u.name === 'Капитан') return;
    const uLower = u.name.trim().toLowerCase();
    if (!seenUserNames.has(uLower)) {
      seenUserNames.add(uLower);
      if (!isInitialGrace && uLower !== myNameLower && u.id !== myUserId) {
        self.registration.showNotification('👋 Новый участник в комнате!', {
          body: `${u.avatar || '😎'} ${u.name} подключился к комнате «${roomName}»`,
          icon: 'icon-192.png',
          badge: 'icon-192.png',
          vibrate: [180, 80, 220],
          tag: 'sw-join-' + uLower,
          renotify: true
        }).catch(() => {});
      }
    }
  });

  const logsList = [];
  if (data.singleLog) logsList.push(data.singleLog);
  if (Array.isArray(data.logs)) data.logs.forEach(l => logsList.push(l));

  logsList.forEach(l => {
    if (!l || !l.id) return;
    if (!seenLogIds.has(l.id)) {
      seenLogIds.add(l.id);
      const lNameLower = (l.userName || '').trim().toLowerCase();
      const isMine = (myNameLower && lNameLower === myNameLower) || (myUserId && l.userId === myUserId);
      const isRecent = (Date.now() - (Number(l.timestamp) || 0)) < 300000;
      if (!isInitialGrace && !isMine && isRecent) {
        const senderName = l.userName || 'Участник';
        const senderAvatar = l.userAvatar || '🚽';
        self.registration.showNotification(`🚽 ${senderAvatar} ${senderName} сходил на трон!`, {
          body: `Оценка: ${l.rating || 5}★ (+${l.xp || 35} XP)${l.note ? ' — «' + l.note + '»' : ''}`,
          icon: 'icon-192.png',
          badge: 'icon-192.png',
          vibrate: [200, 100, 250],
          tag: 'sw-log-' + l.id,
          renotify: true
        }).catch(() => {});
      }
    }
  });
}

function connectSwBrokers() {
  if (!watchedMqttTopic) return;
  swSockets = swSockets.filter(w => w && w.readyState <= 1);
  const openUrls = new Set(swSockets.map(w => w._url));

  ['wss://broker.emqx.io:8084/mqtt', 'wss://broker.hivemq.com:8884/mqtt', 'wss://mqtt.eclipseprojects.io:443/mqtt'].forEach(url => {
    if (openUrls.has(url)) return;
    try {
      const ws = new WebSocket(url, 'mqtt');
      ws._url = url;
      ws.binaryType = 'arraybuffer';
      let connAcked = false;
      let connectedAt = 0;
      ws.onopen = () => {
        const cid = 'sw21_' + Math.random().toString(36).slice(2, 9);
        ws.send(buildMqttConnectPacket(cid));
      };
      ws.onmessage = (evt) => {
        try {
          const pkts = parseMqttPacketsFromBuffer(evt.data);
          pkts.forEach(pkt => {
            if (pkt.type === 'CONNACK' && !connAcked) {
              connAcked = true;
              connectedAt = Date.now();
              ws._connAcked = true;
              ws.send(buildMqttSubscribePacket(watchedMqttTopic));
            } else if (pkt.type === 'PUBLISH' && pkt.data) {
              const inGrace = (Date.now() - connectedAt) < 3500;
              handleIncomingPayloadInSw(pkt.data, inGrace);
            }
          });
        } catch (e) {}
      };
      swSockets.push(ws);
    } catch (e) {}
  });

  if (!swPingTimer) {
    swPingTimer = setInterval(() => {
      const ping = new Uint8Array([0xc0, 0x00]);
      swSockets.forEach(ws => {
        if (ws && ws.readyState === 1 && ws._connAcked) {
          try { ws.send(ping); } catch (e) {}
        }
      });
    }, 20000);
  }
}

self.addEventListener('message', (event) => {
  const msg = event.data;
  if (msg && msg.type === 'WATCH_ROOM') {
    myUserId = msg.myUserId || myUserId;
    myUserName = msg.myUserName || myUserName;
    roomName = msg.roomName || roomName;
    if (Array.isArray(msg.knownLogIds)) {
      msg.knownLogIds.forEach(id => seenLogIds.add(id));
    }
    if (Array.isArray(msg.knownUserNames)) {
      msg.knownUserNames.forEach(n => seenUserNames.add(String(n || '').trim().toLowerCase()));
    }
    if (msg.mqttTopic) watchedMqttTopic = msg.mqttTopic;
    connectSwBrokers();
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
