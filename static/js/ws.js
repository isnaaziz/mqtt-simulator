let _socket = null;
let _onMessage = null;
let _onConnChange = null;
let _retry = 0;

export function send(obj) {
  if (_socket && _socket.readyState === WebSocket.OPEN) {
    _socket.send(JSON.stringify(obj));
  }
}

export function connect(onMessage, onConnChange) {
  _onMessage = onMessage;
  _onConnChange = onConnChange;
  _dial();
}

function _dial() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  _socket = new WebSocket(`${proto}://${location.host}/ws`);
  _socket.onopen = () => { _retry = 0; _onConnChange && _onConnChange(true); };
  _socket.onclose = () => {
    _onConnChange && _onConnChange(false);
    _retry = Math.min(_retry + 1, 6);
    setTimeout(_dial, 500 * _retry);
  };
  _socket.onerror = () => _socket.close();
  _socket.onmessage = (e) => {
    try {
      const m = JSON.parse(e.data);
      if (m.type === "update") _onMessage && _onMessage(m);
    } catch {}
  };
}
