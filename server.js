import express from "express";
import WebSocket from "ws";

const app = express();
const PORT = process.env.PORT || 3000;

function frame(message) {
  const data = JSON.stringify(message);
  return `~m~${data.length}~m~${data}`;
}

function session(prefix) {
  return `${prefix}_${Math.random().toString(36).slice(2, 14)}`;
}

function getM5() {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(
      "wss://data.tradingview.com/socket.io/websocket",
      {
        headers: {
          Origin: "https://www.tradingview.com"
        }
      }
    );

    const chartSession = session("cs");
    const quoteSession = session("qs");

    const timeout = setTimeout(() => {
      ws.close();
      reject(new Error("TradingView timeout"));
    }, 15000);

    ws.on("open", () => {
      const send = (method, params) =>
        ws.send(frame({ m: method, p: params }));

      send("set_auth_token", ["unauthorized_user"]);
      send("chart_create_session", [chartSession, ""]);
      send("quote_create_session", [quoteSession]);
      send("quote_set_fields", [
        quoteSession,
        "lp",
        "ch",
        "chp",
        "short_name",
        "exchange"
      ]);
      send("quote_add_symbols", [quoteSession, "OANDA:XAUUSD"]);

      send("resolve_symbol", [
        chartSession,
        "symbol_1",
        '=symbol("OANDA:XAUUSD")'
      ]);

      // 5 = 5-minute candles, request latest 10 bars
      send("create_series", [
        chartSession,
        "s1",
        "s1",
        "symbol_1",
        "5",
        10
      ]);
    });

    ws.on("message", raw => {
      const text = raw.toString();

      // TradingView heartbeat must be echoed back.
      const heartbeats = text.match(/~m~\d+~m~(~h~\d+)/g);
      if (heartbeats) {
        for (const hb of heartbeats) ws.send(hb);
      }

      if (!text.includes("timescale_update")) return;

      const matches = [
        ...text.matchAll(/~m~\d+~m~(\{.*?\})(?=~m~|$)/gs)
      ];

      for (const match of matches) {
        try {
          const msg = JSON.parse(match[1]);
          const series = msg?.p?.[1]?.s1?.s;

          if (!Array.isArray(series) || series.length === 0) continue;

          const candles = series.map(bar => {
            const v = bar.v;
            return {
              time: new Date(v[0] * 1000).toISOString(),
              open: v[1],
              high: v[2],
              low: v[3],
              close: v[4],
              volume: v[5]
            };
          });

          clearTimeout(timeout);
          ws.close();

          resolve({
            ok: true,
            symbol: "OANDA:XAUUSD",
            timeframe: "M5",
            candles
          });

          return;
        } catch {
          // Ignore unrelated TradingView messages.
        }
      }
    });

    ws.on("error", err => {
      clearTimeout(timeout);
      reject(err);
    });
  });
}

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "xauusd-123",
    test: "/xauusd/m5"
  });
});

app.get("/xauusd/m5", async (req, res) => {
  try {
    const data = await getM5();
    res.json(data);
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: error.message
    });
  }
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`xauusd-123 running on port ${PORT}`);
});
