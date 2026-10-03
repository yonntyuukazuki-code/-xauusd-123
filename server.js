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
          Origin: "https://www.tradingview.com",
          "User-Agent": "Mozilla/5.0"
        }
      }
    );

    const chartSession = session("cs");
    const quoteSession = session("qs");

    let finished = false;

    const finishError = error => {
      if (finished) return;
      finished = true;
      clearTimeout(timeout);

      try {
        ws.close();
      } catch {}

      reject(error);
    };

    const timeout = setTimeout(() => {
      finishError(new Error("TradingView timeout"));
    }, 20000);

    ws.on("open", () => {
      const send = (method, params) => {
        ws.send(
          frame({
            m: method,
            p: params
          })
        );
      };

      // Anonymous TradingView authentication
      send("set_auth_token", [
        "unauthorized_user_token"
      ]);

      // Sessions
      send("chart_create_session", [
        chartSession,
        ""
      ]);

      send("quote_create_session", [
        quoteSession
      ]);

      // Quote fields
      send("quote_set_fields", [
        quoteSession,
        "lp",
        "ch",
        "chp",
        "short_name",
        "exchange"
      ]);

      send("quote_add_symbols", [
        quoteSession,
        "OANDA:XAUUSD",
        {
          flags: ["force_permission"]
        }
      ]);

      // TradingView symbol descriptor
      const symbolDescriptor =
        "=" +
        JSON.stringify({
          symbol: "OANDA:XAUUSD",
          adjustment: "splits"
        });

      send("resolve_symbol", [
        chartSession,
        "symbol_1",
        symbolDescriptor
      ]);

      // Latest 10 M5 candles
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

      // Echo TradingView heartbeat
      const heartbeatRegex =
        /~m~\d+~m~(~h~\d+)/g;

      let heartbeat;

      while (
        (heartbeat = heartbeatRegex.exec(text)) !== null
      ) {
        ws.send(
          `~m~${heartbeat[1].length}~m~${heartbeat[1]}`
        );
      }

      // Split TradingView frames
      const parts = text.split(/~m~\d+~m~/);

      for (const part of parts) {
        if (!part || !part.startsWith("{")) {
          continue;
        }

        let msg;

        try {
          msg = JSON.parse(part);
        } catch {
          continue;
        }

        if (msg.m === "symbol_error") {
          finishError(
            new Error(
              `TradingView symbol error: ${JSON.stringify(
                msg.p
              )}`
            )
          );
          return;
        }

        if (msg.m !== "timescale_update") {
          continue;
        }

        const payload = msg?.p?.[1];

        if (!payload) continue;

        const series =
          payload?.s1?.s ||
          Object.values(payload).find(
            value => Array.isArray(value?.s)
          )?.s;

        if (
          !Array.isArray(series) ||
          series.length === 0
        ) {
          continue;
        }

        const candles = series
          .map(bar => {
            const v = bar?.v;

            if (!Array.isArray(v)) {
              return null;
            }

            return {
              time: new Date(
                Number(v[0]) * 1000
              ).toISOString(),

              open: Number(v[1]),
              high: Number(v[2]),
              low: Number(v[3]),
              close: Number(v[4]),

              volume:
                v[5] == null
                  ? null
                  : Number(v[5])
            };
          })
          .filter(Boolean);

        if (candles.length === 0) {
          continue;
        }

        if (finished) return;

        finished = true;

        clearTimeout(timeout);

        try {
          ws.close();
        } catch {}

        resolve({
          ok: true,
          symbol: "OANDA:XAUUSD",
          timeframe: "M5",
          count: candles.length,
          candles
        });

        return;
      }
    });

    ws.on("error", error => {
      finishError(error);
    });

    ws.on("close", (code, reason) => {
      if (finished) return;

      finishError(
        new Error(
          `TradingView socket closed (${code}) ${
            reason?.toString() || ""
          }`
        )
      );
    });
  });
}

// Health check
app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "xauusd-123",
    symbol: "OANDA:XAUUSD",
    endpoint: "/xauusd/m5"
  });
});

// XAUUSD M5
app.get("/xauusd/m5", async (req, res) => {
  try {
    const data = await getM5();

    res.json(data);
  } catch (error) {
    console.error(error);

    res.status(500).json({
      ok: false,
      error: error.message
    });
  }
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(
    `xauusd-123 running on port ${PORT}`
  );
});
