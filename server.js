import express from "express";
import WebSocket from "ws";

const app = express();
const PORT = process.env.PORT || 3000;

const SYMBOL = "OANDA:XAUUSD";

function frame(message) {
  const data = JSON.stringify(message);
  return `~m~${data.length}~m~${data}`;
}

function session(prefix) {
  return `${prefix}_${Math.random().toString(36).slice(2, 14)}`;
}

function getFrames(text) {
  const frames = [];
  let pos = 0;

  while (pos < text.length) {
    if (!text.startsWith("~m~", pos)) {
      pos++;
      continue;
    }

    const lenStart = pos + 3;
    const lenEnd = text.indexOf("~m~", lenStart);

    if (lenEnd === -1) break;

    const length = Number(text.slice(lenStart, lenEnd));

    if (!Number.isFinite(length)) {
      pos++;
      continue;
    }

    const dataStart = lenEnd + 3;
    const dataEnd = dataStart + length;

    if (dataEnd > text.length) break;

    frames.push(text.slice(dataStart, dataEnd));
    pos = dataEnd;
  }

  return frames;
}

function getM5() {
  return new Promise((resolve, reject) => {
    let finished = false;

    const ws = new WebSocket(
      "wss://data.tradingview.com/socket.io/websocket",
      {
        headers: {
          Origin: "https://www.tradingview.com",
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/129 Safari/537.36"
        },
        handshakeTimeout: 15000
      }
    );

    const chartSession = session("cs");
    const quoteSession = session("qs");

    const finishError = error => {
      if (finished) return;
      finished = true;
      clearTimeout(timeout);

      try {
        ws.close();
      } catch {}

      reject(error);
    };

    const finishSuccess = data => {
      if (finished) return;
      finished = true;
      clearTimeout(timeout);

      try {
        ws.close();
      } catch {}

      resolve(data);
    };

    const timeout = setTimeout(() => {
      finishError(new Error("TradingView timeout"));
    }, 30000);

    ws.on("open", () => {
      const send = (method, params) => {
        ws.send(
          frame({
            m: method,
            p: params
          })
        );
      };

      send("set_auth_token", ["unauthorized_user"]);

      send("chart_create_session", [
        chartSession,
        ""
      ]);

      send("quote_create_session", [
        quoteSession
      ]);

      send("quote_set_fields", [
        quoteSession,
        "ch",
        "chp",
        "current_session",
        "description",
        "exchange",
        "lp",
        "lp_time",
        "minmov",
        "minmove2",
        "pricescale",
        "pro_name",
        "short_name",
        "type"
      ]);

      send("quote_add_symbols", [
        quoteSession,
        SYMBOL,
        { flags: ["force_permission"] }
      ]);

      const symbolConfig =
        `={"symbol":"${SYMBOL}","adjustment":"splits","session":"regular"}`;

      send("resolve_symbol", [
        chartSession,
        "symbol_1",
        symbolConfig
      ]);

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

      for (const payload of getFrames(text)) {
        // TradingView heartbeat
        if (payload.startsWith("~h~")) {
          try {
            ws.send(`~m~${payload.length}~m~${payload}`);
          } catch {}

          continue;
        }

        let msg;

        try {
          msg = JSON.parse(payload);
        } catch {
          continue;
        }

        if (msg?.m === "critical_error") {
          finishError(
            new Error(
              `TradingView critical_error: ${JSON.stringify(msg.p)}`
            )
          );
          return;
        }

        if (msg?.m !== "timescale_update") continue;

        const update = msg?.p?.[1];

        if (!update || typeof update !== "object") continue;

        const series =
          update?.s1?.s ??
          Object.values(update).find(
            value => Array.isArray(value?.s)
          )?.s;

        if (!Array.isArray(series) || series.length === 0) {
          continue;
        }

        const candles = series
          .map(bar => {
            const v = bar?.v;

            if (!Array.isArray(v) || v.length < 5) {
              return null;
            }

            return {
              time: new Date(Number(v[0]) * 1000).toISOString(),
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
          .filter(Boolean)
          .sort(
            (a, b) =>
              new Date(a.time).getTime() -
              new Date(b.time).getTime()
          );

        if (candles.length === 0) continue;

        finishSuccess({
          ok: true,
          symbol: SYMBOL,
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

    ws.on("unexpected-response", (request, response) => {
      finishError(
        new Error(
          `TradingView HTTP ${response.statusCode}`
        )
      );
    });

    ws.on("close", (code, reason) => {
      if (!finished) {
        finishError(
          new Error(
            `TradingView socket closed (${code}) ${reason.toString()}`
          )
        );
      }
    });
  });
}

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "xauusd-123",
    symbol: SYMBOL,
    test: "/xauusd/m5"
  });
});

app.get("/xauusd/m5", async (req, res) => {
  try {
    const data = await getM5();

    res.set("Cache-Control", "no-store");
    res.json(data);
  } catch (error) {
    console.error(error);

    res.status(500).json({
      ok: false,
      error: error?.message || String(error)
    });
  }
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(
    `xauusd-123 running on port ${PORT}`
  );
});
