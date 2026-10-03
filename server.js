import express from "express";
import WebSocket from "ws";

const app = express();
const PORT = process.env.PORT || 3000;

const SYMBOL = "OANDA:XAUUSD";

const TIMEFRAMES = {
  m5:  { interval: "5",   label: "M5",  bars: 100 },
  m15: { interval: "15",  label: "M15", bars: 100 },
  h1:  { interval: "60",  label: "H1",  bars: 100 },
  h4:  { interval: "240", label: "H4",  bars: 100 },
  d1:  { interval: "1D",  label: "D1",  bars: 100 }
};

function frame(message) {
  const data = JSON.stringify(message);
  return `~m~${data.length}~m~${data}`;
}

function session(prefix) {
  return `${prefix}_${Math.random()
    .toString(36)
    .slice(2, 14)}`;
}

function getCandles(timeframe) {
  return new Promise((resolve, reject) => {
    const config = TIMEFRAMES[timeframe];

    if (!config) {
      reject(new Error("Unsupported timeframe"));
      return;
    }

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

    const timeout = setTimeout(() => {
      finishError(
        new Error(
          `TradingView timeout (${config.label})`
        )
      );
    }, 20000);

    function finishError(error) {
      if (finished) return;

      finished = true;
      clearTimeout(timeout);

      try {
        ws.close();
      } catch {}

      reject(error);
    }

    function finishSuccess(data) {
      if (finished) return;

      finished = true;
      clearTimeout(timeout);

      try {
        ws.close();
      } catch {}

      resolve(data);
    }

    ws.on("open", () => {
      const send = (method, params) => {
        ws.send(
          frame({
            m: method,
            p: params
          })
        );
      };

      send("set_auth_token", [
        "unauthorized_user_token"
      ]);

      send("chart_create_session", [
        chartSession,
        ""
      ]);

      send("quote_create_session", [
        quoteSession
      ]);

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
        SYMBOL,
        {
          flags: ["force_permission"]
        }
      ]);

      const symbolDescriptor =
        "=" +
        JSON.stringify({
          symbol: SYMBOL,
          adjustment: "splits"
        });

      send("resolve_symbol", [
        chartSession,
        "symbol_1",
        symbolDescriptor
      ]);

      send("create_series", [
        chartSession,
        "s1",
        "s1",
        "symbol_1",
        config.interval,
        config.bars
      ]);
    });

    ws.on("message", raw => {
      const text = raw.toString();

      // TradingView heartbeat
      const heartbeatRegex =
        /~m~\d+~m~(~h~\d+)/g;

      let heartbeat;

      while (
        (heartbeat =
          heartbeatRegex.exec(text)) !== null
      ) {
        try {
          ws.send(
            `~m~${heartbeat[1].length}~m~${heartbeat[1]}`
          );
        } catch {}
      }

      // TradingView frames
      const parts =
        text.split(/~m~\d+~m~/);

      for (const part of parts) {
        if (
          !part ||
          !part.startsWith("{")
        ) {
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

        if (
          msg.m !== "timescale_update"
        ) {
          continue;
        }

        const payload =
          msg?.p?.[1];

        if (!payload) continue;

        const series =
          payload?.s1?.s ||
          Object.values(payload).find(
            value =>
              Array.isArray(value?.s)
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

            if (
              !Array.isArray(v) ||
              v.length < 5
            ) {
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

        finishSuccess({
          ok: true,
          symbol: SYMBOL,
          timeframe: config.label,
          interval: config.interval,
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


// ==========================================
// ROOT
// ==========================================

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "xauusd-123",
    symbol: SYMBOL,

    endpoints: {
      M5: "/xauusd/m5",
      M15: "/xauusd/m15",
      H1: "/xauusd/h1",
      H4: "/xauusd/h4",
      D1: "/xauusd/d1",
      ALL: "/xauusd/all"
    }
  });
});


// ==========================================
// ALL TIMEFRAMES
// Sequential to avoid 5 simultaneous
// TradingView WebSocket connections
// ==========================================

app.get("/xauusd/all", async (req, res) => {
  try {
    const startedAt = Date.now();

    const m5 =
      await getCandles("m5");

    const m15 =
      await getCandles("m15");

    const h1 =
      await getCandles("h1");

    const h4 =
      await getCandles("h4");

    const d1 =
      await getCandles("d1");

    res.set("Cache-Control", "no-store");

    res.json({
      ok: true,
      symbol: SYMBOL,

      generatedAt:
        new Date().toISOString(),

      durationMs:
        Date.now() - startedAt,

      counts: {
        M5: m5.candles.length,
        M15: m15.candles.length,
        H1: h1.candles.length,
        H4: h4.candles.length,
        D1: d1.candles.length
      },

      data: {
        M5: m5.candles,
        M15: m15.candles,
        H1: h1.candles,
        H4: h4.candles,
        D1: d1.candles
      }
    });

  } catch (error) {
    console.error(error);

    res.status(500).json({
      ok: false,
      error:
        error?.message ||
        String(error)
    });
  }
});


// ==========================================
// SINGLE TIMEFRAME
// Keep this AFTER /xauusd/all
// ==========================================

app.get(
  "/xauusd/:timeframe",
  async (req, res) => {
    try {
      const timeframe =
        req.params.timeframe.toLowerCase();

      if (!TIMEFRAMES[timeframe]) {
        return res
          .status(400)
          .json({
            ok: false,
            error:
              "Unsupported timeframe",

            supported:
              Object.keys(TIMEFRAMES)
          });
      }

      const data =
        await getCandles(timeframe);

      res.set(
        "Cache-Control",
        "no-store"
      );

      res.json(data);

    } catch (error) {
      console.error(error);

      res.status(500).json({
        ok: false,
        error:
          error?.message ||
          String(error)
      });
    }
  }
);


// ==========================================
// SERVER
// ==========================================

app.listen(
  PORT,
  "0.0.0.0",
  () => {
    console.log(
      `xauusd-123 running on port ${PORT}`
    );
  }
);
