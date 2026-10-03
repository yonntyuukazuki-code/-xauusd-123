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


// ======================================================
// TRADINGVIEW
// ======================================================

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

      // Heartbeat
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
          .filter(Boolean)
          .sort(
            (a, b) =>
              new Date(a.time) -
              new Date(b.time)
          );

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


// ======================================================
// INDICATORS
// ======================================================

function round(value, digits = 3) {
  if (
    value === null ||
    value === undefined ||
    !Number.isFinite(value)
  ) {
    return null;
  }

  return Number(value.toFixed(digits));
}


function ema(values, period) {
  if (values.length < period) {
    return null;
  }

  const multiplier =
    2 / (period + 1);

  let current =
    values
      .slice(0, period)
      .reduce((a, b) => a + b, 0) /
    period;

  for (
    let i = period;
    i < values.length;
    i++
  ) {
    current =
      values[i] * multiplier +
      current * (1 - multiplier);
  }

  return current;
}


function rsiWilder(values, period = 14) {
  if (values.length < period + 1) {
    return null;
  }

  let gains = 0;
  let losses = 0;

  for (
    let i = 1;
    i <= period;
    i++
  ) {
    const change =
      values[i] - values[i - 1];

    if (change >= 0) {
      gains += change;
    } else {
      losses += Math.abs(change);
    }
  }

  let avgGain =
    gains / period;

  let avgLoss =
    losses / period;

  for (
    let i = period + 1;
    i < values.length;
    i++
  ) {
    const change =
      values[i] - values[i - 1];

    const gain =
      change > 0 ? change : 0;

    const loss =
      change < 0
        ? Math.abs(change)
        : 0;

    avgGain =
      (
        avgGain * (period - 1) +
        gain
      ) / period;

    avgLoss =
      (
        avgLoss * (period - 1) +
        loss
      ) / period;
  }

  if (avgLoss === 0) {
    return 100;
  }

  const rs =
    avgGain / avgLoss;

  return (
    100 -
    100 / (1 + rs)
  );
}


function atrWilder(candles, period = 14) {
  if (candles.length < period + 1) {
    return null;
  }

  const tr = [];

  for (
    let i = 1;
    i < candles.length;
    i++
  ) {
    const current =
      candles[i];

    const previous =
      candles[i - 1];

    tr.push(
      Math.max(
        current.high - current.low,
        Math.abs(
          current.high -
          previous.close
        ),
        Math.abs(
          current.low -
          previous.close
        )
      )
    );
  }

  if (tr.length < period) {
    return null;
  }

  let atr =
    tr
      .slice(0, period)
      .reduce((a, b) => a + b, 0) /
    period;

  for (
    let i = period;
    i < tr.length;
    i++
  ) {
    atr =
      (
        atr * (period - 1) +
        tr[i]
      ) / period;
  }

  return atr;
}


// ======================================================
// RANGE HIGH / LOW
// ======================================================

function rangeStats(candles, bars) {
  const slice =
    candles.slice(-bars);

  if (slice.length === 0) {
    return null;
  }

  const highest =
    slice.reduce(
      (a, b) =>
        b.high > a.high ? b : a
    );

  const lowest =
    slice.reduce(
      (a, b) =>
        b.low < a.low ? b : a
    );

  return {
    bars: slice.length,

    high: {
      price: highest.high,
      time: highest.time
    },

    low: {
      price: lowest.low,
      time: lowest.time
    }
  };
}


// ======================================================
// SWING POINTS
// 2 candles left + 2 candles right
// ======================================================

function findSwings(candles) {
  const highs = [];
  const lows = [];

  for (
    let i = 2;
    i < candles.length - 2;
    i++
  ) {
    const c = candles[i];

    const swingHigh =
      c.high >
        candles[i - 1].high &&
      c.high >
        candles[i - 2].high &&
      c.high >=
        candles[i + 1].high &&
      c.high >=
        candles[i + 2].high;

    const swingLow =
      c.low <
        candles[i - 1].low &&
      c.low <
        candles[i - 2].low &&
      c.low <=
        candles[i + 1].low &&
      c.low <=
        candles[i + 2].low;

    if (swingHigh) {
      highs.push({
        time: c.time,
        price: c.high
      });
    }

    if (swingLow) {
      lows.push({
        time: c.time,
        price: c.low
      });
    }
  }

  return {
    highs: highs.slice(-5),
    lows: lows.slice(-5)
  };
}


// ======================================================
// FAIR VALUE GAPS
//
// Bullish FVG:
// candle 1 high < candle 3 low
//
// Bearish FVG:
// candle 1 low > candle 3 high
// ======================================================

function findFVG(candles) {
  const gaps = [];

  const start =
    Math.max(
      2,
      candles.length - 50
    );

  for (
    let i = start;
    i < candles.length;
    i++
  ) {
    const first =
      candles[i - 2];

    const third =
      candles[i];

    // Bullish FVG
    if (
      first.high <
      third.low
    ) {
      const lower =
        first.high;

      const upper =
        third.low;

      let touched = false;
      let fullyFilled = false;

      for (
        let j = i + 1;
        j < candles.length;
        j++
      ) {
        if (
          candles[j].low <= upper
        ) {
          touched = true;
        }

        if (
          candles[j].low <= lower
        ) {
          fullyFilled = true;
          break;
        }
      }

      gaps.push({
        type: "bullish",
        createdAt: third.time,
        lower: round(lower),
        upper: round(upper),
        touched,
        fullyFilled
      });
    }

    // Bearish FVG
    if (
      first.low >
      third.high
    ) {
      const lower =
        third.high;

      const upper =
        first.low;

      let touched = false;
      let fullyFilled = false;

      for (
        let j = i + 1;
        j < candles.length;
        j++
      ) {
        if (
          candles[j].high >= lower
        ) {
          touched = true;
        }

        if (
          candles[j].high >= upper
        ) {
          fullyFilled = true;
          break;
        }
      }

      gaps.push({
        type: "bearish",
        createdAt: third.time,
        lower: round(lower),
        upper: round(upper),
        touched,
        fullyFilled
      });
    }
  }

  return gaps.slice(-10);
}


// ======================================================
// TIMEFRAME ANALYSIS
// ======================================================

function analyzeCandles(candles) {
  const closes =
    candles.map(c => c.close);

  const latest =
    candles[candles.length - 1];

  const ema20 =
    ema(closes, 20);

  const ema50 =
    ema(closes, 50);

  const rsi14 =
    rsiWilder(closes, 14);

  const atr14 =
    atrWilder(candles, 14);

  const swings =
    findSwings(candles);

  const fvg =
    findFVG(candles);

  let emaBias = "neutral";

  if (
    latest.close > ema20 &&
    ema20 > ema50
  ) {
    emaBias = "bullish";
  }

  if (
    latest.close < ema20 &&
    ema20 < ema50
  ) {
    emaBias = "bearish";
  }

  return {
    candleCount:
      candles.length,

    latest: {
      time: latest.time,
      open: latest.open,
      high: latest.high,
      low: latest.low,
      close: latest.close,
      volume: latest.volume
    },

    indicators: {
      ema20: round(ema20),
      ema50: round(ema50),
      rsi14: round(rsi14, 2),
      atr14: round(atr14)
    },

    emaBias,

    ranges: {
      last20:
        rangeStats(candles, 20),

      last50:
        rangeStats(candles, 50)
    },

    swings,

    fvg
  };
}


// ======================================================
// FETCH ALL SEQUENTIALLY
// ======================================================

async function getAllTimeframes() {
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

  return {
    M5: m5,
    M15: m15,
    H1: h1,
    H4: h4,
    D1: d1
  };
}


// ======================================================
// ROOT
// ======================================================

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
      ALL: "/xauusd/all",
      ANALYSIS: "/xauusd/analysis"
    }
  });
});


// ======================================================
// ANALYSIS
// Must be above /xauusd/:timeframe
// ======================================================

app.get(
  "/xauusd/analysis",
  async (req, res) => {
    try {
      const startedAt =
        Date.now();

      const all =
        await getAllTimeframes();

      const analysis = {};

      for (
        const [key, value]
        of Object.entries(all)
      ) {
        analysis[key] =
          analyzeCandles(
            value.candles
          );
      }

      res.set(
        "Cache-Control",
        "no-store"
      );

      res.json({
        ok: true,
        symbol: SYMBOL,

        generatedAt:
          new Date().toISOString(),

        durationMs:
          Date.now() - startedAt,

        analysis
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
  }
);


// ======================================================
// ALL RAW OHLC
// ======================================================

app.get(
  "/xauusd/all",
  async (req, res) => {
    try {
      const startedAt =
        Date.now();

      const all =
        await getAllTimeframes();

      res.set(
        "Cache-Control",
        "no-store"
      );

      res.json({
        ok: true,
        symbol: SYMBOL,

        generatedAt:
          new Date().toISOString(),

        durationMs:
          Date.now() - startedAt,

        counts: {
          M5:
            all.M5.candles.length,

          M15:
            all.M15.candles.length,

          H1:
            all.H1.candles.length,

          H4:
            all.H4.candles.length,

          D1:
            all.D1.candles.length
        },

        data: {
          M5:
            all.M5.candles,

          M15:
            all.M15.candles,

          H1:
            all.H1.candles,

          H4:
            all.H4.candles,

          D1:
            all.D1.candles
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
  }
);


// ======================================================
// SINGLE TIMEFRAME
// ======================================================

app.get(
  "/xauusd/:timeframe",
  async (req, res) => {
    try {
      const timeframe =
        req.params.timeframe
          .toLowerCase();

      if (
        !TIMEFRAMES[timeframe]
      ) {
        return res
          .status(400)
          .json({
            ok: false,
            error:
              "Unsupported timeframe",

            supported:
              Object.keys(
                TIMEFRAMES
              )
          });
      }

      const data =
        await getCandles(
          timeframe
        );

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


// ======================================================
// SERVER
// ======================================================

app.listen(
  PORT,
  "0.0.0.0",
  () => {
    console.log(
      `xauusd-123 running on port ${PORT}`
    );
  }
);
