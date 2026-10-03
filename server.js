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
// HELPERS
// ======================================================

function round(value, digits = 3) {
  if (!Number.isFinite(value)) return null;
  return Number(value.toFixed(digits));
}

function frame(message) {
  const data = JSON.stringify(message);
  return `~m~${data.length}~m~${data}`;
}

function session(prefix) {
  return `${prefix}_${Math.random()
    .toString(36)
    .slice(2, 14)}`;
}


// ======================================================
// TRADINGVIEW
// ======================================================

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

        if (msg.m !== "timescale_update") {
          continue;
        }

        const payload = msg?.p?.[1];
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

        if (!candles.length) continue;

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

    ws.on("error", finishError);

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
// EMA
// ======================================================

function ema(values, period) {
  if (values.length < period) {
    return null;
  }

  let value =
    values
      .slice(0, period)
      .reduce((a, b) => a + b, 0) /
    period;

  const k = 2 / (period + 1);

  for (
    let i = period;
    i < values.length;
    i++
  ) {
    value =
      values[i] * k +
      value * (1 - k);
  }

  return value;
}


// ======================================================
// RSI - WILDER
// ======================================================

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

    gains +=
      Math.max(change, 0);

    losses +=
      Math.max(-change, 0);
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
      Math.max(change, 0);

    const loss =
      Math.max(-change, 0);

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

  return 100 - 100 / (1 + rs);
}


// ======================================================
// ATR - WILDER
// ======================================================

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
    const c = candles[i];
    const p = candles[i - 1];

    tr.push(
      Math.max(
        c.high - c.low,
        Math.abs(
          c.high - p.close
        ),
        Math.abs(
          c.low - p.close
        )
      )
    );
  }

  let value =
    tr
      .slice(0, period)
      .reduce((a, b) => a + b, 0) /
    period;

  for (
    let i = period;
    i < tr.length;
    i++
  ) {
    value =
      (
        value * (period - 1) +
        tr[i]
      ) / period;
  }

  return value;
}


// ======================================================
// RANGES
// ======================================================

function getRange(candles, bars) {
  const slice =
    candles.slice(-bars);

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
    high: {
      price: round(highest.high),
      time: highest.time
    },

    low: {
      price: round(lowest.low),
      time: lowest.time
    },

    midpoint:
      round(
        (
          highest.high +
          lowest.low
        ) / 2
      )
  };
}


// ======================================================
// SWINGS
// 2 candles each side
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

    const high =
      c.high >
        candles[i - 1].high &&
      c.high >
        candles[i - 2].high &&
      c.high >=
        candles[i + 1].high &&
      c.high >=
        candles[i + 2].high;

    const low =
      c.low <
        candles[i - 1].low &&
      c.low <
        candles[i - 2].low &&
      c.low <=
        candles[i + 1].low &&
      c.low <=
        candles[i + 2].low;

    if (high) {
      highs.push({
        time: c.time,
        price: round(c.high)
      });
    }

    if (low) {
      lows.push({
        time: c.time,
        price: round(c.low)
      });
    }
  }

  return {
    highs,
    lows
  };
}


// ======================================================
// MARKET STRUCTURE
// ======================================================

function marketStructure(swings) {
  const highs =
    swings.highs.slice(-2);

  const lows =
    swings.lows.slice(-2);

  if (
    highs.length < 2 ||
    lows.length < 2
  ) {
    return {
      direction: "insufficient-data",
      highStructure: null,
      lowStructure: null
    };
  }

  const highStructure =
    highs[1].price >
    highs[0].price
      ? "HH"
      : highs[1].price <
        highs[0].price
      ? "LH"
      : "EH";

  const lowStructure =
    lows[1].price >
    lows[0].price
      ? "HL"
      : lows[1].price <
        lows[0].price
      ? "LL"
      : "EL";

  let direction = "mixed";

  if (
    highStructure === "HH" &&
    lowStructure === "HL"
  ) {
    direction = "bullish";
  }

  if (
    highStructure === "LH" &&
    lowStructure === "LL"
  ) {
    direction = "bearish";
  }

  return {
    direction,
    highStructure,
    lowStructure,

    previousSwingHigh:
      highs[0],

    latestSwingHigh:
      highs[1],

    previousSwingLow:
      lows[0],

    latestSwingLow:
      lows[1]
  };
}


// ======================================================
// SUPPORT / RESISTANCE
//
// Uses recent confirmed swing points.
// These are candidates, not absolute levels.
// ======================================================

function supportResistance(
  swings,
  currentPrice
) {
  const supports =
    swings.lows
      .filter(
        s =>
          s.price <
          currentPrice
      )
      .sort(
        (a, b) =>
          b.price - a.price
      )
      .slice(0, 3)
      .map(s => ({
        price: s.price,
        time: s.time,
        distance:
          round(
            currentPrice -
            s.price
          )
      }));

  const resistances =
    swings.highs
      .filter(
        s =>
          s.price >
          currentPrice
      )
      .sort(
        (a, b) =>
          a.price - b.price
      )
      .slice(0, 3)
      .map(s => ({
        price: s.price,
        time: s.time,
        distance:
          round(
            s.price -
            currentPrice
          )
      }));

  return {
    supports,
    resistances
  };
}


// ======================================================
// FVG + INVALIDATION / IFVG CANDIDATE
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

    // -------------------------
    // BULLISH FVG
    // -------------------------

    if (
      first.high <
      third.low
    ) {
      const lower =
        first.high;

      const upper =
        third.low;

      let touched = false;
      let filled = false;
      let invalidated = false;
      let invalidatedAt = null;

      for (
        let j = i + 1;
        j < candles.length;
        j++
      ) {
        const c = candles[j];

        if (
          c.low <= upper
        ) {
          touched = true;
        }

        if (
          c.low <= lower
        ) {
          filled = true;
        }

        // close through opposite side
        if (
          c.close < lower
        ) {
          invalidated = true;
          invalidatedAt =
            c.time;
          break;
        }
      }

      gaps.push({
        originalType:
          "bullish",

        createdAt:
          third.time,

        lower:
          round(lower),

        upper:
          round(upper),

        midpoint:
          round(
            (lower + upper) / 2
          ),

        touched,
        filled,
        invalidated,
        invalidatedAt,

        status:
          invalidated
            ? "ifvg-candidate-bearish"
            : filled
            ? "filled"
            : touched
            ? "partially-mitigated"
            : "open"
      });
    }


    // -------------------------
    // BEARISH FVG
    // -------------------------

    if (
      first.low >
      third.high
    ) {
      const lower =
        third.high;

      const upper =
        first.low;

      let touched = false;
      let filled = false;
      let invalidated = false;
      let invalidatedAt = null;

      for (
        let j = i + 1;
        j < candles.length;
        j++
      ) {
        const c = candles[j];

        if (
          c.high >= lower
        ) {
          touched = true;
        }

        if (
          c.high >= upper
        ) {
          filled = true;
        }

        if (
          c.close > upper
        ) {
          invalidated = true;
          invalidatedAt =
            c.time;
          break;
        }
      }

      gaps.push({
        originalType:
          "bearish",

        createdAt:
          third.time,

        lower:
          round(lower),

        upper:
          round(upper),

        midpoint:
          round(
            (lower + upper) / 2
          ),

        touched,
        filled,
        invalidated,
        invalidatedAt,

        status:
          invalidated
            ? "ifvg-candidate-bullish"
            : filled
            ? "filled"
            : touched
            ? "partially-mitigated"
            : "open"
      });
    }
  }

  return gaps;
}


// ======================================================
// ANALYSIS
// ======================================================

function analyzeCandles(candles) {
  const latest =
    candles[candles.length - 1];

  const closes =
    candles.map(
      c => c.close
    );

  const ema20 =
    ema(closes, 20);

  const ema50 =
    ema(closes, 50);

  const rsi14 =
    rsiWilder(
      closes,
      14
    );

  const atr14 =
    atrWilder(
      candles,
      14
    );

  const swings =
    findSwings(candles);

  const structure =
    marketStructure(swings);

  const levels =
    supportResistance(
      swings,
      latest.close
    );

  const allFvg =
    findFVG(candles);

  const activeFvg =
    allFvg
      .filter(
        f =>
          f.status === "open" ||
          f.status ===
            "partially-mitigated"
      )
      .slice(-10);

  const ifvgCandidates =
    allFvg
      .filter(
        f =>
          f.invalidated
      )
      .slice(-10);

  let emaBias = "neutral";

  if (
    latest.close > ema20 &&
    ema20 > ema50
  ) {
    emaBias = "bullish";
  } else if (
    latest.close < ema20 &&
    ema20 < ema50
  ) {
    emaBias = "bearish";
  }

  let combinedBias = "mixed";

  if (
    emaBias === "bullish" &&
    structure.direction ===
      "bullish"
  ) {
    combinedBias = "bullish";
  }

  if (
    emaBias === "bearish" &&
    structure.direction ===
      "bearish"
  ) {
    combinedBias = "bearish";
  }

  return {
    candleCount:
      candles.length,

    currentPrice:
      round(latest.close),

    latestCandle: {
      time: latest.time,
      open: latest.open,
      high: latest.high,
      low: latest.low,
      close: latest.close,
      volume: latest.volume
    },

    indicators: {
      ema20:
        round(ema20),

      ema50:
        round(ema50),

      rsi14:
        round(rsi14, 2),

      atr14:
        round(atr14)
    },

    bias: {
      ema:
        emaBias,

      marketStructure:
        structure.direction,

      combined:
        combinedBias
    },

    marketStructure:
      structure,

    ranges: {
      last20:
        getRange(
          candles,
          20
        ),

      last50:
        getRange(
          candles,
          50
        )
    },

    levels,

    recentSwings: {
      highs:
        swings.highs.slice(-5),

      lows:
        swings.lows.slice(-5)
    },

    fairValueGaps: {
      active:
        activeFvg,

      ifvgCandidates:
        ifvgCandidates
    }
  };
}


// ======================================================
// ALL TIMEFRAMES
// Sequential = more stable
// ======================================================

async function getAllTimeframes() {
  const result = {};

  for (
    const key of
    Object.keys(TIMEFRAMES)
  ) {
    const data =
      await getCandles(key);

    result[
      TIMEFRAMES[key].label
    ] = data;
  }

  return result;
}


// ======================================================
// ROOT
// ======================================================

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "xauusd-123",
    version: "analysis-v2",
    symbol: SYMBOL,

    endpoints: {
      M5: "/xauusd/m5",
      M15: "/xauusd/m15",
      H1: "/xauusd/h1",
      H4: "/xauusd/h4",
      D1: "/xauusd/d1",
      ALL: "/xauusd/all",
      ANALYSIS:
        "/xauusd/analysis"
    }
  });
});


// ======================================================
// ANALYSIS V2
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
        const [timeframe, data]
        of Object.entries(all)
      ) {
        analysis[timeframe] =
          analyzeCandles(
            data.candles
          );
      }

      res.set(
        "Cache-Control",
        "no-store"
      );

      res.json({
        ok: true,
        version:
          "analysis-v2",

        symbol:
          SYMBOL,

        generatedAt:
          new Date()
            .toISOString(),

        durationMs:
          Date.now() -
          startedAt,

        analysis
      });

    } catch (error) {
      console.error(error);

      res
        .status(500)
        .json({
          ok: false,
          error:
            error?.message ||
            String(error)
        });
    }
  }
);


// ======================================================
// RAW ALL
// ======================================================

app.get(
  "/xauusd/all",
  async (req, res) => {
    try {
      const startedAt =
        Date.now();

      const all =
        await getAllTimeframes();

      const data = {};
      const counts = {};

      for (
        const [timeframe, value]
        of Object.entries(all)
      ) {
        data[timeframe] =
          value.candles;

        counts[timeframe] =
          value.candles.length;
      }

      res.set(
        "Cache-Control",
        "no-store"
      );

      res.json({
        ok: true,
        symbol: SYMBOL,

        generatedAt:
          new Date()
            .toISOString(),

        durationMs:
          Date.now() -
          startedAt,

        counts,
        data
      });

    } catch (error) {
      console.error(error);

      res
        .status(500)
        .json({
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
        !TIMEFRAMES[
          timeframe
        ]
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

      res
        .status(500)
        .json({
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
