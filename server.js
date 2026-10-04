import express from "express";
import WebSocket from "ws";

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";

const app = express();
const PORT = process.env.PORT || 3000;

const SYMBOL = "OANDA:XAUUSD";

/*
  FINAL DATA DEPTH
  M5  : 260
  M15 : 260
  H1  : 300
  H4  : 300
  D1  : 300
  W1  : 160
*/

const TIMEFRAMES = {
  m5:  { interval: "5",   label: "M5",  bars: 260 },
  m15: { interval: "15",  label: "M15", bars: 260 },
  h1:  { interval: "60",  label: "H1",  bars: 300 },
  h4:  { interval: "240", label: "H4",  bars: 300 },
  d1:  { interval: "1D",  label: "D1",  bars: 300 },
  w1:  { interval: "1W",  label: "W1",  bars: 160 }
};

app.use(express.json());


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

function average(values) {
  const valid = values.filter(Number.isFinite);

  if (!valid.length) return null;

  return valid.reduce((a, b) => a + b, 0) / valid.length;
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
    }, 30000);

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

      // HEARTBEAT
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
          requestedBars: config.bars,
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
// EMA SERIES
// ======================================================

function emaSeries(values, period) {
  if (values.length < period) return [];

  const result =
    new Array(values.length).fill(null);

  let value =
    values
      .slice(0, period)
      .reduce((a, b) => a + b, 0) /
    period;

  result[period - 1] = value;

  const k = 2 / (period + 1);

  for (
    let i = period;
    i < values.length;
    i++
  ) {
    value =
      values[i] * k +
      value * (1 - k);

    result[i] = value;
  }

  return result;
}

function ema(values, period) {
  const series =
    emaSeries(values, period);

  if (!series.length) return null;

  return series.at(-1);
}


// ======================================================
// RSI SERIES - WILDER
// ======================================================

function rsiSeries(
  values,
  period = 14
) {
  if (
    values.length <
    period + 1
  ) {
    return [];
  }

  const result =
    new Array(values.length).fill(null);

  let gains = 0;
  let losses = 0;

  for (
    let i = 1;
    i <= period;
    i++
  ) {
    const change =
      values[i] - values[i - 1];

    gains += Math.max(change, 0);
    losses += Math.max(-change, 0);
  }

  let avgGain = gains / period;
  let avgLoss = losses / period;

  result[period] =
    avgLoss === 0
      ? 100
      : 100 -
        100 /
          (
            1 +
            avgGain / avgLoss
          );

  for (
    let i = period + 1;
    i < values.length;
    i++
  ) {
    const change =
      values[i] - values[i - 1];

    avgGain =
      (
        avgGain * (period - 1) +
        Math.max(change, 0)
      ) / period;

    avgLoss =
      (
        avgLoss * (period - 1) +
        Math.max(-change, 0)
      ) / period;

    result[i] =
      avgLoss === 0
        ? 100
        : 100 -
          100 /
            (
              1 +
              avgGain / avgLoss
            );
  }

  return result;
}

function rsiWilder(
  values,
  period = 14
) {
  const series =
    rsiSeries(values, period);

  if (!series.length) return null;

  return series.at(-1);
}


// ======================================================
// ATR
// ======================================================

function atrWilder(
  candles,
  period = 14
) {
  if (
    candles.length <
    period + 1
  ) {
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
        Math.abs(c.high - p.close),
        Math.abs(c.low - p.close)
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
// RANGE
// ======================================================

function getRange(
  candles,
  bars
) {
  const slice =
    candles.slice(-bars);

  if (!slice.length) {
    return {
      high: null,
      low: null,
      midpoint: null
    };
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
    high: round(highest.high),
    low: round(lowest.low),

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

    const isHigh =
      c.high > candles[i - 1].high &&
      c.high > candles[i - 2].high &&
      c.high >= candles[i + 1].high &&
      c.high >= candles[i + 2].high;

    const isLow =
      c.low < candles[i - 1].low &&
      c.low < candles[i - 2].low &&
      c.low <= candles[i + 1].low &&
      c.low <= candles[i + 2].low;

    if (isHigh) {
      highs.push({
        index: i,
        time: c.time,
        price: round(c.high)
      });
    }

    if (isLow) {
      lows.push({
        index: i,
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

function getStructure(swings) {
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
    highs[1].price > highs[0].price
      ? "HH"
      : highs[1].price < highs[0].price
      ? "LH"
      : "EH";

  const lowStructure =
    lows[1].price > lows[0].price
      ? "HL"
      : lows[1].price < lows[0].price
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
    lowStructure
  };
}


// ======================================================
// SUPPORT / RESISTANCE
// ======================================================

function getLevels(
  swings,
  price
) {
  const supports =
    swings.lows
      .filter(
        x => x.price < price
      )
      .sort(
        (a, b) =>
          b.price - a.price
      )
      .slice(0, 5);

  const resistances =
    swings.highs
      .filter(
        x => x.price > price
      )
      .sort(
        (a, b) =>
          a.price - b.price
      )
      .slice(0, 5);

  return {
    supports,
    resistances
  };
}


// ======================================================
// FVG / IFVG
// ======================================================

function findFVG(candles) {
  const result = [];

  /*
    Search deeper history now that
    we have more candles.
  */

  const start =
    Math.max(
      2,
      candles.length - 200
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

    // BULLISH FVG
    if (first.high < third.low) {
      const lower = first.high;
      const upper = third.low;

      let touched = false;
      let filled = false;
      let invalidated = false;

      for (
        let j = i + 1;
        j < candles.length;
        j++
      ) {
        const c = candles[j];

        if (c.low <= upper) {
          touched = true;
        }

        if (c.low <= lower) {
          filled = true;
        }

        if (c.close < lower) {
          invalidated = true;
          break;
        }
      }

      result.push({
        createdIndex: i,
        createdTime: third.time,
        type: "bullish",
        lower: round(lower),
        upper: round(upper),

        midpoint:
          round(
            (lower + upper) / 2
          ),

        touched,
        filled,
        invalidated
      });
    }

    // BEARISH FVG
    if (first.low > third.high) {
      const lower = third.high;
      const upper = first.low;

      let touched = false;
      let filled = false;
      let invalidated = false;

      for (
        let j = i + 1;
        j < candles.length;
        j++
      ) {
        const c = candles[j];

        if (c.high >= lower) {
          touched = true;
        }

        if (c.high >= upper) {
          filled = true;
        }

        if (c.close > upper) {
          invalidated = true;
          break;
        }
      }

      result.push({
        createdIndex: i,
        createdTime: third.time,
        type: "bearish",
        lower: round(lower),
        upper: round(upper),

        midpoint:
          round(
            (lower + upper) / 2
          ),

        touched,
        filled,
        invalidated
      });
    }
  }

  return result;
}


// ======================================================
// VOLUME ANALYSIS
// ======================================================

function getVolumeAnalysis(
  candles,
  period = 20
) {
  const valid =
    candles.filter(
      c => Number.isFinite(c.volume)
    );

  if (!valid.length) {
    return {
      available: false,
      latest: null,
      average20: null,
      ratio: null,
      state: "unavailable"
    };
  }

  const latest =
    valid.at(-1).volume;

  const prior =
    valid
      .slice(
        -(period + 1),
        -1
      )
      .map(c => c.volume);

  const avg =
    average(prior);

  const ratio =
    Number.isFinite(avg) &&
    avg !== 0
      ? latest / avg
      : null;

  let state = "normal";

  if (
    Number.isFinite(ratio) &&
    ratio >= 1.5
  ) {
    state = "high";
  } else if (
    Number.isFinite(ratio) &&
    ratio <= 0.7
  ) {
    state = "low";
  }

  return {
    available: true,
    latest: round(latest, 2),
    average20: round(avg, 2),
    ratio: round(ratio, 2),
    state
  };
}


// ======================================================
// RSI DIVERGENCE
// ======================================================

function findRsiDivergence(
  candles,
  swings,
  rsiValues
) {
  let bullish = null;
  let bearish = null;

  const lows =
    swings.lows.filter(
      x =>
        Number.isFinite(
          rsiValues[x.index]
        )
    );

  const highs =
    swings.highs.filter(
      x =>
        Number.isFinite(
          rsiValues[x.index]
        )
    );

  if (lows.length >= 2) {
    const a = lows.at(-2);
    const b = lows.at(-1);

    const rsiA =
      rsiValues[a.index];

    const rsiB =
      rsiValues[b.index];

    if (
      b.price < a.price &&
      rsiB > rsiA
    ) {
      bullish = {
        detected: true,

        first: {
          time: a.time,
          price: a.price,
          rsi: round(rsiA, 2)
        },

        second: {
          time: b.time,
          price: b.price,
          rsi: round(rsiB, 2)
        }
      };
    }
  }

  if (highs.length >= 2) {
    const a = highs.at(-2);
    const b = highs.at(-1);

    const rsiA =
      rsiValues[a.index];

    const rsiB =
      rsiValues[b.index];

    if (
      b.price > a.price &&
      rsiB < rsiA
    ) {
      bearish = {
        detected: true,

        first: {
          time: a.time,
          price: a.price,
          rsi: round(rsiA, 2)
        },

        second: {
          time: b.time,
          price: b.price,
          rsi: round(rsiB, 2)
        }
      };
    }
  }

  return {
    bullish,
    bearish
  };
}


// ======================================================
// LIQUIDITY SWEEPS
// ======================================================

function findLiquiditySweeps(
  candles,
  swings
) {
  const recent =
    candles.slice(-80);

  const offset =
    candles.length -
    recent.length;

  const sweeps = [];

  const candidateHighs =
    swings.highs.slice(-12);

  const candidateLows =
    swings.lows.slice(-12);

  for (
    let localIndex = 0;
    localIndex < recent.length;
    localIndex++
  ) {
    const c =
      recent[localIndex];

    const index =
      offset + localIndex;

    for (
      const swing of
      candidateHighs
    ) {
      if (
        swing.index >= index
      ) {
        continue;
      }

      /*
        Price trades above previous
        swing high but closes back below.
      */

      if (
        c.high > swing.price &&
        c.close < swing.price
      ) {
        sweeps.push({
          type: "buy-side-sweep",
          time: c.time,
          sweptLevel: swing.price,
          extreme: round(c.high),
          close: round(c.close)
        });
      }
    }

    for (
      const swing of
      candidateLows
    ) {
      if (
        swing.index >= index
      ) {
        continue;
      }

      /*
        Price trades below previous
        swing low but closes back above.
      */

      if (
        c.low < swing.price &&
        c.close > swing.price
      ) {
        sweeps.push({
          type: "sell-side-sweep",
          time: c.time,
          sweptLevel: swing.price,
          extreme: round(c.low),
          close: round(c.close)
        });
      }
    }
  }

  /*
    Remove near-duplicates.
  */

  const unique = [];

  const seen = new Set();

  for (const sweep of sweeps) {
    const key =
      `${sweep.type}|${sweep.time}|${sweep.sweptLevel}`;

    if (seen.has(key)) continue;

    seen.add(key);
    unique.push(sweep);
  }

  return unique.slice(-10);
}


// ======================================================
// PREMIUM / DISCOUNT
// ======================================================

function getPremiumDiscount(
  candles,
  bars = 50
) {
  const range =
    getRange(
      candles,
      Math.min(
        bars,
        candles.length
      )
    );

  const latest =
    candles.at(-1)?.close;

  if (
    !Number.isFinite(latest) ||
    !Number.isFinite(range.midpoint)
  ) {
    return {
      rangeHigh: null,
      equilibrium: null,
      rangeLow: null,
      location: null
    };
  }

  let location =
    "equilibrium";

  const totalRange =
    range.high - range.low;

  const tolerance =
    totalRange * 0.05;

  if (
    latest >
    range.midpoint + tolerance
  ) {
    location = "premium";
  }

  if (
    latest <
    range.midpoint - tolerance
  ) {
    location = "discount";
  }

  return {
    rangeHigh: range.high,
    equilibrium: range.midpoint,
    rangeLow: range.low,
    location
  };
}


// ======================================================
// S/R FLIP + BREAK / RETEST
// ======================================================

function detectFlipAndRetest(
  candles,
  swings
) {
  const events = [];

  const recentCandles =
    candles.slice(-100);

  const offset =
    candles.length -
    recentCandles.length;

  const levels = [
    ...swings.highs
      .slice(-15)
      .map(x => ({
        source: "swing-high",
        level: x.price,
        index: x.index
      })),

    ...swings.lows
      .slice(-15)
      .map(x => ({
        source: "swing-low",
        level: x.price,
        index: x.index
      }))
  ];

  for (const item of levels) {
    let breakIndex = null;
    let breakDirection = null;

    for (
      let i =
        Math.max(
          item.index + 1,
          offset
        );

      i < candles.length;

      i++
    ) {
      const previous =
        candles[i - 1];

      const current =
        candles[i];

      /*
        Bullish break
      */

      if (
        previous.close <= item.level &&
        current.close > item.level
      ) {
        breakIndex = i;
        breakDirection = "bullish";
        break;
      }

      /*
        Bearish break
      */

      if (
        previous.close >= item.level &&
        current.close < item.level
      ) {
        breakIndex = i;
        breakDirection = "bearish";
        break;
      }
    }

    if (
      breakIndex == null
    ) {
      continue;
    }

    let retest = null;

    for (
      let j = breakIndex + 1;
      j < candles.length;
      j++
    ) {
      const c =
        candles[j];

      if (
        breakDirection === "bullish" &&
        c.low <= item.level &&
        c.close >= item.level
      ) {
        retest = {
          time: c.time,
          result: "support-retest-held"
        };
        break;
      }

      if (
        breakDirection === "bearish" &&
        c.high >= item.level &&
        c.close <= item.level
      ) {
        retest = {
          time: c.time,
          result: "resistance-retest-held"
        };
        break;
      }
    }

    events.push({
      level: item.level,
      source: item.source,
      breakDirection,

      breakTime:
        candles[breakIndex].time,

      retest
    });
  }

  /*
    Deduplicate levels.
  */

  const map = new Map();

  for (const event of events) {
    const key =
      `${event.level}|${event.breakDirection}`;

    map.set(key, event);
  }

  return Array
    .from(map.values())
    .slice(-10);
}


// ======================================================
// ANALYZE ONE TIMEFRAME
// ======================================================

function analyze(candles) {
  const latest =
    candles.at(-1);

  const closes =
    candles.map(
      c => c.close
    );

  const e20 =
    ema(closes, 20);

  const e50 =
    ema(closes, 50);

  const e200 =
    ema(closes, 200);

  const rsiValues =
    rsiSeries(
      closes,
      14
    );

  const rsi =
    rsiWilder(
      closes,
      14
    );

  const atr =
    atrWilder(
      candles,
      14
    );

  const swings =
    findSwings(candles);

  const structure =
    getStructure(swings);

  const levels =
    getLevels(
      swings,
      latest.close
    );

  const fvgs =
    findFVG(candles);

  const activeFVG =
    fvgs.filter(
      x =>
        !x.invalidated &&
        !x.filled
    );

  const ifvg =
    fvgs.filter(
      x => x.invalidated
    );

  const volume =
    getVolumeAnalysis(
      candles
    );

  const divergence =
    findRsiDivergence(
      candles,
      swings,
      rsiValues
    );

  const liquiditySweeps =
    findLiquiditySweeps(
      candles,
      swings
    );

  const premiumDiscount =
    getPremiumDiscount(
      candles,
      50
    );

  const flipRetest =
    detectFlipAndRetest(
      candles,
      swings
    );

  let emaBias =
    "neutral";

  if (
    Number.isFinite(e20) &&
    Number.isFinite(e50) &&
    latest.close > e20 &&
    e20 > e50
  ) {
    emaBias = "bullish";
  }

  if (
    Number.isFinite(e20) &&
    Number.isFinite(e50) &&
    latest.close < e20 &&
    e20 < e50
  ) {
    emaBias = "bearish";
  }

  let ema200Bias =
    "unavailable";

  if (
    Number.isFinite(e200)
  ) {
    ema200Bias =
      latest.close > e200
        ? "above"
        : latest.close < e200
        ? "below"
        : "at";
  }

  let combinedBias =
    "mixed";

  if (
    emaBias === "bullish" &&
    structure.direction === "bullish"
  ) {
    combinedBias = "bullish";
  }

  if (
    emaBias === "bearish" &&
    structure.direction === "bearish"
  ) {
    combinedBias = "bearish";
  }

  return {
    price:
      round(latest.close),

    latestTime:
      latest.time,

    candleCount:
      candles.length,

    ema20:
      round(e20),

    ema50:
      round(e50),

    ema200:
      round(e200),

    emaBias,

    ema200Position:
      ema200Bias,

    rsi14:
      round(rsi, 2),

    atr14:
      round(atr),

    structure:
      structure.direction,

    highStructure:
      structure.highStructure,

    lowStructure:
      structure.lowStructure,

    combinedBias,

    range20:
      getRange(
        candles,
        20
      ),

    range50:
      getRange(
        candles,
        50
      ),

    range100:
      getRange(
        candles,
        Math.min(
          100,
          candles.length
        )
      ),

    premiumDiscount,

    support1:
      levels.supports[0]?.price ??
      null,

    support2:
      levels.supports[1]?.price ??
      null,

    support3:
      levels.supports[2]?.price ??
      null,

    support4:
      levels.supports[3]?.price ??
      null,

    support5:
      levels.supports[4]?.price ??
      null,

    resistance1:
      levels.resistances[0]?.price ??
      null,

    resistance2:
      levels.resistances[1]?.price ??
      null,

    resistance3:
      levels.resistances[2]?.price ??
      null,

    resistance4:
      levels.resistances[3]?.price ??
      null,

    resistance5:
      levels.resistances[4]?.price ??
      null,

    latestSwingHigh:
      swings.highs.at(-1)?.price ??
      null,

    previousSwingHigh:
      swings.highs.at(-2)?.price ??
      null,

    latestSwingLow:
      swings.lows.at(-1)?.price ??
      null,

    previousSwingLow:
      swings.lows.at(-2)?.price ??
      null,

    recentSwingHighs:
      swings.highs
        .slice(-8)
        .map(x => ({
          time: x.time,
          price: x.price
        })),

    recentSwingLows:
      swings.lows
        .slice(-8)
        .map(x => ({
          time: x.time,
          price: x.price
        })),

    volume,

    rsiDivergence:
      divergence,

    liquiditySweeps,

    flipRetest,

    activeFVG:
      activeFVG
        .slice(-10)
        .map(x => ({
          createdTime:
            x.createdTime,

          type:
            x.type,

          lower:
            x.lower,

          upper:
            x.upper,

          midpoint:
            x.midpoint,

          touched:
            x.touched
        })),

    ifvgCandidates:
      ifvg
        .slice(-10)
        .map(x => ({
          createdTime:
            x.createdTime,

          originalType:
            x.type,

          newBias:
            x.type === "bullish"
              ? "bearish"
              : "bullish",

          lower:
            x.lower,

          upper:
            x.upper,

          midpoint:
            x.midpoint
        }))
  };
}


// ======================================================
// GET ALL - SEQUENTIAL
//
// Keep sequential fetching.
// More reliable for TradingView websocket.
// ======================================================

async function getAllTimeframes() {
  const result = {};

  for (
    const key of
    Object.keys(TIMEFRAMES)
  ) {
    const value =
      await getCandles(key);

    result[
      TIMEFRAMES[key].label
    ] = value;
  }

  return result;
}


// ======================================================
// BUILD SUMMARY
// ======================================================

async function buildSummary() {
  const startedAt =
    Date.now();

  const all =
    await getAllTimeframes();

  const output = {};
  const counts = {};

  for (
    const [timeframe, value]
    of Object.entries(all)
  ) {
    output[timeframe] =
      analyze(
        value.candles
      );

    counts[timeframe] =
      value.candles.length;
  }

  return {
    ok: true,

    version:
      "final-v1",

    symbol:
      SYMBOL,

    source:
      "TradingView / OANDA:XAUUSD",

    generatedAt:
      new Date()
        .toISOString(),

    durationMs:
      Date.now() -
      startedAt,

    requestedBars: {
      M5: 260,
      M15: 260,
      H1: 300,
      H4: 300,
      D1: 300,
      W1: 160
    },

    actualCounts:
      counts,

    M5:
      output.M5,

    M15:
      output.M15,

    H1:
      output.H1,

    H4:
      output.H4,

    D1:
      output.D1,

    W1:
      output.W1
  };
}


// ======================================================
// MCP SERVER
// ======================================================

function createMcpServer() {
  const server =
    new McpServer({
      name:
        "xauusd-tradingview",

      version:
        "2.0.0"
    });

  server.tool(
    "get_xauusd_analysis",

    `Get fresh TradingView OANDA:XAUUSD market data for the existing XAUUSD 123 analysis system.

IMPORTANT:
This tool is the market-data replacement for the previous TradingView data plugin.
It does NOT define or replace the user's analysis methodology.

Always use the returned raw/derived market data as inputs to the established 123 analysis, zone-map and Sniper methodology.

Timeframes and requested history:
M5: 260 candles
M15: 260 candles
H1: 300 candles
H4: 300 candles
D1: 300 candles
W1: 160 candles

Returned analysis inputs include:
OHLC-derived current price and latest time
EMA20
EMA50
EMA200 where sufficient history exists
RSI14
ATR14
market structure
HH / HL / LH / LL
support and resistance
recent swing highs and lows
20 / 50 / 100 candle ranges
FVG
IFVG candidates
volume analysis when TradingView volume is available
liquidity sweep candidates
RSI bullish/bearish divergence
premium / discount location
support-resistance flips
break and retest candidates.

Data source:
TradingView OANDA:XAUUSD.`,

    {},

    async () => {
      try {
        const data =
          await buildSummary();

        return {
          content: [
            {
              type: "text",

              text:
                JSON.stringify(
                  data,
                  null,
                  2
                )
            }
          ]
        };

      } catch (error) {
        console.error(
          "MCP TOOL ERROR",
          error
        );

        return {
          isError: true,

          content: [
            {
              type: "text",

              text:
                JSON.stringify(
                  {
                    ok: false,

                    error:
                      error?.message ||
                      String(error)
                  },
                  null,
                  2
                )
            }
          ]
        };
      }
    }
  );

  return server;
}


// ======================================================
// MCP STREAMABLE HTTP
// ======================================================

app.post(
  "/mcp",
  async (req, res) => {
    let transport = null;
    let server = null;

    try {
      server =
        createMcpServer();

      transport =
        new StreamableHTTPServerTransport({
          sessionIdGenerator:
            undefined
        });

      res.on(
        "close",
        () => {
          try {
            transport?.close();
          } catch {}

          try {
            server?.close();
          } catch {}
        }
      );

      await server.connect(
        transport
      );

      await transport.handleRequest(
        req,
        res,
        req.body
      );

    } catch (error) {
      console.error(
        "MCP POST ERROR",
        error
      );

      if (
        !res.headersSent
      ) {
        res
          .status(500)
          .json({
            jsonrpc: "2.0",

            error: {
              code: -32603,

              message:
                error?.message ||
                "Internal server error"
            },

            id:
              req.body?.id ??
              null
          });
      }
    }
  }
);

app.get(
  "/mcp",
  (req, res) => {
    res
      .status(405)
      .set(
        "Allow",
        "POST"
      )
      .json({
        ok: false,
        message:
          "Use POST for MCP Streamable HTTP"
      });
  }
);

app.delete(
  "/mcp",
  (req, res) => {
    res
      .status(405)
      .set(
        "Allow",
        "POST"
      )
      .json({
        ok: false,

        message:
          "Stateless MCP server has no persistent session"
      });
  }
);


// ======================================================
// ROOT
// ======================================================

app.get("/", (req, res) => {
  res.json({
    ok: true,

    service:
      "xauusd-123",

    version:
      "final-v1",

    symbol:
      SYMBOL,

    mcp:
      true,

    requestedBars: {
      M5: 260,
      M15: 260,
      H1: 300,
      H4: 300,
      D1: 300,
      W1: 160
    },

    features: [
      "EMA20",
      "EMA50",
      "EMA200",
      "RSI14",
      "ATR14",
      "Market Structure",
      "Support Resistance",
      "FVG",
      "IFVG",
      "Volume",
      "Liquidity Sweep",
      "RSI Divergence",
      "Premium Discount",
      "SR Flip",
      "Break Retest"
    ],

    endpoints: {
      M5: "/xauusd/m5",
      M15: "/xauusd/m15",
      H1: "/xauusd/h1",
      H4: "/xauusd/h4",
      D1: "/xauusd/d1",
      W1: "/xauusd/w1",
      ALL: "/xauusd/all",
      ANALYSIS: "/xauusd/analysis",
      SUMMARY: "/xauusd/summary",
      MCP: "/mcp"
    }
  });
});


// ======================================================
// SUMMARY
// ======================================================

app.get(
  "/xauusd/summary",
  async (req, res) => {
    try {
      const result =
        await buildSummary();

      res.set(
        "Cache-Control",
        "no-store"
      );

      res.json(result);

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
// DETAILED ANALYSIS
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
      const counts = {};

      for (
        const [timeframe, value]
        of Object.entries(all)
      ) {
        analysis[timeframe] =
          analyze(
            value.candles
          );

        counts[timeframe] =
          value.candles.length;
      }

      res.set(
        "Cache-Control",
        "no-store"
      );

      res.json({
        ok: true,

        version:
          "final-v1",

        symbol:
          SYMBOL,

        generatedAt:
          new Date()
            .toISOString(),

        durationMs:
          Date.now() -
          startedAt,

        counts,

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

        symbol:
          SYMBOL,

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
      `xauusd-123 FINAL running on port ${PORT}`
    );

    console.log(
      "MCP endpoint: /mcp"
    );

    console.log(
      "Bars: M5=260 M15=260 H1=300 H4=300 D1=300 W1=160"
    );
  }
);
