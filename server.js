import express from "express";
import WebSocket from "ws";

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";

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
// JSON
// ======================================================

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


// ======================================================
// TRADINGVIEW
// ======================================================

function getCandles(timeframe) {
  return new Promise((resolve, reject) => {

    const config = TIMEFRAMES[timeframe];

    if (!config) {
      reject(
        new Error("Unsupported timeframe")
      );
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

    const chartSession =
      session("cs");

    const quoteSession =
      session("qs");

    let finished = false;


    const timeout =
      setTimeout(() => {

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

      const send =
        (method, params) => {

          ws.send(
            frame({
              m: method,
              p: params
            })
          );
        };


      send(
        "set_auth_token",
        ["unauthorized_user_token"]
      );


      send(
        "chart_create_session",
        [
          chartSession,
          ""
        ]
      );


      send(
        "quote_create_session",
        [
          quoteSession
        ]
      );


      send(
        "quote_set_fields",
        [
          quoteSession,
          "lp",
          "ch",
          "chp",
          "short_name",
          "exchange"
        ]
      );


      send(
        "quote_add_symbols",
        [
          quoteSession,
          SYMBOL,
          {
            flags: [
              "force_permission"
            ]
          }
        ]
      );


      const symbolDescriptor =
        "=" +
        JSON.stringify({
          symbol: SYMBOL,
          adjustment: "splits"
        });


      send(
        "resolve_symbol",
        [
          chartSession,
          "symbol_1",
          symbolDescriptor
        ]
      );


      send(
        "create_series",
        [
          chartSession,
          "s1",
          "s1",
          "symbol_1",
          config.interval,
          config.bars
        ]
      );
    });


    ws.on("message", raw => {

      const text =
        raw.toString();


      // ------------------------------------------
      // HEARTBEAT
      // ------------------------------------------

      const heartbeatRegex =
        /~m~\d+~m~(~h~\d+)/g;

      let heartbeat;


      while (
        (
          heartbeat =
            heartbeatRegex.exec(text)
        ) !== null
      ) {

        try {

          ws.send(
            `~m~${heartbeat[1].length}~m~${heartbeat[1]}`
          );

        } catch {}
      }


      // ------------------------------------------
      // PARSE
      // ------------------------------------------

      const parts =
        text.split(
          /~m~\d+~m~/
        );


      for (const part of parts) {

        if (
          !part ||
          !part.startsWith("{")
        ) {
          continue;
        }


        let msg;


        try {

          msg =
            JSON.parse(part);

        } catch {

          continue;
        }


        if (
          msg.m ===
          "symbol_error"
        ) {

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
          msg.m !==
          "timescale_update"
        ) {
          continue;
        }


        const payload =
          msg?.p?.[1];


        if (!payload) {
          continue;
        }


        const series =
          payload?.s1?.s ||
          Object
            .values(payload)
            .find(
              value =>
                Array.isArray(
                  value?.s
                )
            )?.s;


        if (
          !Array.isArray(series) ||
          series.length === 0
        ) {
          continue;
        }


        const candles =
          series

            .map(bar => {

              const v =
                bar?.v;


              if (
                !Array.isArray(v) ||
                v.length < 5
              ) {
                return null;
              }


              return {

                time:
                  new Date(
                    Number(v[0]) *
                    1000
                  )
                    .toISOString(),

                open:
                  Number(v[1]),

                high:
                  Number(v[2]),

                low:
                  Number(v[3]),

                close:
                  Number(v[4]),

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


        if (!candles.length) {
          continue;
        }


        finishSuccess({

          ok: true,

          symbol:
            SYMBOL,

          timeframe:
            config.label,

          interval:
            config.interval,

          count:
            candles.length,

          candles
        });


        return;
      }
    });


    ws.on(
      "error",
      finishError
    );


    ws.on(
      "close",
      (code, reason) => {

        if (finished) return;


        finishError(
          new Error(
            `TradingView socket closed (${code}) ${
              reason?.toString() || ""
            }`
          )
        );
      }
    );
  });
}


// ======================================================
// EMA
// ======================================================

function ema(
  values,
  period
) {

  if (
    values.length <
    period
  ) {
    return null;
  }


  let value =
    values
      .slice(
        0,
        period
      )
      .reduce(
        (a, b) =>
          a + b,
        0
      ) /
    period;


  const k =
    2 /
    (
      period + 1
    );


  for (
    let i = period;
    i < values.length;
    i++
  ) {

    value =
      values[i] *
        k +
      value *
        (
          1 - k
        );
  }


  return value;
}


// ======================================================
// RSI
// ======================================================

function rsiWilder(
  values,
  period = 14
) {

  if (
    values.length <
    period + 1
  ) {
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
      values[i] -
      values[i - 1];


    gains +=
      Math.max(
        change,
        0
      );


    losses +=
      Math.max(
        -change,
        0
      );
  }


  let avgGain =
    gains /
    period;


  let avgLoss =
    losses /
    period;


  for (
    let i =
      period + 1;

    i <
      values.length;

    i++
  ) {

    const change =
      values[i] -
      values[i - 1];


    avgGain =
      (
        avgGain *
          (
            period - 1
          ) +
        Math.max(
          change,
          0
        )
      ) /
      period;


    avgLoss =
      (
        avgLoss *
          (
            period - 1
          ) +
        Math.max(
          -change,
          0
        )
      ) /
      period;
  }


  if (
    avgLoss === 0
  ) {
    return 100;
  }


  const rs =
    avgGain /
    avgLoss;


  return (
    100 -
    100 /
      (
        1 + rs
      )
  );
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

    const c =
      candles[i];

    const p =
      candles[i - 1];


    tr.push(
      Math.max(

        c.high -
          c.low,

        Math.abs(
          c.high -
          p.close
        ),

        Math.abs(
          c.low -
          p.close
        )
      )
    );
  }


  let value =
    tr
      .slice(
        0,
        period
      )
      .reduce(
        (a, b) =>
          a + b,
        0
      ) /
    period;


  for (
    let i = period;
    i < tr.length;
    i++
  ) {

    value =
      (
        value *
          (
            period - 1
          ) +
        tr[i]
      ) /
      period;
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
    candles.slice(
      -bars
    );


  const highest =
    slice.reduce(
      (a, b) =>
        b.high >
        a.high
          ? b
          : a
    );


  const lowest =
    slice.reduce(
      (a, b) =>
        b.low <
        a.low
          ? b
          : a
    );


  return {

    high:
      round(
        highest.high
      ),

    low:
      round(
        lowest.low
      ),

    midpoint:
      round(
        (
          highest.high +
          lowest.low
        ) /
        2
      )
  };
}


// ======================================================
// SWINGS
// ======================================================

function findSwings(
  candles
) {

  const highs = [];
  const lows = [];


  for (
    let i = 2;
    i <
      candles.length - 2;
    i++
  ) {

    const c =
      candles[i];


    const isHigh =

      c.high >
        candles[i - 1].high &&

      c.high >
        candles[i - 2].high &&

      c.high >=
        candles[i + 1].high &&

      c.high >=
        candles[i + 2].high;


    const isLow =

      c.low <
        candles[i - 1].low &&

      c.low <
        candles[i - 2].low &&

      c.low <=
        candles[i + 1].low &&

      c.low <=
        candles[i + 2].low;


    if (isHigh) {

      highs.push({

        time:
          c.time,

        price:
          round(
            c.high
          )
      });
    }


    if (isLow) {

      lows.push({

        time:
          c.time,

        price:
          round(
            c.low
          )
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

function getStructure(
  swings
) {

  const highs =
    swings.highs.slice(
      -2
    );


  const lows =
    swings.lows.slice(
      -2
    );


  if (
    highs.length < 2 ||
    lows.length < 2
  ) {

    return {

      direction:
        "insufficient-data",

      highStructure:
        null,

      lowStructure:
        null
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


  let direction =
    "mixed";


  if (
    highStructure === "HH" &&
    lowStructure === "HL"
  ) {

    direction =
      "bullish";
  }


  if (
    highStructure === "LH" &&
    lowStructure === "LL"
  ) {

    direction =
      "bearish";
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
        x =>
          x.price <
          price
      )

      .sort(
        (a, b) =>
          b.price -
          a.price
      )

      .slice(
        0,
        3
      );


  const resistances =
    swings.highs

      .filter(
        x =>
          x.price >
          price
      )

      .sort(
        (a, b) =>
          a.price -
          b.price
      )

      .slice(
        0,
        3
      );


  return {
    supports,
    resistances
  };
}


// ======================================================
// FVG / IFVG
// ======================================================

function findFVG(
  candles
) {

  const result = [];


  const start =
    Math.max(
      2,
      candles.length -
        50
    );


  for (
    let i = start;
    i <
      candles.length;
    i++
  ) {

    const first =
      candles[i - 2];


    const third =
      candles[i];


    // --------------------------------------------------
    // BULLISH FVG
    // --------------------------------------------------

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


      for (
        let j =
          i + 1;

        j <
          candles.length;

        j++
      ) {

        const c =
          candles[j];


        if (
          c.low <=
          upper
        ) {

          touched = true;
        }


        if (
          c.low <=
          lower
        ) {

          filled = true;
        }


        if (
          c.close <
          lower
        ) {

          invalidated = true;

          break;
        }
      }


      result.push({

        type:
          "bullish",

        lower:
          round(
            lower
          ),

        upper:
          round(
            upper
          ),

        midpoint:
          round(
            (
              lower +
              upper
            ) /
            2
          ),

        touched,

        filled,

        invalidated
      });
    }


    // --------------------------------------------------
    // BEARISH FVG
    // --------------------------------------------------

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


      for (
        let j =
          i + 1;

        j <
          candles.length;

        j++
      ) {

        const c =
          candles[j];


        if (
          c.high >=
          lower
        ) {

          touched = true;
        }


        if (
          c.high >=
          upper
        ) {

          filled = true;
        }


        if (
          c.close >
          upper
        ) {

          invalidated = true;

          break;
        }
      }


      result.push({

        type:
          "bearish",

        lower:
          round(
            lower
          ),

        upper:
          round(
            upper
          ),

        midpoint:
          round(
            (
              lower +
              upper
            ) /
            2
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
// ANALYZE
// ======================================================

function analyze(
  candles
) {

  const latest =
    candles[
      candles.length - 1
    ];


  const closes =
    candles.map(
      c =>
        c.close
    );


  const e20 =
    ema(
      closes,
      20
    );


  const e50 =
    ema(
      closes,
      50
    );


  const rsi =
    rsiWilder(
      closes
    );


  const atr =
    atrWilder(
      candles
    );


  const swings =
    findSwings(
      candles
    );


  const structure =
    getStructure(
      swings
    );


  const levels =
    getLevels(
      swings,
      latest.close
    );


  const fvgs =
    findFVG(
      candles
    );


  const activeFVG =
    fvgs.filter(
      x =>
        !x.invalidated &&
        !x.filled
    );


  const ifvg =
    fvgs.filter(
      x =>
        x.invalidated
    );


  let emaBias =
    "neutral";


  if (
    latest.close >
      e20 &&
    e20 >
      e50
  ) {

    emaBias =
      "bullish";
  }


  if (
    latest.close <
      e20 &&
    e20 <
      e50
  ) {

    emaBias =
      "bearish";
  }


  let combinedBias =
    "mixed";


  if (
    emaBias ===
      "bullish" &&

    structure.direction ===
      "bullish"
  ) {

    combinedBias =
      "bullish";
  }


  if (
    emaBias ===
      "bearish" &&

    structure.direction ===
      "bearish"
  ) {

    combinedBias =
      "bearish";
  }


  return {

    price:
      round(
        latest.close
      ),

    latestTime:
      latest.time,

    ema20:
      round(
        e20
      ),

    ema50:
      round(
        e50
      ),

    rsi14:
      round(
        rsi,
        2
      ),

    atr14:
      round(
        atr
      ),

    emaBias,

    structure:
      structure.direction,

    highStructure:
      structure.highStructure,

    lowStructure:
      structure.lowStructure,

    combinedBias,


    range20High:
      getRange(
        candles,
        20
      ).high,

    range20Low:
      getRange(
        candles,
        20
      ).low,


    range50High:
      getRange(
        candles,
        50
      ).high,

    range50Low:
      getRange(
        candles,
        50
      ).low,


    support1:
      levels
        .supports[0]
        ?.price ??
      null,

    support2:
      levels
        .supports[1]
        ?.price ??
      null,

    support3:
      levels
        .supports[2]
        ?.price ??
      null,


    resistance1:
      levels
        .resistances[0]
        ?.price ??
      null,

    resistance2:
      levels
        .resistances[1]
        ?.price ??
      null,

    resistance3:
      levels
        .resistances[2]
        ?.price ??
      null,


    latestSwingHigh:
      swings
        .highs
        .at(-1)
        ?.price ??
      null,

    previousSwingHigh:
      swings
        .highs
        .at(-2)
        ?.price ??
      null,


    latestSwingLow:
      swings
        .lows
        .at(-1)
        ?.price ??
      null,

    previousSwingLow:
      swings
        .lows
        .at(-2)
        ?.price ??
      null,


    activeFVG:
      activeFVG
        .slice(-5)
        .map(
          x => ({
            type:
              x.type,

            lower:
              x.lower,

            upper:
              x.upper,

            midpoint:
              x.midpoint
          })
        ),


    ifvgCandidates:
      ifvg
        .slice(-5)
        .map(
          x => ({

            originalType:
              x.type,

            newBias:
              x.type ===
              "bullish"
                ? "bearish"
                : "bullish",

            lower:
              x.lower,

            upper:
              x.upper,

            midpoint:
              x.midpoint
          })
        )
  };
}


// ======================================================
// GET ALL TIMEFRAMES
// ======================================================

async function getAllTimeframes() {

  const result = {};


  for (
    const key of
    Object.keys(
      TIMEFRAMES
    )
  ) {

    const value =
      await getCandles(
        key
      );


    result[
      TIMEFRAMES[
        key
      ].label
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


  for (
    const [
      timeframe,
      value
    ] of
      Object.entries(
        all
      )
  ) {

    output[
      timeframe
    ] =
      analyze(
        value.candles
      );
  }


  return {

    ok: true,

    version:
      "summary-v2-mcp",

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

    M5:
      output.M5,

    M15:
      output.M15,

    H1:
      output.H1,

    H4:
      output.H4,

    D1:
      output.D1
  };
}


// ======================================================
// MCP SERVER FACTORY
// ======================================================

function createMcpServer() {

  const server =
    new McpServer({

      name:
        "xauusd-tradingview",

      version:
        "1.0.0"
    });


  server.tool(

    "get_xauusd_analysis",

    `Get the latest OANDA:XAUUSD TradingView market data and multi-timeframe technical analysis.

Use this tool whenever current XAUUSD or Gold market data is required.

The tool retrieves fresh TradingView data for:
M5
M15
H1
H4
D1

It returns:
current price
latest candle time
EMA20
EMA50
RSI14
ATR14
EMA bias
market structure
HH HL LH LL structure
combined bias
20-bar range
50-bar range
support levels
resistance levels
recent swing highs
recent swing lows
active FVG
IFVG candidates.

The data source is OANDA:XAUUSD through TradingView.`,

    {},

    async () => {

      try {

        const data =
          await buildSummary();


        return {

          content: [
            {
              type:
                "text",

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

          isError:
            true,

          content: [
            {
              type:
                "text",

              text:
                JSON.stringify(
                  {
                    ok:
                      false,

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
//
// STATELESS:
// Each MCP request gets a fresh transport/server.
// This avoids session-ID problems during ChatGPT
// connector creation and tool discovery.
// ======================================================

app.post(
  "/mcp",
  async (
    req,
    res
  ) => {

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

            jsonrpc:
              "2.0",

            error: {

              code:
                -32603,

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


// ======================================================
// MCP GET
//
// Stateless Streamable HTTP does not require
// a persistent SSE stream.
// ======================================================

app.get(
  "/mcp",
  (
    req,
    res
  ) => {

    res
      .status(405)
      .set(
        "Allow",
        "POST"
      )
      .json({

        ok:
          false,

        message:
          "Use POST for MCP Streamable HTTP"
      });
  }
);


// ======================================================
// MCP DELETE
// ======================================================

app.delete(
  "/mcp",
  (
    req,
    res
  ) => {

    res
      .status(405)
      .set(
        "Allow",
        "POST"
      )
      .json({

        ok:
          false,

        message:
          "Stateless MCP server has no persistent session"
      });
  }
);


// ======================================================
// ROOT
// ======================================================

app.get(
  "/",
  (
    req,
    res
  ) => {

    res.json({

      ok:
        true,

      service:
        "xauusd-123",

      version:
        "analysis-v3-mcp-v2",

      symbol:
        SYMBOL,

      mcp:
        true,

      endpoints: {

        M5:
          "/xauusd/m5",

        M15:
          "/xauusd/m15",

        H1:
          "/xauusd/h1",

        H4:
          "/xauusd/h4",

        D1:
          "/xauusd/d1",

        ALL:
          "/xauusd/all",

        ANALYSIS:
          "/xauusd/analysis",

        SUMMARY:
          "/xauusd/summary",

        MCP:
          "/mcp"
      }
    });
  }
);


// ======================================================
// SUMMARY
// ======================================================

app.get(
  "/xauusd/summary",
  async (
    req,
    res
  ) => {

    try {

      const result =
        await buildSummary();


      res.set(
        "Cache-Control",
        "no-store"
      );


      res.json(
        result
      );


    } catch (error) {

      console.error(
        error
      );


      res
        .status(500)
        .json({

          ok:
            false,

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
  async (
    req,
    res
  ) => {

    try {

      const startedAt =
        Date.now();


      const all =
        await getAllTimeframes();


      const analysis = {};


      for (
        const [
          timeframe,
          value
        ] of
          Object.entries(
            all
          )
      ) {

        analysis[
          timeframe
        ] =
          analyze(
            value.candles
          );
      }


      res.set(
        "Cache-Control",
        "no-store"
      );


      res.json({

        ok:
          true,

        version:
          "analysis-v3",

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

      console.error(
        error
      );


      res
        .status(500)
        .json({

          ok:
            false,

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
  async (
    req,
    res
  ) => {

    try {

      const startedAt =
        Date.now();


      const all =
        await getAllTimeframes();


      const data = {};
      const counts = {};


      for (
        const [
          timeframe,
          value
        ] of
          Object.entries(
            all
          )
      ) {

        data[
          timeframe
        ] =
          value.candles;


        counts[
          timeframe
        ] =
          value
            .candles
            .length;
      }


      res.set(
        "Cache-Control",
        "no-store"
      );


      res.json({

        ok:
          true,

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

      console.error(
        error
      );


      res
        .status(500)
        .json({

          ok:
            false,

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
  async (
    req,
    res
  ) => {

    try {

      const timeframe =
        req.params
          .timeframe
          .toLowerCase();


      if (
        !TIMEFRAMES[
          timeframe
        ]
      ) {

        return res
          .status(400)
          .json({

            ok:
              false,

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


      res.json(
        data
      );


    } catch (error) {

      console.error(
        error
      );


      res
        .status(500)
        .json({

          ok:
            false,

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

    console.log(
      `MCP endpoint: /mcp`
    );
  }
);
