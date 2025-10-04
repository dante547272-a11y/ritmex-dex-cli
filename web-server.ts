// Remove this line - Bun is global
import type { TableRow, SpreadEntry } from "./src/types/table";
import type { EdgexFundingEntry } from "./src/types/edgex";
import type { LighterFundingEntry } from "./src/types/lighter";
import type { AsterFundingEntry } from "./src/types/aster";
import type { BackpackFundingEntry } from "./src/types/backpack";
import { buildTableRows } from "./src/utils/table";
import { calculateTopSpreads } from "./src/utils/spread";
import { fetchLighterFundingRates, fetchBinanceFundingInfo } from "./src/services/http/lighter";
import { fetchHyperliquidPredictedFundings, mapHlPerpToEntries } from "./src/services/http/hyperliquid";
import { fetchGrvtInstruments, fetchGrvtFundingPoint } from "./src/services/http/grvt";
import { fetchAsterFundingRates } from "./src/services/http/aster";
import { fetchBackpackFundingRates } from "./src/services/http/backpack";
import { loadConfigSync, type ExchangeKey } from "./src/utils/config";
import { LIGHTER_REFRESH_MS, ASTER_REFRESH_MS } from "./src/utils/constants";
import { parseNumber } from "./src/utils/format";
import type { EdgexWsMessage, EdgexQuoteEventMessage } from "./src/types/edgex";

interface WebSocketMessage {
  type: 'filter_update';
  enabledExchanges: string[];
  capitalAmount: number;
}

interface WebSocketData {
  rows: TableRow[];
  spreads: SpreadEntry[];
  status: {
    edgex: { connected: boolean; connecting: boolean; error: string | null };
    lighter: { refreshing: boolean; error: string | null; lastUpdated: Date | null };
    grvt: { refreshing: boolean; error: string | null; lastUpdated: Date | null };
    aster: { refreshing: boolean; error: string | null; lastUpdated: Date | null };
    backpack: { refreshing: boolean; error: string | null; lastUpdated: Date | null };
  };
  lastUpdated: Date | null;
}

class FundingDataManager {
  private edgexData: Record<string, EdgexFundingEntry> = {};
  private lighterRates: LighterFundingEntry[] = [];
  private grvtFunding: Record<string, number> = {};
  private asterRates: AsterFundingEntry[] = [];
  private backpackRates: BackpackFundingEntry[] = [];
  
  private edgexWs: WebSocket | null = null;
  private isEdgexConnecting = false;
  private isEdgexConnected = false;
  private edgexError: string | null = null;
  
  private lighterStatus = { refreshing: false, error: null as string | null, lastUpdated: null as Date | null };
  private grvtStatus = { refreshing: false, error: null as string | null, lastUpdated: null as Date | null };
  private asterStatus = { refreshing: false, error: null as string | null, lastUpdated: null as Date | null };
  private backpackStatus = { refreshing: false, error: null as string | null, lastUpdated: null as Date | null };
  
  private clients = new Map<WebSocket, { enabledExchanges: Set<string>; capitalAmount: number }>();
  private config = loadConfigSync();

  constructor() {
    this.initEdgexConnection();
    this.initLighterFetcher();
    this.initGrvtFetcher();
    this.initAsterFetcher();
    this.initBackpackFetcher();
  }

  addClient(ws: WebSocket) {
    this.clients.set(ws, {
      enabledExchanges: new Set(['lighter', 'binance', 'hyperliquid', 'edgex', 'grvt', 'aster', 'backpack']),
      capitalAmount: 10000
    });
    // Send current data immediately
    this.sendDataToClient(ws);
  }

  removeClient(ws: WebSocket) {
    this.clients.delete(ws);
  }

  handleClientMessage(ws: WebSocket, message: string) {
    try {
      const data = JSON.parse(message) as WebSocketMessage;
      if (data.type === 'filter_update') {
        const clientState = this.clients.get(ws);
        if (clientState) {
          clientState.enabledExchanges = new Set(data.enabledExchanges);
          clientState.capitalAmount = data.capitalAmount;
          this.sendDataToClient(ws); // Send updated data for this client
        }
      }
    } catch (error) {
      console.error('Failed to parse client message:', error);
    }
  }

  private broadcast() {
    this.clients.forEach((clientState, client) => {
      if (client.readyState === WebSocket.OPEN) {
        const data = this.getCurrentData(clientState.enabledExchanges, clientState.capitalAmount);
        client.send(JSON.stringify(data));
      }
    });
  }

  private sendDataToClient(ws: WebSocket) {
    if (ws.readyState === WebSocket.OPEN) {
      const clientState = this.clients.get(ws);
      if (clientState) {
        const data = this.getCurrentData(clientState.enabledExchanges, clientState.capitalAmount);
        ws.send(JSON.stringify(data));
      }
    }
  }

  private getCurrentData(enabledExchanges: Set<string>, capitalAmount: number): WebSocketData {
    const rows = buildTableRows(
      this.edgexData,
      this.lighterRates,
      this.grvtFunding,
      this.asterRates,
      this.backpackRates
    );
    
    // Calculate spreads with filtered exchanges
    const spreads = this.calculateFilteredSpreads(rows, enabledExchanges, capitalAmount);
    
    return {
      rows,
      spreads,
      status: {
        edgex: {
          connected: this.isEdgexConnected,
          connecting: this.isEdgexConnecting,
          error: this.edgexError
        },
        lighter: this.lighterStatus,
        grvt: this.grvtStatus,
        aster: this.asterStatus,
        backpack: this.backpackStatus
      },
      lastUpdated: new Date()
    };
  }

  private calculateFilteredSpreads(rows: TableRow[], enabledExchanges: Set<string>, capitalAmount: number): SpreadEntry[] {
    const entries: SpreadEntry[] = [];
    
    const EXCHANGE_LABELS = {
      lighter: "Lighter",
      binance: "Binance", 
      hyperliquid: "Hyperliquid",
      edgex: "EdgeX",
      grvt: "GRVT",
      aster: "Aster",
      backpack: "Backpack"
    } as const;

    rows.forEach((row) => {
      const rateEntries: Array<{
        key: keyof TableRow;
        exchange: keyof typeof EXCHANGE_LABELS;
        value: number;
      }> = [];

      // Only include enabled exchanges with data
      if (enabledExchanges.has("lighter") && row.lighterFunding !== undefined) {
        rateEntries.push({ key: "lighterFunding", exchange: "lighter", value: row.lighterFunding });
      }
      if (enabledExchanges.has("binance") && row.binanceFunding !== undefined) {
        rateEntries.push({ key: "binanceFunding", exchange: "binance", value: row.binanceFunding });
      }
      if (enabledExchanges.has("hyperliquid") && row.hyperliquidFunding !== undefined) {
        rateEntries.push({ key: "hyperliquidFunding", exchange: "hyperliquid", value: row.hyperliquidFunding });
      }
      if (enabledExchanges.has("edgex") && row.edgexFunding !== undefined) {
        rateEntries.push({ key: "edgexFunding", exchange: "edgex", value: row.edgexFunding });
      }
      if (enabledExchanges.has("grvt") && row.grvtFunding !== undefined) {
        rateEntries.push({ key: "grvtFunding", exchange: "grvt", value: row.grvtFunding });
      }
      if (enabledExchanges.has("aster") && row.asterFunding !== undefined) {
        rateEntries.push({ key: "asterFunding", exchange: "aster", value: row.asterFunding });
      }
      if (enabledExchanges.has("backpack") && row.backpackFunding !== undefined) {
        rateEntries.push({ key: "backpackFunding", exchange: "backpack", value: row.backpackFunding });
      }

      if (rateEntries.length < 2) return;

      // Generate all profitable pairs for this symbol
      for (let i = 0; i < rateEntries.length - 1; i += 1) {
        for (let j = i + 1; j < rateEntries.length; j += 1) {
          const a = rateEntries[i]!;
          const b = rateEntries[j]!;

          const sell = a.value >= b.value ? a : b; // Higher rate = SELL (short)
          const buy = a.value >= b.value ? b : a;  // Lower rate = BUY (long)
          const diff = sell.value - buy.value;
          
          if (diff <= 0) continue;

          const estimated24hProfit = diff * 3; // 8h * 3 = 24h
          const estimated24hProfitAmount = capitalAmount > 0 ? capitalAmount * estimated24hProfit : undefined;

          entries.push({
            symbol: row.symbol,
            diff,
            high: { exchange: EXCHANGE_LABELS[sell.exchange], rate: sell.value },
            low: { exchange: EXCHANGE_LABELS[buy.exchange], rate: buy.value },
            estimated24hProfit,
            estimated24hProfitAmount,
          });
        }
      }
    });

    return entries
      .sort((a, b) => b.diff - a.diff)
      .slice(0, 10);
  }

  private initEdgexConnection() {
      if (this.edgexWs) {
        this.edgexWs.close();
      }

      this.isEdgexConnecting = true;
      this.isEdgexConnected = false;
      this.edgexError = null;

      try {
        this.edgexWs = new WebSocket(`${EDGEX_WS_ENDPOINT}?timestamp=${Date.now()}`);

        this.edgexWs.onopen = () => {
          this.isEdgexConnecting = false;
          this.isEdgexConnected = true;
          this.edgexError = null;
          this.edgexWs?.send(JSON.stringify({ type: "subscribe", channel: EDGEX_TICKER_CHANNEL }));
          this.broadcast();
        };

        this.edgexWs.onmessage = (event) => {
          this.handleEdgexMessage(event.data);
        };

        this.edgexWs.onerror = () => {
          this.edgexError = "EdgeX websocket connection failed";
          this.broadcast();
        };

        this.edgexWs.onclose = () => {
          this.isEdgexConnected = false;
          this.isEdgexConnecting = false;
          setTimeout(connect, RECONNECT_DELAY_MS);
          this.broadcast();
        };
      } catch (error) {
        this.edgexError = "Failed to establish EdgeX websocket connection";
        this.isEdgexConnected = false;
        this.isEdgexConnecting = false;
        setTimeout(connect, RECONNECT_DELAY_MS);
        this.broadcast();
      }
    };

    connect();
  }

  private handleEdgexMessage(data: any) {
    let message: EdgexWsMessage;
    try {
      if (typeof data === "string") {
        message = JSON.parse(data);
      } else if (data instanceof ArrayBuffer) {
        const text = new TextDecoder().decode(data);
        message = JSON.parse(text);
      } else {
        message = JSON.parse(String(data));
      }
    } catch (error) {
      console.error("Failed to parse EdgeX websocket payload", error);
      return;
    }

    if (message.type === "ping") {
      const time = (message.time ?? String(Date.now())) as string;
      this.edgexWs?.send(JSON.stringify({ type: "pong", time }));
      return;
    }

    if (message.type === "error") {
      this.edgexError = typeof message.content === "string" ? message.content : "EdgeX websocket error";
      this.broadcast();
      return;
    }

    if (message.type !== "quote-event") return;

    const quoteMessage = message as EdgexQuoteEventMessage;
    if (quoteMessage.channel !== "ticker.all.1s") return;

    const content = quoteMessage.content;
    if (!content || !Array.isArray(content.data)) return;

    const entries = content.data;
    const updates: Record<string, EdgexFundingEntry> = {};
    const MIN_NOTIONAL = 10_000;

    entries.forEach((entry) => {
      if (!entry?.contractId || !entry.contractName) return;

      const openInterest = parseNumber(entry.openInterest ?? undefined);
      const fundingRate = parseNumber(entry.fundingRate ?? undefined);
      const lastPrice = parseNumber(entry.lastPrice ?? undefined);

      const notional = openInterest !== null && lastPrice !== null ? openInterest * lastPrice : Number.NaN;

      if (
        openInterest === null ||
        lastPrice === null ||
        !Number.isFinite(openInterest) ||
        !Number.isFinite(lastPrice) ||
        !Number.isFinite(notional) ||
        notional < MIN_NOTIONAL ||
        fundingRate === null ||
        !Number.isFinite(fundingRate)
      ) {
        delete this.edgexData[entry.contractId];
        return;
      }

      const eightHourRate = fundingRate * 2;

      updates[entry.contractId] = {
        contractId: entry.contractId,
        contractName: entry.contractName,
        openInterest,
        fundingRate: eightHourRate,
        fundingRateTime: entry.fundingTime,
      };
    });

    const dataType = typeof content.dataType === "string" ? content.dataType.toLowerCase() : "";
    const isSnapshot = dataType === "snapshot";

    if (isSnapshot) {
      this.edgexData = updates;
    } else {
      this.edgexData = { ...this.edgexData, ...updates };
    }

    this.broadcast();
  }

  private async initLighterFetcher() {
    const fetchData = async () => {
      this.lighterStatus.refreshing = true;
      this.lighterStatus.error = null;
      this.broadcast();

      try {
        const [rates, binanceInfo, hlPredicted] = await Promise.all([
          fetchLighterFundingRates(),
          fetchBinanceFundingInfo().catch(() => new Map<string, number>()),
          fetchHyperliquidPredictedFundings().catch(() => []),
        ]);

        const hlEntries = mapHlPerpToEntries(hlPredicted).map((e) => ({
          market_id: -1,
          exchange: "hyperliquid",
          symbol: e.symbol,
          rate: e.rate,
        })) as LighterFundingEntry[];

        const normalized = [...rates, ...hlEntries].map((entry) => {
          if (entry.exchange === "binance") {
            const symbolKey = entry.symbol.toUpperCase();
            const hours = binanceInfo.get(symbolKey) ?? 8;
            const eightHourRate = typeof entry.rate === "number" ? entry.rate * (8 / hours) : entry.rate;
            return { ...entry, rate: eightHourRate } as typeof entry;
          }

          if (entry.exchange === "hyperliquid") {
            const eightHourRate = typeof entry.rate === "number" ? entry.rate * 8 : entry.rate;
            return { ...entry, rate: eightHourRate } as typeof entry;
          }

          return entry;
        });

        this.lighterRates = normalized;
        this.lighterStatus.refreshing = false;
        this.lighterStatus.lastUpdated = new Date();
        this.broadcast();
      } catch (error) {
        this.lighterStatus.error = (error as Error).message;
        this.lighterStatus.refreshing = false;
        this.broadcast();
      }
    };

    await fetchData();
    setInterval(fetchData, LIGHTER_REFRESH_MS);
  }

  private async initGrvtFetcher() {
    const fetchData = async () => {
      this.grvtStatus.refreshing = true;
      this.grvtStatus.error = null;
      this.broadcast();

      try {
        const instruments = await fetchGrvtInstruments();
        const fundingData: Record<string, number> = {};

        // 逐个获取每个合约的资金费率，避免频率限制
        for (const instrument of instruments) {
          try {
            const fundingPoint = await fetchGrvtFundingPoint(instrument.instrument);
            if (fundingPoint && fundingPoint.funding_rate !== undefined) {
              const normalizedSymbol = instrument.base.toUpperCase();
              // GRVT 的资金费率已经是8小时制
              fundingData[normalizedSymbol] = Number(fundingPoint.funding_rate);
            }
            // 添加延迟避免频率限制
            await new Promise(resolve => setTimeout(resolve, 100));
          } catch (error) {
            console.warn(`Failed to fetch GRVT funding for ${instrument.instrument}:`, error);
          }
        }

        this.grvtFunding = fundingData;
        this.grvtStatus.refreshing = false;
        this.grvtStatus.lastUpdated = new Date();
        this.broadcast();
      } catch (error) {
        this.grvtStatus.error = (error as Error).message;
        this.grvtStatus.refreshing = false;
        this.broadcast();
      }
    };

    await fetchData();
    setInterval(fetchData, LIGHTER_REFRESH_MS);
  }

  private async initAsterFetcher() {
    const fetchData = async () => {
      this.asterStatus.refreshing = true;
      this.asterStatus.error = null;
      this.broadcast();

      try {
        this.asterRates = await fetchAsterFundingRates();
        this.asterStatus.refreshing = false;
        this.asterStatus.lastUpdated = new Date();
        this.broadcast();
      } catch (error) {
        this.asterStatus.error = (error as Error).message;
        this.asterStatus.refreshing = false;
        this.broadcast();
      }
    };

    await fetchData();
    setInterval(fetchData, ASTER_REFRESH_MS);
  }

  private async initBackpackFetcher() {
    const fetchData = async () => {
      this.backpackStatus.refreshing = true;
      this.backpackStatus.error = null;
      this.broadcast();

      try {
        this.backpackRates = await fetchBackpackFundingRates();
        this.backpackStatus.refreshing = false;
        this.backpackStatus.lastUpdated = new Date();
        this.broadcast();
      } catch (error) {
        this.backpackStatus.error = (error as Error).message;
        this.backpackStatus.refreshing = false;
        this.broadcast();
      }
    };

    await fetchData();
    setInterval(fetchData, ASTER_REFRESH_MS);
  }
}

const dataManager = new FundingDataManager();

const server = Bun.serve({
  port: 3000,
  fetch(request, server) {
    const url = new URL(request.url);
    
    // WebSocket upgrade
    if (request.headers.get("upgrade") === "websocket") {
      return server.upgrade(request);
    }
    
    if (url.pathname === "/") {
      return new Response(Bun.file("./web/index.html"));
    }
    
    if (url.pathname === "/app.js") {
      return new Response(Bun.file("./web/app.js"));
    }
    
    if (url.pathname === "/style.css") {
      return new Response(Bun.file("./web/style.css"));
    }

    return new Response("Not found", { status: 404 });
  },
  websocket: {
    open(ws) {
      dataManager.addClient(ws);
    },
    message(ws, message) {
      dataManager.handleClientMessage(ws, message as string);
    },
    close(ws) {
      dataManager.removeClient(ws);
    },
  },
});

console.log(`🚀 Ritmex Funding Monitor Web Server running at http://localhost:${server.port}`);
