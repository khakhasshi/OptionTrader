import { afterEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, cleanup, act } from "@testing-library/react";
import { Cockpit } from "./Cockpit";
import { canOpenNewPosition, parseServiceHealth, type ServiceHealth } from "./health";
import { parseMarketSnapshot } from "./snapshot";

const HEALTHY: ServiceHealth = {
  schema_version: "1.0",
  status: "ok",
  service: "trading-core",
  data_health: "HEALTHY",
  broker_health: "HEALTHY",
  reconciled: true,
  new_position_allowed: true,
};

const SNAPSHOT = {
  schema_version: "1.0",
  snapshot_id: "mkt_20260720_094500_000123",
  occurred_at_utc: "2026-07-20T13:45:00Z",
  symbol: "QQQ.US",
  price: "500.00",
  open: "498.10",
  vwap: "499.40",
  sequence_number: 123,
  data_health: "HEALTHY",
};

const EVENT_CONTEXT = {
  schema_version: "1.0",
  event_context_id: "evtctx_test",
  trading_date: "2026-07-20",
  generated_at_utc: "2026-07-20T13:45:00Z",
  available: true,
  source_documents: ["macro", "holdings", "earnings", "news"].map((category) => ({
    category,
    source: "test-source",
    source_timestamp_utc: "2026-07-20T13:40:00Z",
    received_at_utc: "2026-07-20T13:41:00Z",
    confidence: 1,
    raw_ref: `fixture://${category}`,
  })),
  event_day_type: "Normal",
  macro_events: [],
  earnings_events: [],
  news_events: [],
  qqq_weighted_event_score: "0.0000",
  minutes_to_major_event: 1440,
  event_released: false,
  risk_flags: ["NO_NAKED_0DTE"],
  deterministic_context_summary: "day=Normal; fixture",
};

// A LIVE cockpit frame (data/decision dimension) mirroring cockpit_state.json.
function frame(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    schema_version: "1.0",
    seq: 1,
    session_id: "live",
    server_time_utc: "2026-07-20T13:45:00Z",
    connection: "LIVE",
    new_position_allowed: true,
    snapshot: SNAPSHOT,
    regime: { regime: "Trend", trend_score: 3, range_score: 0, components: {}, unavailable: [] },
    vol: { iv_hv_state: "IV Fair", interpretation: "Long Vol", realized_move: 0.009 },
    signal: {
      schema_version: "1.0",
      signal_id: "sig_x",
      session_id: "live",
      occurred_at_utc: "2026-07-20T13:45:00Z",
      regime: "Trend",
      strategy: "LongGamma",
      initial_risk_status: "PASSED",
      reason: ["Trend + IV cheap/fair + breakout in allowed window"],
      rule_version: "phase1-test",
    },
    event_context: EVENT_CONTEXT,
    risk_flags: [],
    ...overrides,
  };
}

// --- Mock WebSocket (jsdom has none) ---------------------------------------
class MockWebSocket {
  static instances: MockWebSocket[] = [];
  static autoOpen = true;
  static last(): MockWebSocket {
    return MockWebSocket.instances[MockWebSocket.instances.length - 1];
  }
  onopen: (() => void) | null = null;
  onmessage: ((e: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  readyState = 0;
  constructor(public url: string) {
    MockWebSocket.instances.push(this);
    if (MockWebSocket.autoOpen) {
      setTimeout(() => {
        this.readyState = 1;
        this.onopen?.();
      }, 0);
    }
  }
  emit(f: unknown): void {
    this.onmessage?.({ data: JSON.stringify(f) });
  }
  emitRaw(data: string): void {
    this.onmessage?.({ data });
  }
  close(): void {
    this.readyState = 3;
    this.onclose?.();
  }
}

/** Route fetches by URL: `/core/health` and `/cockpit/state` recovery. */
function mockFetch(opts: { health?: unknown | "fail"; recovery?: unknown | "fail" }) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => {
      const which = url.includes("/core/health") ? opts.health : opts.recovery;
      if (which === "fail" || which === undefined) throw new Error("network");
      return { ok: true, json: async () => which } as Response;
    }),
  );
}

function installWebSocket() {
  MockWebSocket.instances = [];
  vi.stubGlobal("WebSocket", MockWebSocket as unknown as typeof WebSocket);
}

/** Wait for the WS to open, then push one frame. */
async function pushFrame(f: Record<string, unknown>): Promise<void> {
  await waitFor(() => expect(MockWebSocket.last()).toBeDefined());
  await act(async () => {
    MockWebSocket.last().onopen?.();
    MockWebSocket.last().emit(f);
  });
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  MockWebSocket.instances = [];
  MockWebSocket.autoOpen = true;
});

// --- Pure gate: the fail-closed conjunction --------------------------------
describe("canOpenNewPosition", () => {
  it("all healthy + reconciled + gateway allows -> ALLOWED", () => {
    expect(canOpenNewPosition({ reachable: true, health: HEALTHY })).toBe(true);
  });

  it("gateway veto (new_position_allowed=false) -> No Trade", () => {
    expect(
      canOpenNewPosition({ reachable: true, health: { ...HEALTHY, new_position_allowed: false } }),
    ).toBe(false);
  });

  it("broker disconnected -> No Trade", () => {
    expect(
      canOpenNewPosition({ reachable: true, health: { ...HEALTHY, broker_health: "DISCONNECTED" } }),
    ).toBe(false);
  });

  it("not reconciled -> No Trade", () => {
    expect(canOpenNewPosition({ reachable: true, health: { ...HEALTHY, reconciled: false } })).toBe(
      false,
    );
  });

  it("data STALE -> No Trade", () => {
    expect(
      canOpenNewPosition({ reachable: true, health: { ...HEALTHY, data_health: "STALE" } }),
    ).toBe(false);
  });

  it("status unreachable -> No Trade", () => {
    expect(
      canOpenNewPosition({ reachable: true, health: { ...HEALTHY, status: "unreachable" } }),
    ).toBe(false);
  });

  it("unreachable / null health -> No Trade", () => {
    expect(canOpenNewPosition({ reachable: false, health: null })).toBe(false);
    expect(canOpenNewPosition({ reachable: false, health: HEALTHY })).toBe(false);
  });
});

// --- Strict parser: missing field / wrong type / bad enum -> null ----------
describe("parseServiceHealth", () => {
  it("parses a complete valid body", () => {
    expect(parseServiceHealth(HEALTHY)?.new_position_allowed).toBe(true);
  });

  it("returns null when new_position_allowed is missing", () => {
    const { new_position_allowed: _omit, ...partial } = HEALTHY;
    expect(parseServiceHealth(partial)).toBeNull();
  });

  it("returns null when new_position_allowed is the wrong type", () => {
    expect(parseServiceHealth({ ...HEALTHY, new_position_allowed: "true" })).toBeNull();
  });

  it("returns null on an unknown broker_health enum", () => {
    expect(parseServiceHealth({ ...HEALTHY, broker_health: "WOBBLY" })).toBeNull();
  });

  it("returns null on a non-object body", () => {
    expect(parseServiceHealth(null)).toBeNull();
    expect(parseServiceHealth("nope")).toBeNull();
  });
});

describe("parseMarketSnapshot", () => {
  it("parses a complete valid snapshot", () => {
    expect(parseMarketSnapshot(SNAPSHOT)?.snapshot_id).toBe(SNAPSHOT.snapshot_id);
  });

  it("returns null on the BFF fail-closed body (missing required fields)", () => {
    expect(
      parseMarketSnapshot({ schema_version: "1.0", status: "unreachable", data_health: "STALE" }),
    ).toBeNull();
  });

  it("returns null when sequence_number is the wrong type", () => {
    expect(parseMarketSnapshot({ ...SNAPSHOT, sequence_number: "123" })).toBeNull();
  });

  it("returns null when schema_version is missing", () => {
    const { schema_version: _omit, ...partial } = SNAPSHOT;
    expect(parseMarketSnapshot(partial)).toBeNull();
  });

  it("returns null when schema_version is not 1.0", () => {
    expect(parseMarketSnapshot({ ...SNAPSHOT, schema_version: "2.0" })).toBeNull();
  });

  it("returns null on a non-numeric decimal (nan/inf)", () => {
    expect(parseMarketSnapshot({ ...SNAPSHOT, price: "nan" })).toBeNull();
    expect(parseMarketSnapshot({ ...SNAPSHOT, price: "inf" })).toBeNull();
    expect(parseMarketSnapshot({ ...SNAPSHOT, vwap: "1.2.3" })).toBeNull();
  });

  it("returns null when sequence_number is negative or non-integer", () => {
    expect(parseMarketSnapshot({ ...SNAPSHOT, sequence_number: -1 })).toBeNull();
    expect(parseMarketSnapshot({ ...SNAPSHOT, sequence_number: 1.5 })).toBeNull();
  });

  it("returns null on an invalid occurred_at_utc", () => {
    expect(parseMarketSnapshot({ ...SNAPSHOT, occurred_at_utc: "not-a-time" })).toBeNull();
    expect(parseMarketSnapshot({ ...SNAPSHOT, occurred_at_utc: "2026-07-20T13:45:00" })).toBeNull();
  });
});

// --- Rendered Cockpit: trading gate end to end -----------------------------
async function tradingText(): Promise<string> {
  const badge = await screen.findByRole("status", { name: /^Trading:/ });
  return badge.getAttribute("aria-label") ?? "";
}

describe("Cockpit trading badge", () => {
  it("shows ALLOWED when broker healthy AND stream frame LIVE + tradable", async () => {
    mockFetch({ health: HEALTHY });
    installWebSocket();
    render(<Cockpit />);
    await pushFrame(frame());
    await waitFor(async () => expect(await tradingText()).toBe("Trading: ALLOWED"));
  });

  it("shows No Trade when the broker gateway vetoes even if the frame is LIVE", async () => {
    mockFetch({ health: { ...HEALTHY, new_position_allowed: false } });
    installWebSocket();
    render(<Cockpit />);
    await pushFrame(frame());
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
  });

  it("shows No Trade when broker disconnected", async () => {
    mockFetch({ health: { ...HEALTHY, broker_health: "DISCONNECTED" } });
    installWebSocket();
    render(<Cockpit />);
    await pushFrame(frame());
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
  });

  it("shows No Trade when the frame is not tradable (new_position_allowed=false)", async () => {
    mockFetch({ health: HEALTHY });
    installWebSocket();
    render(<Cockpit />);
    await pushFrame(frame({ new_position_allowed: false, connection: "STALE" }));
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
  });

  it("shows No Trade when the frame snapshot data_health is not HEALTHY", async () => {
    mockFetch({ health: HEALTHY });
    installWebSocket();
    render(<Cockpit />);
    await pushFrame(frame({ snapshot: { ...SNAPSHOT, data_health: "STALE" } }));
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
  });

  it("shows No Trade + OFFLINE when the core health request fails", async () => {
    mockFetch({ health: "fail" });
    installWebSocket();
    render(<Cockpit />);
    await pushFrame(frame());
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
    expect(
      await screen.findByRole("status", { name: "Connection: OFFLINE (read-only)" }),
    ).toBeInTheDocument();
  });

  it("shows No Trade when broker healthy but no frame has arrived yet", async () => {
    mockFetch({ health: HEALTHY });
    installWebSocket();
    render(<Cockpit />);
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
  });
});

// --- Rendered Cockpit: decision + snapshot come from the stream frame ------
describe("Cockpit stream frame rendering", () => {
  it("renders regime, strategy, and snapshot fields from the frame", async () => {
    mockFetch({ health: HEALTHY });
    installWebSocket();
    render(<Cockpit />);
    await pushFrame(frame());
    expect(await screen.findByLabelText("Regime: Trend")).toBeInTheDocument();
    expect(await screen.findByLabelText("Strategy: LongGamma")).toBeInTheDocument();
    expect(await screen.findByLabelText(`Price: ${SNAPSHOT.price}`)).toBeInTheDocument();
    expect(await screen.findByLabelText("Snapshot Data Health: HEALTHY")).toBeInTheDocument();
  });

  it("proves values are data-driven: a different frame price renders that price", async () => {
    mockFetch({ health: HEALTHY });
    installWebSocket();
    render(<Cockpit />);
    await pushFrame(frame({ snapshot: { ...SNAPSHOT, price: "512.34" } }));
    expect(await screen.findByLabelText("Price: 512.34")).toBeInTheDocument();
  });

  it("shows STALE/unavailable snapshot when the stream is not live", async () => {
    mockFetch({ health: HEALTHY });
    installWebSocket();
    render(<Cockpit />);
    await pushFrame(frame({ connection: "DISCONNECTED", new_position_allowed: false, snapshot: null }));
    expect(
      await screen.findByRole("status", { name: "Market Snapshot: unavailable" }),
    ).toBeInTheDocument();
  });

  it("surfaces risk flags from the frame", async () => {
    mockFetch({ health: HEALTHY });
    installWebSocket();
    render(<Cockpit />);
    await pushFrame(
      frame({
        connection: "STALE",
        new_position_allowed: false,
        risk_flags: ["new positions blocked: data_health=STALE"],
      }),
    );
    expect(
      await screen.findByText("new positions blocked: data_health=STALE"),
    ).toBeInTheDocument();
  });

  it("appends signals to the signal log", async () => {
    mockFetch({ health: HEALTHY });
    installWebSocket();
    render(<Cockpit />);
    await pushFrame(frame({ seq: 1 }));
    await act(async () => {
      MockWebSocket.last().emit(frame({ seq: 2, signal: { ...(frame().signal as object), strategy: "NoTrade", regime: "Range" } }));
    });
    const log = await screen.findByRole("list", { name: "Signal Log" });
    expect(log.querySelectorAll("li").length).toBe(2);
  });

  it("drops to DISCONNECTED and No Trade when the socket closes", async () => {
    mockFetch({ health: HEALTHY });
    installWebSocket();
    render(<Cockpit />);
    await pushFrame(frame());
    await waitFor(async () => expect(await tradingText()).toBe("Trading: ALLOWED"));
    await act(async () => {
      MockWebSocket.last().close();
    });
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
    expect(await screen.findByLabelText("Link: DISCONNECTED")).toBeInTheDocument();
  });
});

// --- P0-2 fail-open window fixes -------------------------------------------
describe("Cockpit fail-closed edges", () => {
  it("malformed frame fails closed: No Trade + link DISCONNECTED (not a stale LIVE)", async () => {
    mockFetch({ health: HEALTHY });
    installWebSocket();
    render(<Cockpit />);
    await pushFrame(frame()); // establish a LIVE tradable frame
    await waitFor(async () => expect(await tradingText()).toBe("Trading: ALLOWED"));
    // A garbage frame must drop the link and clear the frame, not be ignored.
    await act(async () => {
      MockWebSocket.last().emitRaw("{ not json");
    });
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
    expect(await screen.findByLabelText("Link: DISCONNECTED")).toBeInTheDocument();
  });

  it("schema-invalid frame also fails closed", async () => {
    mockFetch({ health: HEALTHY });
    installWebSocket();
    render(<Cockpit />);
    await pushFrame(frame());
    await waitFor(async () => expect(await tradingText()).toBe("Trading: ALLOWED"));
    await act(async () => {
      MockWebSocket.last().emit(frame({ connection: "WOBBLY" })); // bad enum -> parse null
    });
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
  });

  it("a late recovery response cannot overwrite a newer WS frame (seq arbitration)", async () => {
    // Recovery returns an OLD frame (seq 1); the WS has already delivered seq 5.
    mockFetch({ health: HEALTHY, recovery: frame({ seq: 1, snapshot: { ...SNAPSHOT, price: "111.11" } }) });
    installWebSocket();
    render(<Cockpit />);
    await pushFrame(frame({ seq: 5, snapshot: { ...SNAPSHOT, price: "555.55" } }));
    // Even if the (older) recovery resolves now, the newer price must remain.
    await waitFor(() => expect(screen.queryByLabelText("Price: 555.55")).toBeInTheDocument());
    expect(screen.queryByLabelText("Price: 111.11")).not.toBeInTheDocument();
  });

  it("does not show ALLOWED while link is CONNECTING even with a tradable frame", async () => {
    // Recovery delivers a tradable frame BEFORE the socket opens. With auto-open
    // suppressed the link stays CONNECTING, so the gate must still be No Trade.
    MockWebSocket.autoOpen = false;
    mockFetch({ health: HEALTHY, recovery: frame() });
    installWebSocket();
    MockWebSocket.autoOpen = false; // installWebSocket resets instances only
    render(<Cockpit />);
    await waitFor(() => expect(MockWebSocket.last()).toBeDefined());
    // Give the recovery fetch a chance to resolve and set the frame.
    await act(async () => {
      await Promise.resolve();
    });
    // link is CONNECTING (onopen never fired) -> gate must be No Trade.
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
  });
});
