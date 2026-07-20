import { afterEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, cleanup } from "@testing-library/react";
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

/**
 * Route the two Cockpit fetches by URL. `health` may be a body object, or
 * "fail" to reject the request. `snapshot` likewise.
 */
function mockFetch(opts: { health?: unknown | "fail"; snapshot?: unknown | "fail" }) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => {
      const which = url.includes("/health") ? opts.health : opts.snapshot;
      if (which === "fail" || which === undefined) throw new Error("network");
      return { ok: true, json: async () => which } as Response;
    }),
  );
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
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
      canOpenNewPosition({
        reachable: true,
        health: { ...HEALTHY, broker_health: "DISCONNECTED" },
      }),
    ).toBe(false);
  });

  it("not reconciled -> No Trade", () => {
    expect(
      canOpenNewPosition({ reachable: true, health: { ...HEALTHY, reconciled: false } }),
    ).toBe(false);
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
  it("shows ALLOWED when all healthy, reconciled, and gateway allows", async () => {
    mockFetch({ health: HEALTHY, snapshot: SNAPSHOT });
    render(<Cockpit />);
    await waitFor(async () => expect(await tradingText()).toBe("Trading: ALLOWED"));
  });

  it("shows No Trade when the gateway vetoes (new_position_allowed=false)", async () => {
    mockFetch({ health: { ...HEALTHY, new_position_allowed: false }, snapshot: SNAPSHOT });
    render(<Cockpit />);
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
  });

  it("shows No Trade when new_position_allowed field is missing", async () => {
    const { new_position_allowed: _omit, ...partial } = HEALTHY;
    mockFetch({ health: partial, snapshot: SNAPSHOT });
    render(<Cockpit />);
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
  });

  it("shows No Trade when broker disconnected", async () => {
    mockFetch({ health: { ...HEALTHY, broker_health: "DISCONNECTED" }, snapshot: SNAPSHOT });
    render(<Cockpit />);
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
  });

  it("shows No Trade when not reconciled", async () => {
    mockFetch({ health: { ...HEALTHY, reconciled: false }, snapshot: SNAPSHOT });
    render(<Cockpit />);
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
  });

  it("shows No Trade when data is STALE", async () => {
    mockFetch({ health: { ...HEALTHY, data_health: "STALE" }, snapshot: SNAPSHOT });
    render(<Cockpit />);
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
  });

  it("shows No Trade + OFFLINE when the core request fails", async () => {
    mockFetch({ health: "fail", snapshot: "fail" });
    render(<Cockpit />);
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
    expect(
      await screen.findByRole("status", { name: "Connection: OFFLINE (read-only)" }),
    ).toBeInTheDocument();
  });

  it("shows No Trade when health is all-green but the snapshot fetch fails", async () => {
    // P1: no trustworthy price -> No Trade even though Connection is ONLINE.
    mockFetch({ health: HEALTHY, snapshot: "fail" });
    render(<Cockpit />);
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
    expect(
      await screen.findByRole("status", { name: "Connection: ONLINE" }),
    ).toBeInTheDocument();
  });

  it("shows No Trade when health is all-green but the snapshot is STALE", async () => {
    mockFetch({ health: HEALTHY, snapshot: { ...SNAPSHOT, data_health: "STALE" } });
    render(<Cockpit />);
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
  });

  it("shows OFFLINE when BFF reports status=unreachable even if body parses", async () => {
    mockFetch({
      health: {
        schema_version: "1.0",
        status: "unreachable",
        service: "trading-core",
        data_health: "STALE",
        broker_health: "DISCONNECTED",
        reconciled: false,
        new_position_allowed: false,
      },
      snapshot: "fail",
    });
    render(<Cockpit />);
    await waitFor(async () =>
      expect(
        await screen.findByRole("status", { name: "Connection: OFFLINE (read-only)" }),
      ).toBeInTheDocument(),
    );
    expect(await tradingText()).toBe("Trading: No Trade");
  });
});

// --- Rendered Cockpit: snapshot comes from the BFF, not a local fixture ----
describe("Cockpit market snapshot", () => {
  it("renders snapshot_id, symbol, price, data_health from the BFF response", async () => {
    mockFetch({ health: HEALTHY, snapshot: SNAPSHOT });
    render(<Cockpit />);
    // The rendered values must equal what the mocked BFF returned.
    expect(
      await screen.findByLabelText(`Snapshot ID: ${SNAPSHOT.snapshot_id}`),
    ).toBeInTheDocument();
    expect(await screen.findByLabelText(`Symbol: ${SNAPSHOT.symbol}`)).toBeInTheDocument();
    expect(await screen.findByLabelText(`Price: ${SNAPSHOT.price}`)).toBeInTheDocument();
    expect(await screen.findByLabelText("Snapshot Data Health: HEALTHY")).toBeInTheDocument();
  });

  it("proves the value is data-driven: a different BFF price renders that price", async () => {
    mockFetch({ health: HEALTHY, snapshot: { ...SNAPSHOT, price: "512.34" } });
    render(<Cockpit />);
    expect(await screen.findByLabelText("Price: 512.34")).toBeInTheDocument();
  });

  it("shows STALE/unavailable when snapshot fetch fails", async () => {
    mockFetch({ health: HEALTHY, snapshot: "fail" });
    render(<Cockpit />);
    expect(
      await screen.findByRole("status", { name: "Market Snapshot: unavailable" }),
    ).toBeInTheDocument();
  });

  it("shows STALE/unavailable when snapshot data_health is not HEALTHY", async () => {
    mockFetch({ health: HEALTHY, snapshot: { ...SNAPSHOT, data_health: "STALE" } });
    render(<Cockpit />);
    expect(
      await screen.findByRole("status", { name: "Market Snapshot: unavailable" }),
    ).toBeInTheDocument();
  });
});
