import { describe, expect, it } from "vitest";
import { cockpitCanTrade, frameDataHealth, parseCockpitState } from "./cockpitState";

const SNAPSHOT = {
  schema_version: "1.0",
  snapshot_id: "mkt_1",
  occurred_at_utc: "2026-07-20T13:45:00Z",
  symbol: "QQQ.US",
  price: "500.00",
  open: "498.10",
  vwap: "499.40",
  sequence_number: 1,
  data_health: "HEALTHY",
};

function frame(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    schema_version: "1.0",
    seq: 1,
    session_id: "live",
    server_time_utc: "2026-07-20T13:45:00Z",
    connection: "LIVE",
    new_position_allowed: true,
    snapshot: SNAPSHOT,
    regime: { regime: "Trend", trend_score: 3, range_score: 0 },
    vol: { iv_hv_state: "IV Fair", interpretation: "Long Vol", realized_move: 0.009 },
    signal: {
      strategy: "LongGamma",
      regime: "Trend",
      initial_risk_status: "PASSED",
      reason: ["ok"],
      rule_version: "r1",
    },
    risk_flags: [],
    ...overrides,
  };
}

describe("parseCockpitState", () => {
  it("parses a complete LIVE frame", () => {
    const parsed = parseCockpitState(frame());
    expect(parsed?.connection).toBe("LIVE");
    expect(parsed?.signal?.strategy).toBe("LongGamma");
    expect(parsed?.snapshot?.price).toBe("500.00");
  });

  it("parses a fail-closed DISCONNECTED frame with all-null derivations", () => {
    const parsed = parseCockpitState(
      frame({
        connection: "DISCONNECTED",
        new_position_allowed: false,
        snapshot: null,
        regime: null,
        vol: null,
        signal: null,
        risk_flags: ["stream ended"],
      }),
    );
    expect(parsed).not.toBeNull();
    expect(parsed?.snapshot).toBeNull();
    expect(parsed?.new_position_allowed).toBe(false);
  });

  it("returns null on schema_version drift", () => {
    expect(parseCockpitState(frame({ schema_version: "2.0" }))).toBeNull();
  });

  it("returns null on an unknown connection enum", () => {
    expect(parseCockpitState(frame({ connection: "WOBBLY" }))).toBeNull();
  });

  it("returns null when new_position_allowed is the wrong type", () => {
    expect(parseCockpitState(frame({ new_position_allowed: "true" }))).toBeNull();
  });

  it("returns null when seq is negative or non-integer", () => {
    expect(parseCockpitState(frame({ seq: -1 }))).toBeNull();
    expect(parseCockpitState(frame({ seq: 1.5 }))).toBeNull();
  });

  it("drops an invalid signal to null without failing the frame", () => {
    const parsed = parseCockpitState(frame({ signal: { strategy: "Bogus" } }));
    expect(parsed).not.toBeNull();
    expect(parsed?.signal).toBeNull();
  });

  it("rejects an unknown regime enum inside a signal (drops signal)", () => {
    const parsed = parseCockpitState(
      frame({ signal: { ...(frame().signal as object), regime: "Sideways" } }),
    );
    expect(parsed?.signal).toBeNull();
  });
});

describe("cockpitCanTrade", () => {
  it("true only when LIVE + tradable + snapshot HEALTHY + broker allowed", () => {
    const f = parseCockpitState(frame());
    expect(cockpitCanTrade({ frame: f, brokerAllowed: true })).toBe(true);
  });

  it("false when broker gate fails", () => {
    const f = parseCockpitState(frame());
    expect(cockpitCanTrade({ frame: f, brokerAllowed: false })).toBe(false);
  });

  it("false when frame is not LIVE", () => {
    const f = parseCockpitState(frame({ connection: "STALE", new_position_allowed: false }));
    expect(cockpitCanTrade({ frame: f, brokerAllowed: true })).toBe(false);
  });

  it("false when snapshot data_health is not HEALTHY", () => {
    const f = parseCockpitState(frame({ snapshot: { ...SNAPSHOT, data_health: "DEGRADED" } }));
    expect(cockpitCanTrade({ frame: f, brokerAllowed: true })).toBe(false);
  });

  it("false when frame is null", () => {
    expect(cockpitCanTrade({ frame: null, brokerAllowed: true })).toBe(false);
  });
});

describe("frameDataHealth", () => {
  it("returns the snapshot health when present", () => {
    expect(frameDataHealth(parseCockpitState(frame()))).toBe("HEALTHY");
  });
  it("returns STALE when frame or snapshot is absent", () => {
    expect(frameDataHealth(null)).toBe("STALE");
    expect(frameDataHealth(parseCockpitState(frame({ snapshot: null })))).toBe("STALE");
  });
});
