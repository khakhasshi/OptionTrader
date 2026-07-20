import { afterEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, cleanup } from "@testing-library/react";
import { Cockpit } from "./Cockpit";
import { canOpenNewPosition, type ServiceHealth } from "./health";

const HEALTHY: ServiceHealth = {
  status: "ok",
  service: "trading-core",
  data_health: "HEALTHY",
  broker_health: "HEALTHY",
  reconciled: true,
  new_position_allowed: true,
};

function mockFetchOnce(ok: boolean, body?: unknown) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => {
      if (!ok) throw new Error("network");
      return { ok: true, json: async () => body } as Response;
    }),
  );
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

// --- Pure gate: the fail-closed conjunction, 5 required scenarios ----------
describe("canOpenNewPosition", () => {
  it("all healthy + reconciled -> ALLOWED", () => {
    expect(canOpenNewPosition({ reachable: true, health: HEALTHY })).toBe(true);
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

  it("API/core unreachable -> No Trade", () => {
    expect(canOpenNewPosition({ reachable: false, health: null })).toBe(false);
    // even if a stale body somehow lingers, unreachable forces No Trade
    expect(canOpenNewPosition({ reachable: false, health: HEALTHY })).toBe(false);
  });

  it("degraded broker -> No Trade", () => {
    expect(
      canOpenNewPosition({ reachable: true, health: { ...HEALTHY, broker_health: "DEGRADED" } }),
    ).toBe(false);
  });
});

// --- Rendered Cockpit: same 5 scenarios end to end ------------------------
async function tradingText(): Promise<string> {
  const badge = await screen.findByRole("status", { name: /^Trading:/ });
  return badge.getAttribute("aria-label") ?? "";
}

describe("Cockpit trading badge", () => {
  it("shows ALLOWED when all healthy and reconciled", async () => {
    mockFetchOnce(true, HEALTHY);
    render(<Cockpit />);
    await waitFor(async () => expect(await tradingText()).toBe("Trading: ALLOWED"));
  });

  it("shows No Trade when broker disconnected", async () => {
    mockFetchOnce(true, { ...HEALTHY, broker_health: "DISCONNECTED" });
    render(<Cockpit />);
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
  });

  it("shows No Trade when not reconciled", async () => {
    mockFetchOnce(true, { ...HEALTHY, reconciled: false });
    render(<Cockpit />);
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
  });

  it("shows No Trade when data is STALE", async () => {
    mockFetchOnce(true, { ...HEALTHY, data_health: "STALE" });
    render(<Cockpit />);
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
  });

  it("shows No Trade when the core request fails", async () => {
    mockFetchOnce(false);
    render(<Cockpit />);
    await waitFor(async () => expect(await tradingText()).toBe("Trading: No Trade"));
    expect(await screen.findByRole("status", { name: "Connection: OFFLINE (read-only)" }))
      .toBeInTheDocument();
  });
});
