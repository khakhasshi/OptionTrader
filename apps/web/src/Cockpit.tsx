import { useEffect, useState } from "react";
import {
  canOpenNewPosition,
  parseServiceHealth,
  type BrokerHealth,
  type DataHealth,
  type ServiceHealth,
} from "./health";
import { isSnapshotLive, parseMarketSnapshot, type MarketSnapshot } from "./snapshot";

/**
 * Phase 0 Cockpit skeleton. Polls trading-core health AND the latest
 * MarketSnapshot via the Application BFF proxy.
 *
 * A new position is only permitted when the core reports status "ok" AND data
 * is HEALTHY AND broker is HEALTHY AND the book is reconciled AND the Rust
 * gateway set new_position_allowed=true (see canOpenNewPosition). Anything
 * else — a degraded broker, an unreconciled book, stale data, a gateway veto,
 * a malformed payload, or a failed fetch — shows No Trade. Fail closed.
 *
 * The snapshot fields shown here come entirely from the BFF response; there is
 * no local fixture. A missing/invalid snapshot or non-HEALTHY data_health is
 * shown as STALE / unavailable.
 */
export function Cockpit() {
  const [health, setHealth] = useState<ServiceHealth | null>(null);
  const [reachable, setReachable] = useState(false);
  const [snapshot, setSnapshot] = useState<MarketSnapshot | null>(null);

  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      // Health
      try {
        const r = await fetch("/api/v1/core/health");
        if (!r.ok) throw new Error(String(r.status));
        const parsed = parseServiceHealth(await r.json());
        // status "unreachable" or an unparseable body is NOT a live connection.
        if (!cancelled) {
          if (parsed && parsed.status === "ok") {
            setHealth(parsed);
            setReachable(true);
          } else {
            setHealth(parsed);
            setReachable(false);
          }
        }
      } catch {
        // Fail closed: drop reachable AND clear the last body so no stale
        // HEALTHY snapshot lingers behind an OFFLINE connection.
        if (!cancelled) {
          setReachable(false);
          setHealth(null);
        }
      }
      // Snapshot (independent: a bad snapshot must not mark the whole UI offline)
      try {
        const r = await fetch("/api/v1/market/snapshot");
        if (!r.ok) throw new Error(String(r.status));
        if (!cancelled) setSnapshot(parseMarketSnapshot(await r.json()));
      } catch {
        if (!cancelled) setSnapshot(null);
      }
    };
    poll();
    const id = setInterval(poll, 2000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  const online = reachable && health?.status === "ok";
  const dataHealth: DataHealth = online && health ? health.data_health : "STALE";
  const brokerHealth: BrokerHealth = online && health ? health.broker_health : "DISCONNECTED";
  const reconciled = online && health ? health.reconciled === true : false;
  const snapLive = isSnapshotLive(snapshot);
  // A tradable decision needs a live market snapshot too: if the snapshot fetch
  // failed or data_health != HEALTHY, we have no trustworthy price -> No Trade,
  // even when every health field is green. Fail closed.
  const canTrade = canOpenNewPosition({ reachable, health }) && snapLive;

  return (
    <main style={{ fontFamily: "system-ui", padding: 24 }}>
      <h1>OptionTrader Cockpit</h1>
      <p style={{ color: "#5d6677" }}>Phase 0 skeleton — QQQ intraday volatility trading</p>
      <section style={{ display: "flex", gap: 16, marginTop: 16, flexWrap: "wrap" }}>
        <Badge
          label="Connection"
          value={online ? "ONLINE" : "OFFLINE (read-only)"}
          ok={Boolean(online)}
        />
        <Badge label="Data Health" value={dataHealth} ok={dataHealth === "HEALTHY"} />
        <Badge label="Broker Health" value={brokerHealth} ok={brokerHealth === "HEALTHY"} />
        <Badge
          label="Reconciliation"
          value={reconciled ? "RECONCILED" : "NOT RECONCILED"}
          ok={reconciled}
        />
        <Badge label="Trading" value={canTrade ? "ALLOWED" : "No Trade"} ok={canTrade} />
      </section>

      <h2 style={{ marginTop: 28, fontSize: 18 }}>Market Snapshot</h2>
      {snapLive && snapshot ? (
        <section
          role="group"
          aria-label="Market Snapshot"
          style={{ display: "flex", gap: 16, marginTop: 12, flexWrap: "wrap" }}
        >
          <Field label="Snapshot ID" value={snapshot.snapshot_id} />
          <Field label="Symbol" value={snapshot.symbol} />
          <Field label="Price" value={snapshot.price} />
          <Field label="Snapshot Data Health" value={snapshot.data_health} />
        </section>
      ) : (
        <p role="status" aria-label="Market Snapshot: unavailable" style={{ color: "#b23", marginTop: 12 }}>
          Snapshot STALE / unavailable
        </p>
      )}
    </main>
  );
}

function Badge({ label, value, ok }: { label: string; value: string; ok: boolean }) {
  return (
    <div
      role="status"
      aria-label={`${label}: ${value}`}
      style={{
        border: "1px solid #d9dee8",
        borderRadius: 8,
        padding: "10px 14px",
        background: ok ? "#eef7ef" : "#fff1ef",
        minWidth: 160,
      }}
    >
      <div style={{ fontSize: 12, color: "#5d6677" }}>{label}</div>
      <strong>{value}</strong>
    </div>
  );
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div
      aria-label={`${label}: ${value}`}
      style={{
        border: "1px solid #d9dee8",
        borderRadius: 8,
        padding: "10px 14px",
        minWidth: 160,
      }}
    >
      <div style={{ fontSize: 12, color: "#5d6677" }}>{label}</div>
      <strong>{value}</strong>
    </div>
  );
}
