import { useEffect, useState } from "react";
import {
  canOpenNewPosition,
  type BrokerHealth,
  type DataHealth,
  type ServiceHealth,
} from "./health";

/**
 * Phase 0 Cockpit skeleton. Polls trading-core health via the API proxy.
 *
 * A new position is only permitted when the core is reachable AND data is
 * HEALTHY AND broker is HEALTHY AND the book is reconciled (see
 * canOpenNewPosition). Anything else — a degraded broker, an unreconciled
 * book, stale data, or a failed fetch — shows No Trade. Fail closed in the UI.
 */
export function Cockpit() {
  const [health, setHealth] = useState<ServiceHealth | null>(null);
  const [reachable, setReachable] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      try {
        const r = await fetch("/api/v1/core/health");
        if (!r.ok) throw new Error(String(r.status));
        const body = (await r.json()) as ServiceHealth;
        if (!cancelled) {
          setHealth(body);
          setReachable(true);
        }
      } catch {
        // Fail closed: drop reachable AND clear the last body so no stale
        // HEALTHY snapshot lingers behind an OFFLINE connection.
        if (!cancelled) {
          setReachable(false);
          setHealth(null);
        }
      }
    };
    poll();
    const id = setInterval(poll, 2000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  const dataHealth: DataHealth = reachable && health ? health.data_health : "STALE";
  const brokerHealth: BrokerHealth =
    reachable && health ? health.broker_health : "DISCONNECTED";
  const reconciled = reachable && health ? health.reconciled === true : false;
  const canTrade = canOpenNewPosition({ reachable, health });

  return (
    <main style={{ fontFamily: "system-ui", padding: 24 }}>
      <h1>OptionTrader Cockpit</h1>
      <p style={{ color: "#5d6677" }}>Phase 0 skeleton — QQQ intraday volatility trading</p>
      <section style={{ display: "flex", gap: 16, marginTop: 16, flexWrap: "wrap" }}>
        <Badge
          label="Connection"
          value={reachable ? "ONLINE" : "OFFLINE (read-only)"}
          ok={reachable}
        />
        <Badge label="Data Health" value={dataHealth} ok={dataHealth === "HEALTHY"} />
        <Badge
          label="Broker Health"
          value={brokerHealth}
          ok={brokerHealth === "HEALTHY"}
        />
        <Badge
          label="Reconciliation"
          value={reconciled ? "RECONCILED" : "NOT RECONCILED"}
          ok={reconciled}
        />
        <Badge
          label="Trading"
          value={canTrade ? "ALLOWED" : "No Trade"}
          ok={canTrade}
        />
      </section>
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
