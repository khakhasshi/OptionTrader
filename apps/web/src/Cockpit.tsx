import { useEffect, useState } from "react";

type DataHealth = "HEALTHY" | "DEGRADED" | "STALE" | "DISCONNECTED" | "RECONCILING";

interface CoreHealth {
  status: string;
  service: string;
  data_health: DataHealth;
  broker_health: string;
}

/**
 * Phase 0 Cockpit skeleton. Polls trading-core health via the API proxy.
 * Trading is only permitted when data_health is HEALTHY; anything else
 * (including a failed fetch) shows STALE / No Trade — fail closed in the UI.
 */
export function Cockpit() {
  const [health, setHealth] = useState<CoreHealth | null>(null);
  const [reachable, setReachable] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      try {
        const r = await fetch("/api/v1/core/health");
        if (!r.ok) throw new Error(String(r.status));
        const body = (await r.json()) as CoreHealth;
        if (!cancelled) {
          setHealth(body);
          setReachable(true);
        }
      } catch {
        if (!cancelled) setReachable(false);
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
  const canTrade = reachable && dataHealth === "HEALTHY";

  return (
    <main style={{ fontFamily: "system-ui", padding: 24 }}>
      <h1>OptionTrader Cockpit</h1>
      <p style={{ color: "#5d6677" }}>Phase 0 skeleton — QQQ intraday volatility trading</p>
      <section style={{ display: "flex", gap: 16, marginTop: 16 }}>
        <Badge label="Connection" value={reachable ? "ONLINE" : "OFFLINE (read-only)"} ok={reachable} />
        <Badge label="Data Health" value={dataHealth} ok={dataHealth === "HEALTHY"} />
        <Badge
          label="Trading"
          value={canTrade ? "ALLOWED" : "No Trade / STALE"}
          ok={canTrade}
        />
      </section>
    </main>
  );
}

function Badge({ label, value, ok }: { label: string; value: string; ok: boolean }) {
  return (
    <div
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
