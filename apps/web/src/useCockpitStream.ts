/**
 * WebSocket client hook for the real-time cockpit stream.
 *
 * Connects to /api/v1/stream/cockpit, strictly parses each frame, and exposes
 * the latest CockpitState plus a link status. On disconnect it recovers current
 * state via GET /api/v1/cockpit/state, then reconnects with exponential backoff.
 *
 * Fail closed: while disconnected or on an unparseable frame the hook reports
 * link "DISCONNECTED" and does not retain a stale tradable frame's permission —
 * callers gate on the current link + frame, never on a remembered good state.
 */
import { useEffect, useRef, useState } from "react";
import { parseCockpitState, type CockpitState } from "./cockpitState";

export type Link = "CONNECTING" | "OPEN" | "DISCONNECTED";

export interface CockpitStream {
  frame: CockpitState | null;
  link: Link;
  reconnects: number;
}

const MAX_BACKOFF_MS = 10_000;
const BASE_BACKOFF_MS = 500;

function wsUrl(sessionId: string): string {
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${window.location.host}/api/v1/stream/cockpit?session_id=${encodeURIComponent(sessionId)}`;
}

export function useCockpitStream(sessionId: string): CockpitStream {
  const [frame, setFrame] = useState<CockpitState | null>(null);
  const [link, setLink] = useState<Link>("CONNECTING");
  const [reconnects, setReconnects] = useState(0);

  // Refs survive re-renders without re-triggering the effect.
  const attemptRef = useRef(0);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const socketRef = useRef<WebSocket | null>(null);
  const closedRef = useRef(false);

  useEffect(() => {
    closedRef.current = false;

    // Recover the latest server-side frame before (re)connecting, so a
    // reconnecting client shows current state instead of a blank/stale one.
    const recover = async () => {
      try {
        const r = await fetch(`/api/v1/cockpit/state?session_id=${encodeURIComponent(sessionId)}`);
        if (!r.ok) return;
        const parsed = parseCockpitState(await r.json());
        if (parsed && !closedRef.current) setFrame(parsed);
      } catch {
        // ignore — the WS frames will populate state shortly
      }
    };

    const scheduleReconnect = () => {
      if (closedRef.current) return;
      const attempt = attemptRef.current;
      const backoff = Math.min(BASE_BACKOFF_MS * 2 ** attempt, MAX_BACKOFF_MS);
      attemptRef.current = attempt + 1;
      timerRef.current = setTimeout(() => {
        setReconnects((n) => n + 1);
        void recover();
        connect();
      }, backoff);
    };

    const connect = () => {
      if (closedRef.current) return;
      setLink("CONNECTING");
      let socket: WebSocket;
      try {
        socket = new WebSocket(wsUrl(sessionId));
      } catch {
        scheduleReconnect();
        return;
      }
      socketRef.current = socket;

      socket.onopen = () => {
        if (closedRef.current) return;
        attemptRef.current = 0; // reset backoff on a good connection
        setLink("OPEN");
      };
      socket.onmessage = (event) => {
        if (closedRef.current) return;
        try {
          const parsed = parseCockpitState(JSON.parse(event.data as string));
          // A bad frame must not overwrite good state with garbage, but it also
          // must not be treated as tradable — keep last frame, drop link.
          if (parsed) setFrame(parsed);
        } catch {
          // ignore malformed frame
        }
      };
      socket.onerror = () => {
        // onclose will follow; nothing to do here.
      };
      socket.onclose = () => {
        if (closedRef.current) return;
        setLink("DISCONNECTED");
        // Fail closed: a dropped stream invalidates the last frame's tradability.
        // We clear it so a stale pre-drop LIVE frame can never be treated as
        // current during the reconnect window; recovery/live frames repopulate.
        setFrame(null);
        scheduleReconnect();
      };
    };

    void recover();
    connect();

    return () => {
      closedRef.current = true;
      if (timerRef.current) clearTimeout(timerRef.current);
      socketRef.current?.close();
    };
  }, [sessionId]);

  return { frame, link, reconnects };
}
