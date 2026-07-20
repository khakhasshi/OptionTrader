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
  // Generation bumps on every (re)connect; a recovery response from an older
  // generation is discarded so it cannot resurrect stale state.
  const genRef = useRef(0);
  // Highest frame seq applied so far. An older/equal-or-lower seq (from a late
  // recovery response) must never overwrite newer WS state.
  const lastSeqRef = useRef(-1);

  useEffect(() => {
    closedRef.current = false;

    // Arbiter: apply a frame only if it is at least as new as what we've shown.
    // A stale response (recovery landing after a newer WS frame) is dropped.
    const applyFrame = (parsed: CockpitState | null) => {
      if (closedRef.current || !parsed) return;
      if (parsed.seq < lastSeqRef.current) return; // never overwrite newer state
      lastSeqRef.current = parsed.seq;
      setFrame(parsed);
    };

    // Recover the latest server-side frame before (re)connecting, so a
    // reconnecting client shows current state instead of a blank/stale one.
    // Tagged with the connect generation; a response for a superseded
    // generation is ignored (seq arbitration is the second line of defence).
    const recover = async (gen: number) => {
      try {
        const r = await fetch(`/api/v1/cockpit/state?session_id=${encodeURIComponent(sessionId)}`);
        if (!r.ok) return;
        const parsed = parseCockpitState(await r.json());
        if (closedRef.current || gen !== genRef.current) return; // superseded
        applyFrame(parsed);
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
        connect();
      }, backoff);
    };

    const connect = () => {
      if (closedRef.current) return;
      const gen = (genRef.current += 1);
      setLink("CONNECTING");
      void recover(gen);
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
        let parsed: CockpitState | null = null;
        try {
          parsed = parseCockpitState(JSON.parse(event.data as string));
        } catch {
          parsed = null;
        }
        if (!parsed) {
          // Malformed/unparseable frame: fail closed. Drop the link, clear the
          // frame, and force a reconnect — never keep showing a prior LIVE frame
          // as tradable behind a broken stream.
          setLink("DISCONNECTED");
          setFrame(null);
          socket.close();
          return;
        }
        applyFrame(parsed);
      };
      socket.onerror = () => {
        // onclose will follow; nothing to do here.
      };
      socket.onclose = () => {
        if (closedRef.current) return;
        setLink("DISCONNECTED");
        // Fail closed: a dropped stream invalidates the last frame's tradability.
        // Clear it so a stale pre-drop LIVE frame can never be treated as current
        // during the reconnect window; recovery/live frames repopulate.
        setFrame(null);
        scheduleReconnect();
      };
    };

    connect();

    return () => {
      closedRef.current = true;
      if (timerRef.current) clearTimeout(timerRef.current);
      socketRef.current?.close();
    };
  }, [sessionId]);

  return { frame, link, reconnects };
}
