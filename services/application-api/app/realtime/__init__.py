"""Real-time cockpit pipeline (Phase 2).

Consumes the Rust Market Core gRPC snapshot/bar stream, runs the Phase 1
Regime/Vol/Strategy engines per tick, and projects each result into a
schema-compliant ``CockpitState`` frame
(``packages/contracts/jsonschema/cockpit_state.json``) for WebSocket push.

Fail closed: any missing/late data or engine error yields a No-Trade frame with
``new_position_allowed=false`` — the pipeline never fabricates a tradable state.
"""

from app.realtime.projector import CockpitProjector, ProjectorConfig

__all__ = ["CockpitProjector", "ProjectorConfig"]
