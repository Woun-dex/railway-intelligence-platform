"""Runtime configuration for the predictive STGCN service.

All knobs are environment-driven (12-factor) so the same image runs locally,
in docker-compose, and in CI. Defaults target the local backbone brought up by
`make up` and the graph engine on :8091.
"""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STGCN_", env_file=".env", extra="ignore")

    # ---- Kafka / Schema Registry ------------------------------------------
    bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    consumer_group: str = "stgcn-predictor"

    # Source telemetry. The milestone calls this `rail.telemetry.processed`; the
    # concrete normalized stream produced by the ingestion gateway is
    # `rail.raw.position` (PositionEvent protobuf), which is what carries data.
    telemetry_topic: str = "rail.raw.position"
    # Sink. `rail.predictions.stgcn` is the topic provisioned by docker-compose and
    # consumed by the Java criticality joiner.
    prediction_topic: str = "rail.predictions.stgcn"

    # ---- Graph engine (topology / adjacency source) -----------------------
    graph_engine_url: str = "http://localhost:8091"
    topology_poll_seconds: int = 0  # >0 re-fetches topology to pick up new graph versions

    # ---- Model / inference -------------------------------------------------
    history_steps: int = 12          # T: temporal window length fed to the net
    horizon_seconds: int = 1800      # forecast horizon (t+30min, inside the 15–45min band)
    cheb_order: int = 3              # K: Chebyshev polynomial order (spatial receptive field)
    hidden_channels: int = 32
    frame_seconds: int = 60          # one temporal step = this many wall-clock seconds
    checkpoint_path: str = ""        # optional .pt state_dict; random-init + heuristic fallback if empty
    min_warm_steps: int = 3          # below this many observed frames a station uses the heuristic fallback
    torch_threads: int = 1           # intra-op threads; 1 keeps per-request latency predictable

    # ---- Inference scheduling ---------------------------------------------
    inference_interval_ms: int = 1000  # cadence of the rolling batch forward pass
    model_version: str = "stgcn-0.1.0"

    # ---- Service -----------------------------------------------------------
    host: str = "0.0.0.0"
    port: int = 8092
    log_level: str = "INFO"


settings = Settings()
