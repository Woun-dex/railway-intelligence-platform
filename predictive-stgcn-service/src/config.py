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
    # Trained weights. Auto-loaded when the file exists; otherwise the model runs
    # random-initialized and stations fall back to the persistence heuristic.
    checkpoint_path: str = "artifacts/stgcn.pt"
    delay_scale: float = 600.0       # seconds; inputs/targets are divided by this for stable training
    min_warm_steps: int = 3          # below this many observed frames a station uses the heuristic fallback
    torch_threads: int = 1           # intra-op threads; 1 keeps per-request latency predictable

    # ---- Inference scheduling ---------------------------------------------
    inference_interval_ms: int = 1000  # cadence of the rolling batch forward pass
    model_version: str = "stgcn-0.1.0"

    # ---- Scaling / state ---------------------------------------------------
    # The spatial graph conv needs the WHOLE network's state, but the telemetry
    # topic is partitioned by trip. So an inference instance manually assigns ALL
    # partitions (global state) instead of joining a balanced consumer group:
    # scale vertically, and run a warm standby that restores from the snapshot.
    assign_all_partitions: bool = True
    state_path: str = "artifacts/state.npz"   # rolling-buffer snapshot for warm start
    state_snapshot_ms: int = 15000            # 0 disables periodic snapshots

    # ---- Training (offline) -----------------------------------------------
    # The simulator bootstraps a digital-twin dataset from the REAL topology when
    # no historical feed exists yet (see src/train/). Swap for a real feature
    # store later without touching the model.
    train_episodes: int = 600        # simulated disruption episodes
    train_epochs: int = 30
    train_batch_size: int = 32
    train_lr: float = 1e-3
    train_val_frac: float = 0.2
    train_seed: int = 1234
    train_max_sources: int = 4       # concurrent disruptions per episode

    # ---- Service -----------------------------------------------------------
    host: str = "0.0.0.0"
    port: int = 8092
    log_level: str = "INFO"


settings = Settings()
