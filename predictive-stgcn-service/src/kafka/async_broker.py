"""Kafka brokerage: telemetry in -> STGCN -> predictions out.

Binds the predictor to the core bus using Confluent's Protobuf wire format so it
interoperates 1:1 with the Java producers/consumers (Schema-Registry id header +
message-index prefix). Two cooperating threads keep ingestion decoupled from
inference:

  * **consumer thread** — polls `telemetry_topic`, deserializes `PositionEvent`,
    and feeds the per-station delay into the inference engine. Never blocks on the
    model, so the poll loop keeps pace with the stream.
  * **inference thread** — every ``inference_interval_ms`` advances the temporal
    window, runs one batched forward, and produces a `PredictionEvent` per
    operationally-relevant station to `prediction_topic`.

Records are keyed so that librdkafka's ``murmur2_random`` partitioner places a key
on the *same* partition as the JVM `TripIdPartitioner` (both hash with murmur2),
preserving per-key order across the language boundary.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer, ProtobufSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from src.config import Settings
from src.model.inference import InferenceEngine
from src.proto import prediction_pb2, telemetry_pb2

log = logging.getLogger("stgcn.broker")


class AsyncBroker:
    def __init__(self, engine: InferenceEngine, settings: Settings):
        self.engine = engine
        self.cfg = settings
        self._running = False
        self._threads: list[threading.Thread] = []

        sr = SchemaRegistryClient({"url": settings.schema_registry_url})
        self._deser = ProtobufDeserializer(
            telemetry_pb2.PositionEvent, {"use.deprecated.format": False})
        self._ser = ProtobufSerializer(
            prediction_pb2.PredictionEvent, sr,
            {"use.deprecated.format": False, "auto.register.schemas": True})

        self._consumer = Consumer({
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": settings.consumer_group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "partition.assignment.strategy": "cooperative-sticky",
        })
        self._producer = Producer({
            "bootstrap.servers": settings.bootstrap_servers,
            "partitioner": "murmur2_random",      # match the JVM TripIdPartitioner
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 10,
        })

        # lightweight telemetry for /metrics + /health
        self.frames_consumed = 0
        self.predictions_emitted = 0
        self.last_infer_ms = 0.0
        self._infer_samples: deque[float] = deque(maxlen=2000)

    # -- lifecycle -----------------------------------------------------------
    def start(self) -> None:
        self._running = True
        self._consumer.subscribe([self.cfg.telemetry_topic])
        self._spawn(self._consume_loop, "stgcn-consumer")
        self._spawn(self._infer_loop, "stgcn-inference")
        log.info("broker started: %s -> [STGCN] -> %s",
                 self.cfg.telemetry_topic, self.cfg.prediction_topic)

    def stop(self) -> None:
        self._running = False
        for t in self._threads:
            t.join(timeout=5.0)
        try:
            self._producer.flush(5.0)
            self._consumer.close()
        except Exception:  # noqa: BLE001 — best-effort shutdown
            pass

    def _spawn(self, target, name: str) -> None:
        t = threading.Thread(target=target, name=name, daemon=True)
        t.start()
        self._threads.append(t)

    # -- consumer thread -----------------------------------------------------
    def _consume_loop(self) -> None:
        ctx = SerializationContext(self.cfg.telemetry_topic, MessageField.VALUE)
        while self._running:
            msg = self._consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                log.warning("consume error: %s", msg.error())
                continue
            try:
                ev = self._deser(msg.value(), ctx)
            except Exception as exc:  # noqa: BLE001 — skip poison records, keep the stream alive
                log.debug("deserialize failed, skipping: %s", exc)
                continue
            if ev is None:
                continue
            self.engine.observe(ev.station_id, ev.delay_seconds, ev.trip_id)
            self.frames_consumed += 1

    # -- inference thread ----------------------------------------------------
    def _infer_loop(self) -> None:
        ctx = SerializationContext(self.cfg.prediction_topic, MessageField.VALUE)
        interval = self.cfg.inference_interval_ms / 1000.0
        while self._running:
            t_tick = time.perf_counter()
            try:
                preds = self.engine.step_and_infer()
                for p in preds:
                    self._emit(p, ctx)
                    self.predictions_emitted += 1
                if preds:
                    self.last_infer_ms = preds[0].inference_time_ms
                    self._infer_samples.append(preds[0].inference_time_ms)
                self._producer.poll(0)  # serve delivery callbacks
            except Exception as exc:  # noqa: BLE001 — never let a tick kill the loop
                log.exception("inference tick failed: %s", exc)
            sleep = interval - (time.perf_counter() - t_tick)
            if sleep > 0:
                time.sleep(sleep)

    def _emit(self, p, ctx: SerializationContext) -> None:
        now_ms = int(time.time() * 1000)
        event = prediction_pb2.PredictionEvent(
            event_id=f"stgcn-{p.station_id}-{now_ms}",
            trip_id=p.trip_id,
            station_id=p.station_id,
            horizon_seconds=p.horizon_seconds,
            current_delay_seconds=p.current_delay_s,
            quantiles=[
                prediction_pb2.Quantile(tau=0.5, delay_seconds=p.median_s),
                prediction_pb2.Quantile(tau=0.9, delay_seconds=p.p90_s),
                prediction_pb2.Quantile(tau=0.95, delay_seconds=p.p95_s),
            ],
            escalation_ratio=p.escalation_ratio,
            fallback_used=p.fallback_used,
            model_version=self.cfg.model_version,
            graph_version=self.engine.topo.graph_version,
            event_time_ms=now_ms,
            inference_time_ms=int(p.inference_time_ms),
        )
        # Key by trip when known (preserves per-journey order); else by station so
        # a station's prediction stream stays on one partition.
        key = (p.trip_id or f"station-{p.station_id}").encode("utf-8")
        self._producer.produce(
            self.cfg.prediction_topic, key=key,
            value=self._ser(event, ctx))

    # -- introspection -------------------------------------------------------
    def p99_infer_ms(self) -> float:
        if not self._infer_samples:
            return 0.0
        ordered = sorted(self._infer_samples)
        idx = max(0, int(0.99 * (len(ordered) - 1)))
        return ordered[idx]
