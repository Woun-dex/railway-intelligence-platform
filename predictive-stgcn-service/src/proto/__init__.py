"""Generated Protobuf bindings live here.

The ``*_pb2.py`` modules are NOT committed — they are generated from the single
source of truth in ``shared-schemas/src/main/proto`` so the Python predictor and
the JVM services always compile against the identical contracts.

Generate them with::

    # from predictive-stgcn-service/
    bash scripts/gen_protos.sh        # or: powershell scripts/gen_protos.ps1

The Docker image runs this step at build time. If you see
``ModuleNotFoundError: src.proto.telemetry_pb2``, run the generator first.
"""
