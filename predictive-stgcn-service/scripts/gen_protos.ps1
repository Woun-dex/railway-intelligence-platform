# Generate Python protobuf bindings from the shared schema (single source of truth).
# Run from the predictive-stgcn-service/ directory:  powershell scripts/gen_protos.ps1
$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $PSScriptRoot
$protoSrc = if ($env:PROTO_SRC) { $env:PROTO_SRC } else { Join-Path $here "../shared-schemas/src/main/proto" }
$out = Join-Path $here "src/proto"

if (-not (Test-Path $protoSrc)) { throw "proto source dir not found: $protoSrc" }
New-Item -ItemType Directory -Force -Path $out | Out-Null

Write-Host "Generating Python protobufs from $protoSrc -> $out"
$protos = Get-ChildItem -Path $protoSrc -Filter *.proto | ForEach-Object { $_.FullName }
python -m grpc_tools.protoc -I $protoSrc --python_out=$out @protos
Write-Host "Done."
