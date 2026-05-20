#!/usr/bin/env bash
#
# get_job_commands.sh — Reconstruct gcloud submission commands for all running Dataflow jobs.
#
# Usage:
#   ./bin/get_job_commands.sh [--project PROJECT] [--region REGION] [--update]
#
# Flags:
#   --project PROJECT   GCP project ID (default: current gcloud config)
#   --region  REGION    Only list jobs in this region (default: all regions "-")
#   --update            Append --update to each flex-template command for in-place job update
#
# Output: one ready-to-run gcloud / java command block per running job, separated by blank lines.

set -euo pipefail

# ── defaults ──────────────────────────────────────────────────────────────────
PROJECT=$(gcloud config get-value project 2>/dev/null)
REGION="-"
ADD_UPDATE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT="$2"; shift 2 ;;
    --region)  REGION="$2";  shift 2 ;;
    --update)  ADD_UPDATE=true; shift ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "${PROJECT}" ]]; then
  echo "ERROR: No project set. Pass --project or run: gcloud config set project PROJECT" >&2
  exit 1
fi

ACCESS_TOKEN=$(gcloud auth print-access-token 2>/dev/null)

# ── fetch all running jobs ────────────────────────────────────────────────────
echo "# Running Dataflow jobs in project=${PROJECT} region=${REGION}" >&2
echo "# Generated at $(date -u '+%Y-%m-%dT%H:%M:%SZ')" >&2
echo >&2

JOBS=$(gcloud dataflow jobs list \
  --project="${PROJECT}" \
  --region="${REGION}" \
  --status=active \
  --format="value(id,location,name)" 2>/dev/null)

if [[ -z "${JOBS}" ]]; then
  echo "# No running Dataflow jobs found." >&2
  exit 0
fi

# ── parameter classification ──────────────────────────────────────────────────
# These become gcloud flags; everything else goes into --parameters.
RUNNER_PARAMS="region subnetwork usePublicIps enableStreamingEngine serviceAccount
  maxNumWorkers numWorkers workerMachineType diskSizeGb tempLocation gcpTempLocation
  zone workerRegion labels dataflowKmsKey impersonateServiceAccount sdkContainerImage
  dataflowServiceOptions experiments stagingLocation autoscalingAlgorithm network"

# These are Beam/Dataflow internals — omit them entirely.
SYSTEM_PARAMS="filesToStage pipelineUrl templateLocation stagerClass runner gcpOauthScopes
  credentialFactoryClass userAgent optionsId stableUniqueNames defaultEnvironmentConfig
  defaultEnvironmentType apiRootUrl dataflowEndpoint gcsEndpoint sdkHarnessContainerImageOverrides
  workerHarnessContainerImage saveProfilesToGcs googleApiTrace gbek gcsHttpRequestWriteTimeout
  gcsHttpRequestReadTimeout gcsUploadBufferSizeBytes gcsRewriteDataOpBatchLimit gcsCustomAuditEntries
  enableBucketWriteMetricCounter enableBucketReadMetricCounter gcsPerformanceMetrics
  overrideWindmillBinary transformsToOverride resourceHints hotKeyLoggingEnabled
  checkpointingInterval storageApiAppendThresholdBytes numberOfWorkerHarnessThreads
  desiredNumUnboundedSourceSplits updateCompatibilityVersion streaming jobName appName
  pathValidatorClass project enableBucketWriteMetricCounter storageWriteApiMaxRequestSize
  storageWriteApiMaxRetries gcpTempLocation"

# ── process each job ──────────────────────────────────────────────────────────
while IFS=$'\t' read -r JOB_ID JOB_REGION JOB_NAME; do
  echo "# ── Job: ${JOB_NAME}  (${JOB_ID}  ${JOB_REGION}) ──────────────" >&2

  # Fetch full job details via REST API (gcloud CLI doesn't expose sdkPipelineOptions)
  JOB_JSON=$(curl -sf \
    "https://dataflow.googleapis.com/v1b3/projects/${PROJECT}/locations/${JOB_REGION}/jobs/${JOB_ID}?view=JOB_VIEW_ALL" \
    -H "Authorization: Bearer ${ACCESS_TOKEN}" 2>/dev/null) || {
      echo "# ERROR: Could not fetch details for job ${JOB_ID}" >&2
      continue
    }

  # Write JSON to a temp file (too large for env var) and pass path via env
  TMP_JSON=$(mktemp /tmp/df_job_XXXXXX.json)
  printf '%s' "${JOB_JSON}" > "${TMP_JSON}"
  trap 'rm -f "${TMP_JSON}"' RETURN

  DF_JSON_FILE="${TMP_JSON}" \
  DF_JOB_ID="${JOB_ID}" \
  DF_JOB_REGION="${JOB_REGION}" \
  DF_JOB_NAME="${JOB_NAME}" \
  DF_PROJECT="${PROJECT}" \
  DF_RUNNER_PARAMS="${RUNNER_PARAMS}" \
  DF_SYSTEM_PARAMS="${SYSTEM_PARAMS}" \
  DF_ADD_UPDATE="${ADD_UPDATE}" \
  python3 - <<'PYEOF'
import json, os, shlex

with open(os.environ["DF_JSON_FILE"]) as fh:
    job = json.load(fh)
job_id   = os.environ["DF_JOB_ID"]
job_reg  = os.environ["DF_JOB_REGION"]
job_name = os.environ["DF_JOB_NAME"]
project  = os.environ["DF_PROJECT"]
runner_set = set(os.environ["DF_RUNNER_PARAMS"].split())
system_set = set(os.environ["DF_SYSTEM_PARAMS"].split())
add_update = os.environ["DF_ADD_UPDATE"].lower() == "true"

labels    = job.get("labels", {})
is_flex   = labels.get("goog-dataflow-provided-template-type") == "flex"
tmpl_name = labels.get("goog-dataflow-provided-template-name", "")

env  = job.get("environment", {})
opts = env.get("sdkPipelineOptions", {}).get("options", {})

def q(v):
    return shlex.quote(str(v))

# ── FLEX TEMPLATE JOB ────────────────────────────────────────────────────────
if is_flex:
    # Known Google-managed template label → exact GCS path segment
    KNOWN_FLEX_TEMPLATES = {
        "kafka_to_bigquery_flex":          "Kafka_to_BigQuery_Flex",
        "kafka_to_bigquery":               "Kafka_to_BigQuery",
        "pubsub_to_bigquery":              "PubSub_to_BigQuery",
        "gcs_to_bigquery":                 "GCS_to_BigQuery",
        "bigquery_to_bigtable":            "BigQuery_to_Bigtable",
        "datastream_to_bigquery":          "Datastream_to_BigQuery",
        "jdbc_to_bigquery":                "Jdbc_to_BigQuery",
        "spanner_to_bigquery":             "Spanner_to_BigQuery",
        "pubsub_to_pubsub":                "PubSub_to_PubSub",
        "pubsub_to_gcs":                   "Cloud_PubSub_to_GCS_Text_Flex",
        "gcs_to_pubsub":                   "GCS_to_Cloud_PubSub",
        "mongodb_to_bigquery":             "MongoDB_to_BigQuery",
    }
    tmpl_seg  = KNOWN_FLEX_TEMPLATES.get(
        tmpl_name,
        "_".join(w.capitalize() for w in tmpl_name.split("_"))
    )
    tmpl_path = f"gs://dataflow-templates-{job_reg}/latest/flex/{tmpl_seg}"
    region      = opts.get("region", job_reg)
    subnetwork  = opts.get("subnetwork")
    use_public  = opts.get("usePublicIps", True)
    strm_eng    = opts.get("enableStreamingEngine", False)
    svc_account = opts.get("serviceAccount")
    max_workers = opts.get("maxNumWorkers", 0)
    num_workers = opts.get("numWorkers", 0)
    machine     = opts.get("workerMachineType")
    disk_gb     = opts.get("diskSizeGb", 0)
    temp_loc    = opts.get("tempLocation") or opts.get("gcpTempLocation")
    worker_zone = opts.get("zone")
    worker_reg  = opts.get("workerRegion")
    kms_key     = opts.get("dataflowKmsKey")
    impersonate = opts.get("impersonateServiceAccount")
    sdk_image   = opts.get("sdkContainerImage")
    svc_opts    = opts.get("dataflowServiceOptions") or []
    # Filter out system-injected experiments
    AUTO_EXP = {
        "enable_streaming_engine", "enable_windmill_service",
        "enable_streaming_java_vmr", "enable_private_ipv6_google_access",
        "enable_always_on_exception_sampling",
    }
    raw_exp = [e for e in (opts.get("experiments") or [])
               if not e.startswith("disable_runner_v2") and e not in AUTO_EXP]
    user_labels = {k: v for k, v in labels.items() if not k.startswith("goog-")}

    # Template-specific params: everything not runner/system, with a non-empty value
    tmpl_params = {}
    for k, v in opts.items():
        if k in runner_set or k in system_set:
            continue
        if v is None or v == "" or v == [] or v == {}:
            continue
        if isinstance(v, bool) and not v:
            continue
        tmpl_params[k] = v

    parts = [
        f"gcloud dataflow flex-template run {q(job_name)}",
        f"  --template-file-gcs-location {q(tmpl_path)}",
        f"  --project {q(project)}",
        f"  --region {q(region)}",
    ]
    if temp_loc:
        parts.append(f"  --temp-location {q(temp_loc)}")
    if subnetwork:
        parts.append(f"  --subnetwork {q(subnetwork)}")
    if not use_public:
        parts.append("  --disable-public-ips")
    if strm_eng:
        parts.append("  --enable-streaming-engine")
    if svc_account:
        parts.append(f"  --service-account-email {q(svc_account)}")
    if max_workers and max_workers > 0:
        parts.append(f"  --max-workers {max_workers}")
    if num_workers and num_workers > 0:
        parts.append(f"  --num-workers {num_workers}")
    if machine:
        parts.append(f"  --worker-machine-type {q(machine)}")
    if disk_gb and disk_gb > 0:
        parts.append(f"  --disk-size-gb {disk_gb}")
    if worker_zone:
        parts.append(f"  --worker-zone {q(worker_zone)}")
    if worker_reg:
        parts.append(f"  --worker-region {q(worker_reg)}")
    if kms_key:
        parts.append(f"  --dataflow-kms-key {q(kms_key)}")
    if impersonate:
        parts.append(f"  --impersonate-service-account {q(impersonate)}")
    if sdk_image:
        parts.append(f"  --sdk-container-image {q(sdk_image)}")
    if svc_opts:
        parts.append(f"  --dataflow-service-options {q(','.join(svc_opts))}")
    if raw_exp:
        parts.append(f"  --additional-experiments {q(','.join(raw_exp))}")
    if user_labels:
        label_str = ",".join(f"{k}={v}" for k, v in user_labels.items())
        parts.append(f"  --additional-user-labels {q(label_str)}")

    if tmpl_params:
        param_items = []
        for k, v in sorted(tmpl_params.items()):
            if isinstance(v, (list, dict)):
                param_items.append(f"{k}={json.dumps(v)}")
            elif isinstance(v, bool):
                param_items.append(f"{k}={'true' if v else 'false'}")
            else:
                param_items.append(f"{k}={v}")
        parts.append(f"  --parameters {q(','.join(param_items))}")

    if add_update:
        parts.append("  --update")

    print(" \\\n".join(parts))
    print()

# ── CUSTOM JAR JOB ───────────────────────────────────────────────────────────
else:
    print(f"# Job '{job_name}' is a custom jar pipeline.")
    print(f"# Verify the jar path and main class before running.")
    print()
    lines = [
        "java -jar ./target/test-kafka2bq-bundled-1.0.0.jar",
        "  --runner=DataflowRunner",
        f"  --project={q(project)}",
        f"  --region={q(opts.get('region', job_reg))}",
    ]
    for flag, key in [
        ("--tempLocation",          "tempLocation"),
        ("--gcsTempLocation",       "tempLocation"),
        ("--subnetwork",            "subnetwork"),
        ("--serviceAccount",        "serviceAccount"),
        ("--workerMachineType",     "workerMachineType"),
    ]:
        v = opts.get(key)
        if v:
            lines.append(f"  {flag}={q(str(v))}")

    for flag, key in [
        ("--enableStreamingEngine", "enableStreamingEngine"),
        ("--usePublicIps",          "usePublicIps"),
    ]:
        v = opts.get(key)
        if v is not None:
            lines.append(f"  {flag}={'true' if v else 'false'}")

    for flag, key in [
        ("--numWorkers",    "numWorkers"),
        ("--maxNumWorkers", "maxNumWorkers"),
        ("--diskSizeGb",    "diskSizeGb"),
    ]:
        v = opts.get(key, 0)
        if v and v > 0:
            lines.append(f"  {flag}={v}")

    svc_opts = opts.get("dataflowServiceOptions") or []
    if svc_opts:
        lines.append(f"  --dataflowServiceOptions={q(','.join(svc_opts))}")

    # Remaining non-system, non-runner pipeline options
    for k, v in sorted(opts.items()):
        if k in runner_set or k in system_set:
            continue
        if v is None or v == "" or v == [] or v == {}:
            continue
        if isinstance(v, bool):
            lines.append(f"  --{k}={'true' if v else 'false'}")
        elif isinstance(v, (list, dict)):
            lines.append(f"  --{k}={q(json.dumps(v))}")
        else:
            lines.append(f"  --{k}={q(str(v))}")

    print(" \\\n".join(lines))
    print()
PYEOF

done <<< "${JOBS}"
