PUBSUB_EMULATOR_HOST := localhost:8085
PUBSUB_PROJECT_ID := local-pubsub-instance
PUBSUB_DATA_DIR := $(shell pwd)/pubsub_log
STORAGE_EMULATOR_HOST := http://localhost:4443

setup:
	cd shopstream && uv sync --dev

test:
	cd shopstream && .venv/bin/pytest 1_api_sources/tests/ 2_pubsub-2-gcs/tests/ -v

# --- Local infrastructure emulators ---

run_pubsub_emulator:
	gcloud beta emulators pubsub start --project=$(PUBSUB_PROJECT_ID) --data-dir=$(PUBSUB_DATA_DIR)

run_gcs_emulator:
	docker compose up -d
	@echo "fake-gcs-server running at $(STORAGE_EMULATOR_HOST) — files land in ./gcs-local-data/"

stop_gcs_emulator:
	docker compose down

# --- GCF local runners ---

run_gcf_source:
	cd shopstream/1_api_sources && PUBSUB_EMULATOR_HOST=$(PUBSUB_EMULATOR_HOST) functions-framework --target=main --port=8080 --debug

run_gcf_pubsub_to_gcs:
	cd shopstream/2_pubsub-2-gcs && PUBSUB_EMULATOR_HOST=$(PUBSUB_EMULATOR_HOST) STORAGE_EMULATOR_HOST=$(STORAGE_EMULATOR_HOST) CONFIG_PATH=config.local.json functions-framework --target=main --port=8081 --debug

# --- HTTP trigger shortcuts ---

source_request:
	curl -X POST http://localhost:8080 -H "Content-Type: application/json" -d '{ "log_env": "dev", "source_bucket_name": "shopstream-source-data-files", "pubsub_topics": { "clickstream": "projects/$(PUBSUB_PROJECT_ID)/topics/clickstream-topic", "customer_support": "projects/$(PUBSUB_PROJECT_ID)/topics/customer-support-topic", "product_catalog": "projects/$(PUBSUB_PROJECT_ID)/topics/product-catalog", "sales_transactions": "projects/$(PUBSUB_PROJECT_ID)/topics/sales-transactions"}}'

pubsub_to_gcs_request:
	curl -X POST http://localhost:8081 -H "Content-Type: application/json" -d '{ "log_env": "dev", "gcs_prefix": "pubsub-data", "timeout": 30.0 }'

# --- Dataflow streaming pipeline ---
# Run locally with DirectRunner + Pub/Sub emulator.
# Override DATAFLOW_TOPIC and DATAFLOW_TABLE to target a different topic.

DATAFLOW_TOPIC := projects/$(PUBSUB_PROJECT_ID)/topics/clickstream-topic
DATAFLOW_TABLE := clickstream_raw

run_dataflow_local:
	cd shopstream/3_dataflow && \
	  PUBSUB_EMULATOR_HOST=$(PUBSUB_EMULATOR_HOST) \
	  ../.venv/bin/python pipeline.py \
	    --runner=DirectRunner \
	    --project=local-project \
	    --topic=$(DATAFLOW_TOPIC) \
	    --bq_dataset=shopstream_streaming \
	    --bq_table=$(DATAFLOW_TABLE) \
	    --temp_location=/tmp/beam-temp

# Deploy a streaming job to Google Cloud Dataflow.
# Requires GCP credentials: gcloud auth application-default login
deploy_dataflow:
	cd shopstream/3_dataflow && \
	  python pipeline.py \
	    --runner=DataflowRunner \
	    --project=shopstream-proj \
	    --region=us-west1 \
	    --topic=projects/shopstream-proj/topics/$(DATAFLOW_TABLE) \
	    --bq_dataset=shopstream_streaming \
	    --bq_table=$(DATAFLOW_TABLE) \
	    --temp_location=gs://shopstream-dataflow-temp/temp \
	    --staging_location=gs://shopstream-dataflow-temp/staging \
	    --job_name=shopstream-streaming-$(DATAFLOW_TABLE)

