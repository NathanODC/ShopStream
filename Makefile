PUBSUB_EMULATOR_HOST := localhost:8085
PUBSUB_PROJECT_ID := local-pubsub-instance
PUBSUB_DATA_DIR := /home/nathanodc/Projects/Personal/ShopStream/pubsub_log

setup:
	cd shopstream && uv sync
	.venv/bin/activate

run_tests:
	pytest

run_pubsub_emulator:
	gcloud beta emulators pubsub start --project=$(PUBSUB_PROJECT_ID) --data-dir=$(PUBSUB_DATA_DIR)

run_airflow_emulator:
	cd airflow && astro dev start

stop_airflow_emulator:
	cd airflow && astro dev stop

run_gcf_source:
	cd shopstream/1_api_sources && functions-framework --target=main --port=8080 --debug

source_request:
	curl -X POST http://localhost:8080 -H "Content-Type: application/json" -d '{ "log_env": "dev", "source_bucket_name": "shopstream-source-data-files", "pubsub_topics": { "clickstream": "projects/$(PUBSUB_PROJECT_ID)/topics/clickstream-topic", "customer_support": "projects/$(PUBSUB_PROJECT_ID)/topics/customer-support-topic", "product_catalog": "projects/$(PUBSUB_PROJECT_ID)/topics/product-catalog", "sales_transactions": "projects/$(PUBSUB_PROJECT_ID)/topics/sales-transactions"}}'

run_gcf_pubsub_to_gcs:
	cd shopstream/2_pubsub-2-gcs && functions-framework --target=main --port=8080 --debug

pubsub_to_gcs_request:
	curl -X POST http://localhost:8080 -H "Content-Type: application/json" -d '{ "log_env": "dev", "gcs_prefix": "pubsub-data", "timeout": 100.0 }'

