setup:
	cd shopstream && uv sync
	.venv/bin/activate

run_tests:
	pytest

run_pubsub_emulator:
	gcloud beta emulators pubsub start --project=local-pubsub-instance --data-dir=/home/nathanodc/Projects/Personal/ShopStream/pubsub_log

run_airflow_emulator:
	cd airflow && astro dev start

stop_airflow_emulator:
	cd airflow && astro dev stop

run_gcf_source:
	cd shopstream/1_api_sources && functions-framework --target=hello_http --port=8080 --debug

source_request:
	curl -X POST http://localhost:8080 -H "Content-Type: application/json" -d '{ "log_env": "dev", "source_bucket_name": "shopstream-source-data-files", "pubsub_topics": { "clickstream": "projects/local-pubsub-instance/topics/clickstream-topic", "customer_support": "projects/local-pubsub-instance/topics/customer-support-topic", "product_catalog": "projects/local-pubsub-instance/topics/product-catalog", "sales_transactions": "projects/local-pubsub-instance/topics/sales-transactions"}}'

run_pubsub_to_gcs:
	uv run shopstream/2_pubsub-2-gcs/main.py