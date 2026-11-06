This function is the batch ingestion path for customer-support tickets and the
product catalog. Clickstream and sales transactions are handled by Dataflow.

Create the subscriptions by starting this consumer before publishing batch
messages; Pub/Sub does not deliver messages published before a subscription
exists.
