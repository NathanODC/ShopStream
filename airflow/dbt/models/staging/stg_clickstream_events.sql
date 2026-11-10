{{ normalize_streaming_clickstream(source('streaming_bronze', 'clickstream_events')) }}
