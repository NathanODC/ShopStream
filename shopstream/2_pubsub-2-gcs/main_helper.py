import logging

from utils import upload_to_gcs

def callback_factory(topic, data_params, gcs_prefix):
    def callback(message):
        bucket_name = data_params[0]
        file_name = data_params[1]
        file_extension = data_params[2]

        logging.info(f"[Callback] Received message on {topic}: {message.message_id}")
        blob_name = f"{gcs_prefix}/{topic.split('/')[-1]}/dt={message.publish_time.strftime('%Y-%m-%d')}/{file_name}_{message.publish_time.strftime('%Y-%m-%d_%H-%M-%S')}.{file_extension}"
        logging.debug(f"[Callback] Constructed blob name: {blob_name}")
        try:
            upload_to_gcs(bucket_name, blob_name, message.data)
            message.ack()
            logging.info(
                f"[Callback] Message {message.message_id} processed and acknowledged."
            )
        except Exception as e:
            logging.error(
                f"[Callback] Error processing message {message.message_id}: {e}"
            )

    return callback