# from confluent_kafka import Producer
# import time
# import json
# # Kafka broker information
# bootstrap_servers = 'pkc-9q8rv.ap-south-2.aws.confluent.cloud:9092'
# a = 0
# # SASL/PLAIN configurations
# api_key = 'P65IWZDTHDBWVKZC'
# api_secret = 'Y0yzH4IKuaFFs8cbAj1C8VK/BDLqSOEj1uh8wW+22eL+St3Z4gsaN9ihOdLenyHV'
#
# # Additional producer configurations
# producer_config = {
#     'bootstrap.servers': bootstrap_servers,
#     'security.protocol': 'sasl_ssl',
#     'sasl.mechanism': 'PLAIN',
#     'sasl.username': api_key,
#     'sasl.password': api_secret,
# }
#
# # Create KafkaProducer instance with API key and secret
# producer = Producer(producer_config)
#
# # Example: Produce a message to a topic
# topic = 'topic_58'
#
# while 1:
#     key = 'value'
#     value = {"integer": str(a)}
#
#     producer.produce(topic, key=key, value=json.dumps(value))
#     print("sending data-:" + str(a))
#     # Wait for any outstanding messages to be delivered and delivery reports to be received
#     producer.flush()
#     time.sleep(1)
#     a = a + 1







import json, time,sys
from azure.eventhub import EventHubConsumerClient
from SaveData import savedata
import threading
import os
from dotenv import load_dotenv
load_dotenv()
CONNECTION_STR = os.getenv("EVENT_HUB_CONN_STR")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")
savedata = savedata()

def on_event(partition_context, event):
    print("Received the data from iothub: \"{}\" from the partition with ID: \"{}\"".format(event.body_as_str(encoding='UTF-8'),partition_context.partition_id))
    #print(json.loads(event.body_as_str(encoding='UTF-8')))
    savedata.save_into_table(json.loads(event.body_as_str(encoding='UTF-8')))

def on_partition_initialize(partition_context):
    # Put your code here.
    print("Partition: {} has been initialized.".format(partition_context.partition_id))


def on_partition_close(partition_context, reason):
    # Put your code here.
    print("Partition: {} has been closed, reason for closing: {}.".format(
        partition_context.partition_id,
        reason
    ))


def on_error(partition_context, error):
    # Put your code here. partition_context can be None in the on_error callback.
    if partition_context:
        print("An exception: {} occurred during receiving from Partition: {}.".format(
            partition_context.partition_id,
            error
        ))
    else:
        print("An exception: {} occurred during the load balance process.".format(error))

def get_data(num):
    consumer_client = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group='$Default',
        eventhub_name=EVENTHUB_NAME,
    )

    try:
        with consumer_client:
            consumer_client.receive(
                on_event=on_event,
                on_partition_initialize=on_partition_initialize,
                on_partition_close=on_partition_close,
                on_error=on_error,
                starting_position="@latest",  # "-1" is from the beginning of the partition.
            )
    except KeyboardInterrupt:
        print('Stopped receiving.')


def save_azure(num):
    while 1:
        try:
            savedata.send_data_azure_table()
            #print("data sender thred open")
            time.sleep(1)
        except:
            e = sys.exc_info()[0]
            print("exception got..-:" + str(e))
            continue

if __name__ == '__main__':
    t1 = threading.Thread(target=get_data, args=(10,))
    t2 = threading.Thread(target=save_azure, args=(10,))
    t1.start()
    t2.start()