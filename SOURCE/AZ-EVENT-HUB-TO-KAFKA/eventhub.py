import sys
import json
from azure.eventhub import EventHubConsumerClient
from termcolor import colored
from queue import Queue
import time


class Event_Hub():
    def __init__(self):
        with open("config/config.json", "r") as jsonfile:
            self.data = json.load(jsonfile)
            print("Config Read successfully For EventHub")
        self.q = Queue()

    def on_event(self,partition_context, event):
        print("Received the data from IoTHub: \"{}\" from the partition with ID: \"{}\"".format(
            event.body_as_str(encoding='UTF-8'), partition_context.partition_id))
        # print(json.loads(event.body_as_str(encoding='UTF-8')))
        self.q.put(json.loads(event.body_as_str(encoding='UTF-8')))

    def on_partition_initialize(self,partition_context):
        # Put your code here.
        print("Partition: {} has been initialized.".format(partition_context.partition_id))

    def on_partition_close(self,partition_context, reason):
        # Put your code here.
        print("Partition: {} has been closed, reason for closing: {}.".format(
            partition_context.partition_id,
            reason
        ))

    def on_error(self,partition_context, error):
        # Put your code here. partition_context can be None in the on_error callback.
        if partition_context:
            print("An exception: {} occurred during receiving from Partition: {}.".format(
                partition_context.partition_id,
                error
            ))
        else:
            print("An exception: {} occurred during the load balance process.".format(error))

    def get_event_data(self):
        consumer_client = EventHubConsumerClient.from_connection_string(
            conn_str=self.data["AZ-EVENT-HUB-CONFIG"]["CONNECTION-STRING"],
            consumer_group=self.data["AZ-EVENT-HUB-CONFIG"]["CONSUMER-GROUP"],
            eventhub_name=self.data["AZ-EVENT-HUB-CONFIG"]["AZ-EVENT-HUB-NAME"],
        )

        try:
            with consumer_client:
                consumer_client.receive(
                    on_event=self.on_event,
                    on_partition_initialize=self.on_partition_initialize,
                    on_partition_close=self.on_partition_close,
                    on_error=self.on_error,
                    starting_position="@latest",  # "-1" is from the beginning of the partition.
                )
        except KeyboardInterrupt:
            print('Stopped receiving.')

    def fetch_data(self):
        print("Looking For data ...queue size -:" + str(self.q.qsize()))
        if self.q.empty():
            print("No Data Avaialble")
            time.sleep(self.data["AZ-EVENT-HUB-CONFIG"]["WAIT-TIME"])
            return None
        if not self.q.empty():
            incoming_data = self.q.get()
            return incoming_data
