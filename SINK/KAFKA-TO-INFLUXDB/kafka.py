from confluent_kafka import Consumer,KafkaError
import json
import queue

class kafka:
    def __init__(self):
        with open("config/config.json", "r") as jsonfile:
            self.config = json.load(jsonfile)
            print("Config Read successfully For KAFKA")
        if self.config["KAFKA-CONFIG"]["KAFKA-SSL"] == "TRUE":
            conf = {
                'bootstrap.servers': self.config["KAFKA-CONFIG"]["KAFKA-URL"],
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': self.config["KAFKA-CONFIG"]["KAFKA-USR"],
                'sasl.password': self.config["KAFKA-CONFIG"]["KAFKA-PWD"],
                #"queue.buffering.max.messages": 10000000,  # 10M messages,
                'group.id': self.config["KAFKA-CONFIG"]["KAFKA-CONSUMER-ID"],
                'auto.offset.reset': 'earliest'
            }
        else:
            conf = {
                'bootstrap.servers': self.config["KAFKA-CONFIG"]["KAFKA-URL"],
                'group.id': self.config["KAFKA-CONFIG"]["KAFKA-CONSUMER-ID"],
                'auto.offset.reset': 'earliest'
            }
        #self.producer = Producer(conf)
        self.consumer = Consumer(conf)
        topic = self.config["KAFKA-CONFIG"]["KAFKA-TOPIC"]
        self.consumer.subscribe(["topic_58"])
        print("Consumer Init Succesfully..")

    def get_msg_kafka(self):
        while True:
            msg = self.consumer.poll(self.config["KAFKA-CONFIG"]["KAFKA-POLL-INTERVAL"])  # Adjust the timeout as needed

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    break
            else:
                #print(f"Received message: {msg.value().decode('utf-8')}")
                return msg.value().decode('utf-8')

