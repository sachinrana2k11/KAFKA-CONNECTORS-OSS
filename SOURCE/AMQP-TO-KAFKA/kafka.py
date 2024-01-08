import json
from confluent_kafka import Producer


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
                "queue.buffering.max.messages": self.config["KAFKA-CONFIG"]["KAFKA-MAX-BUFFER"],  # 0.1M messages
            }
        else:
            conf = {
                'bootstrap.servers': self.config["KAFKA-CONFIG"]["KAFKA-URL"],
                "queue.buffering.max.messages": self.config["KAFKA-CONFIG"]["KAFKA-MAX-BUFFER"],  # 0.1 M messages
            }
        self.producer = Producer(conf)
        print("Producer Init Successfully..-:")

    def attach_msg_to_kafka_topic(self, msg):
        self.producer.produce(topic=self.config["KAFKA-CONFIG"]["KAFKA-TOPIC"], key='data', value=json.dumps(msg))
        print("sending Message to Kafka Topic-:" + str(msg))
        self.producer.flush()
