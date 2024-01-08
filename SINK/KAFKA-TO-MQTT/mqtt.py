import sys
import json
import paho.mqtt.client as mqtt
from termcolor import colored
from queue import Queue
import time


class Generic_Mqtt():
    def __init__(self):
        with open("config/config.json", "r") as jsonfile:
            self.data = json.load(jsonfile)
            print("Config Read successfully For MQTT")
        self.q = Queue()
        self.MQTT_client = mqtt.Client(client_id=self.data["MQTT-CONFIG"]["CLIENT_ID"], protocol=mqtt.MQTTv311,
                                       clean_session=True)
        self.MQTT_Connect()

    def check_connected(self):
        return self.MQTT_client.is_connected()

    # def on_connect(self, client, userdata, flags, rc):
    #     client.subscribe(self.data["MQTT-CONFIG"]["TOPIC_SUB"])
    #     print("Connected and subscribe already")
    #
    # def on_message(self, client, userdata, msg):
    #     in_data = json.loads(msg.payload.decode("utf-8"))
    #     #print(in_data)
    #     self.q.put(in_data)

    def MQTT_Connect(self):
        try:
            print("connecting")
            self.MQTT_client.username_pw_set(self.data["MQTT-CONFIG"]["MQTT_USR"], self.data["MQTT-CONFIG"]["MQTT_PWD"])
            # self.MQTT_client.on_connect = self.on_connect
            # self.MQTT_client.on_message = self.on_message
            self.MQTT_client.connect(self.data["MQTT-CONFIG"]["MQTT_BROKER_URL"], self.data["MQTT-CONFIG"]["MQTT_PORT"])
            self.MQTT_client.loop_start()
            print(colored("SUCCESSFULLY CONNECTED TO MQTT BROKER", "green"))
        except:
            e = sys.exc_info()[0]
            print(colored("FAILED TO CONNECT MQTT SERVER CHECK CONNECTION - " + str(e), "red"))
            pass

    def MQTT_Send_Data(self, payload):
        try:
            self.MQTT_client.publish(topic=self.data["MQTT-CONFIG"]["TOPIC_PUB"], payload=payload,
                                     qos=self.data["MQTT-CONFIG"]["QOS"])
            print(colored("PUBLISHING  DATA TO MQTT-:" + str(payload), "green"))
            return True
        except:
            e = sys.exc_info()[0]
            print(colored("EXCEPTION IN SENDING DATA BY GENERIC MQTT - " + str(e), "red"))
            pass

    def fetch_data(self):
        print("Looking For data ...queue size -:" + str(self.q.qsize()))
        if self.q.empty():
            print("No Data Avaialble")
            time.sleep(self.data["MQTT-CONFIG"]["MQTT_WAIT_TIME"])
            return None
        if not self.q.empty():
            incoming_data = self.q.get()
            return incoming_data
