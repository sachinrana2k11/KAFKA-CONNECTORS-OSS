import threading
import json
import os
import sys
from kafka import kafka
from eventhub import Event_Hub

#from dotenv import load_dotenv
#load_dotenv()

#Init class
class main:
    def __init__(self):
        self.event = Event_Hub()
        self.kf = kafka()
        #read config
        with open("config/config.json", "r") as jsonfile:
                    config = json.load(jsonfile)
        print("Loaded Configuration Was -:"+ json.dumps(config, indent=2))
        print("Starting the Bridge Worker....")

        #set ENV variable
        self.my_variable = int(os.getenv('WORKER'))

        if self.my_variable is not None:
            print(f'The value of WORKER is: {self.my_variable}')
        else:
            print('CPU_ALLOWED is not set.')
            self.my_variable = int(config["CONNECT-CONFIG"]["DEFAULT-TASK"])

    def get_event(self):
        self.event.get_event_data()


    def consume_msg(self):
        print("Consuming Msg:-Attaching to 1 CPU thread ")
        while 1:
            in_data = self.event.fetch_data()
            if in_data != None:
                try:
                    print("Got Data Packet-:"+str(in_data))
                    self.kf.attach_msg_to_kafka_topic(in_data)
                except:
                    e = sys.exc_info()[0]
                    print("FAILED TO GET MSG PROPERTIES .... - "+str(e))
                    pass


if __name__ == '__main__':
    m = main()
    t0 = threading.Thread(target=m.get_event)
    t0.start()
    for x in range(1,m.my_variable+1):
        obj = "t"+str(x)
        print("Starting Thread-:"+ obj)
        obj = threading.Thread(target=m.consume_msg)
        obj.start()

