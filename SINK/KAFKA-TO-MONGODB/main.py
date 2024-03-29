from mongo import MongoDB
import os, json, sys
from kafka import kafka
import threading, time
from queue import Queue

#from dotenv import load_dotenv

# import queue
#load_dotenv()


# q = queue()
# Init class
class main:
    def __init__(self):
        self.mongo = MongoDB()
        self.kf = kafka()
        self.q = Queue()
        # read config
        with open("config/config.json", "r") as jsonfile:
            self.config = json.load(jsonfile)
        print("Loaded Configuration Was -:" + json.dumps(self.config, indent=2))
        print("Starting the Bridge Worker....")

        # set ENV variable
        self.my_variable = int(os.getenv('WORKER'))

        if self.my_variable is not None:
            print(f'The value of CPU_ALLOWED is: {self.my_variable}')
        else:
            print('CPU_ALLOWED is not set.')
            self.my_variable = int(self.config["CONNECT-CONFIG"]["DEFAULT-TASK"])

    def get_msg_from_kafka(self):
        print("Consuming Msg:-Attaching to 1 CPU thread ")
        while 1:
            in_data = self.kf.get_msg_kafka()
            if in_data != None:
                try:
                    print("Got Data Packet-:" + str(in_data))
                    self.q.put(in_data)
                except:
                    e = sys.exc_info()[0]
                    print("FAILED TO GET MSG PROPERTIES .... - " + str(e))
                    pass

    def attach_to_mongo(self):
        while 1:
            print("Queue Size-: " + str(self.q.qsize()))
            if self.q.empty():
                print("No Data Available- waiting for new data...")
                time.sleep(m.config["MONGO"]["MONGO-WAIT-TIME"])
            if not self.q.empty():
                in_data = self.q.get()
                self.mongo.insert_data_single(in_data, m.config["MONGO"]["COLLECTION-NAME"])
                print("Attaching to Mongo DB insert thread-:" + str(in_data))


if __name__ == '__main__':
    m = main()
    for x in range(1, m.my_variable/2 + 1):
        obj = "tg" + str(x)
        print("Starting Thread-:" + obj)
        obj = threading.Thread(target=m.attach_to_mongo)
        obj.start()
    for x in range(1, m.my_variable + 1):
        obj = "ts" + str(x)
        print("Starting Thread-:" + obj)
        obj = threading.Thread(target=m.attach_to_mongo)
        obj.start()
