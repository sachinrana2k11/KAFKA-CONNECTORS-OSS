from influx import InfluxDB
import os, json, sys
from kafka import kafka
import threading, time
from queue import Queue
from dotenv import load_dotenv
from termcolor import colored

# import queue
load_dotenv()


# q = queue()
# Init class
class main:
    def __init__(self):
        self.influx = InfluxDB()
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

    def get_msg_from_kafka(self, thread_number):
        while 1:
            in_data = self.kf.get_msg_kafka()
            if in_data is not None:
                try:
                    print(colored("Got Data Packet with Thread-:" + str(thread_number) + " Payload " + str(in_data),
                                  color='blue'))
                    self.q.put(in_data)
                except:
                    e = sys.exc_info()[0]
                    print(colored("FAILED TO GET MSG PROPERTIES .... - " + str(e), color='red'))
                    pass

    def attach_to_influx(self, thread_number):
        while 1:
            print(colored("Qszie-: " + str(self.q.qsize()), color='cyan'))
            if self.q.empty():
                print(colored("No Data Available- waiting for new data with Thread-:" + str(thread_number),
                              color='magenta'))
                time.sleep(m.config["INFLUX-CONFIG"]["INFLUX-WAIT-TIME"])
            if not self.q.empty():
                in_data = self.q.get()

                if "InfluxTag" in in_data and "InfluxDataFields" in in_data and "timestamp" in in_data:
                    self.influx.insert_data_single(in_data)
                    print(colored(
                        "Attaching to InfluxDB Data Packet with Thread-:" + str(thread_number) + " Payload " + str(
                            in_data),
                        color="green"))
                else:
                    print(colored("Data Schema not Valid please check source...-:" + str(thread_number), color='red'))


if __name__ == '__main__':
    if __name__ == '__main__':
        m = main()
        for x in range(1, m.my_variable-2 + 1):
            obj = "tg" + str(x)
            print(colored("Starting Thread-:" + obj, color='yellow'))
            obj = threading.Thread(target=m.get_msg_from_kafka, args=(obj,))
            obj.start()
        for x in range(1, m.my_variable + 1):
            obj = "ts" + str(x)
            print(colored("Starting Thread-:" + obj, color='yellow'))
            obj = threading.Thread(target=m.attach_to_influx, args=(obj,))
            obj.start()
