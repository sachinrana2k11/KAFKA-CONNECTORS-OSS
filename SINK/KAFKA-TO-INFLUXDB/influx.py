import datetime
import json
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import sys


class InfluxDB:
    def __init__(self):
        with open("CONFIG/config.json", "r") as jsonfile:
            self.data = json.load(jsonfile)
            print("InfluxDB Config Read successfully")
        self.bucket = self.data["INFLUX-CONFIG"]["INFLUX-BUCKET"]
        self.org = self.data["INFLUX-CONFIG"]["INFLUX-ORG"]
        self.token = self.data["INFLUX-CONFIG"]["INFLUX-TOKEN"]
        self.url = self.data["INFLUX-CONFIG"]["INFLUX-URL"]
        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        self.writer = self.client.write_api(write_options=SYNCHRONOUS)

    def insert_data_single(self, in_data):
        try:
            in_data = json.loads(in_data)
            point = Point(self.data["INFLUX-CONFIG"]["INFLUX-MEASUREMENT"])
            first_key, first_value = next(iter(in_data["InfluxTag"].items()))
            point.tag(key=str(first_key),value=str(first_value))
            #point.time(str(datetime.datetime.now()))
            point.time(in_data["timestamp"])
            for key, value in in_data["InfluxDataFields"].items():
                 point.field(key, value)
            result = self.writer.write(bucket=self.bucket, org=self.org,record=point)
            #print("Data Sent-: TO InfluxDB")
            return True
        except:
           e = sys.exc_info()[0]
           print("FAILED TO Push records to Influx Bucket - " + str(e))
           return False

