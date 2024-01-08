import json
import pymongo
import sys

class MongoDB:
    def __init__(self):
        with open("config/config.json", "r") as jsonfile:
            self.data = json.load(jsonfile)
            print("MongoDB Config Read successfully")
        self.client = pymongo.MongoClient(self.data["MONGO"]["SERVER-ADDRESS"])
        self.returndoc = pymongo.ReturnDocument
        print(self.client.list_database_names())
        print("Server Config Read successfully you are learning great")
        database = self.data["MONGO"]['DB-NAME']
        self.cursor = self.client[database]

    def insert_data_single(self, in_data,collection_key):
        try:
            col = self.cursor[collection_key]
            response = col.insert_one(json.loads(in_data))
            print(response.inserted_id)
            return True
        except:
            e = sys.exc_info()[0]
            print("FAILED TO Push records to Mongo Collection - "+str(e))
            return False


    def insert_data_bulk(self, in_data,collection_key):
        col = self.cursor[collection_key]
        response = col.insert_many(json.loads(in_data))
        print(response.inserted_ids)



