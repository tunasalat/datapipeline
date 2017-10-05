import random
from datetime import datetime
import time
import sys
from kafka import KafkaProducer
from pymongo import MongoClient

class randomProducer():
    def __init__(self):
        self.var = 1
        self.kafkaProd = KafkaProducer(bootstrap_servers='localhost:6667', value_serializer=lambda v: v.encode('utf-8'))
        self.client = MongoClient('localhost', 27017)
        self.db = self.client.test_database
        self.collection = self.db.test_collection
        self.dummy_id = '59d64264f212d42f020afcf0'
#
    def runProducer(self):
        try:
            while(self.var==1):
                print("%s I NETWORK  [conn4] end connection 127.0.0.1:36549 (0 connections now open)"%(datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%S.%f%z")))
                time.sleep(1+random.random())
        except:
            sys.exit()
#
    def runKafkaProducer(self):
#        try:
         while(self.var==1):
             #for doc in self.collection.find({"_id": {"$gte": dummy_id}}):
             for doc in self.collection.find({"_id": dummy_id}):
                 #self.kafkaProd.send('test', doc)
                 print(doc)
             time.sleep(1+random.random())
#        except Exception as e:
#            print('Error: ' + str(e))
#            sys.exit()


if __name__ == "__main__":
    a = randomProducer()
    a.runKafkaProducer()
