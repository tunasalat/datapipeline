import random
from datetime import datetime
import time
import json
from bson import json_util
import sys
from kafka import KafkaProducer
import pymongo
from pymongo import MongoClient

class randomProducer():
    def __init__(self):
        self.kafkaProd = KafkaProducer(bootstrap_servers=['localhost1:6667','localhost2:6667'], value_serializer=lambda v: json.dumps(v, default=json_util.default).encode('utf-8'))
        self.client = MongoClient('localhost', 27017)
        self.db = self.client.test_database
        self.oplog = self.client.local.oplog.rs
        self.first = self.oplog.find().sort('$natural', pymongo.ASCENDING).limit(-1).next()
        self.ts = self.first['ts']

    def runProducer(self):
        try:
            while(self.var==1):
                print("%s I NETWORK  [conn4] end connection 127.0.0.1:36549 (0 connections now open)"%(datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%S.%f%z")))
                time.sleep(1+random.random())
        except:
            sys.exit()

    def runKafkaProducer(self):
        try:
             while True:
                 self.cursor = self.oplog.find({'ts': {'$gt': self.ts}},
                            cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                            oplog_replay=True)
                 for doc in self.cursor:
                     self.ts = doc['ts']
                     if doc['ts'].time >= int(time.time()) and '_id' in doc['o']:
                         print(doc)
                         self.kafkaProd.send('test', doc)
        except Exception as e:
            print('Error: ' + str(e))
            sys.exit()


if __name__ == "__main__":
    a = randomProducer()
    a.runKafkaProducer()
