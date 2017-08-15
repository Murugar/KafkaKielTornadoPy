#!/usr/bin/python2.7

__name__ = 'MainHandler'

import json 
import time
from tornado import gen, web, httpclient
from kafka import KafkaConsumer
from kiel import clients

class MainHandler(web.RequestHandler):
    @web.asynchronous
    @gen.coroutine
    def get(self):
        

        self.write('Consumer opening')

        c = clients.SingleConsumer(brokers=["localhost:9092"])

        yield c.connect()

        while True:
          print("Loop")
          msgs = yield c.consume("dirty")
     

          for msg in msgs:
             print(msg['key']) 
                       
                    
          

        print ("Consumed Start") 
        
        self.write("Consumer Close") 
        consumer.close()

        
