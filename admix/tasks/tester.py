# -*- coding: utf-8 -*-
import logging
#import rucio
#from rucio.client.client import Client

#from admix.runDB import xenon_runDB as XenonRunDatabase

from admix.interfaces.database import ConnectMongoDB
from admix.helper.decorator import Collector
from admix.helper.helper import global_dictionary

@Collector
class Tester():

    def __init__(self):
        print("__init__")
        pass
        #self.rucio_client = Client()
        #self.xrd = XenonRunDatabase.XenonRunDatabase()

    def init(self):
        print("init")
        print(global_dictionary)
        #self.db = ConnectMongoDB()
        #self.db.Connect()

    def run(self,*args, **kwargs):
        print("here runs the tester")
        #self.init() this is only important if you don't call run() in admix.py
        logging.info("Yeah")

        #query = {}
        #self.db.SetProjection(projection={'number':True, 'name':True, '_id':True}, from_config=False)

        #collection = self.db.GetQuery(query)

        #self.db.Connect()
        #self.db.SetProjection(projection={'number':True, 'name':True, '_id':True, 'data':True, 'detector':True}, from_config=False)

#        c = 0
#        cUp = 1e8
#        for i_run in collection:
#            r_name = i_run['name']
#            r_number = i_run['number']
#
#            df = self.db.GetRunByName(r_name)
#            if len(df) == 0:
#                continue
#            if 'data' not in df[0] or len(df[0]['data'])== 0:
#                continue
#
#            data = df[0]['data']
#            print(r_name, r_number)
#            for i_data in data:
#                print("- ", i_data['type'], i_data['location'])
#
#            print("")
#
#
#           if c >= cUp:
#               break
#            c+=1
#
    def __del__(self):
        print( 'tester stop')
