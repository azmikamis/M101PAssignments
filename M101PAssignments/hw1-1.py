import pymongo
import sys
# Copyright 2013, 10gen, Inc.
# Author: Andrew Erlichson


# connnecto to the db on standard port
connection = pymongo.MongoClient("mongodb://localhost")
db = connection.m101                 # attach to db
col = db.hw1         # specify the colllection

try:
    iter = col.find()
    for item in iter:
        print '{} {}'.format(item['question'], item['answer'])

except Exception as e:
    print "Error trying to read collection:", type(e), e


