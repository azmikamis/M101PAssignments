import pymongo
import sys
import json


conn = pymongo.MongoClient("mongodb://localhost")
db = conn.students
col = db.grades
col.drop()

filepath = 'handouts/homework_2_1/grades.json'
with open(filepath,'rb') as f:
    for line in f:
        jsondoc = json.loads(line.replace('$oid','_id'))
        col.insert(jsondoc)

try:
    iter = col.find({'score' : { '$gte' : 65 }}).sort('score', 1).limit(1)
    for item in iter:
        print item

except Exception as e:
    print "Error trying to read collection:", type(e), e


