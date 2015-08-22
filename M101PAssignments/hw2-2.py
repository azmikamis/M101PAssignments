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
    iter = col.find({'type' : 'homework'}).sort([('student_id', 1), ('score', 1)])
    prev = -1
    for doc in iter:
        if doc['student_id'] != prev:
            prev = doc['student_id']
            col.delete_one({'_id' : doc['_id']})

    pipeline = [
        {"$group": {"_id": "$student_id", "average": {"$avg": "$score"}}},
        {"$sort": { "average" : -1}},
        {"$limit" : 1}
    ]

    for item in list(col.aggregate(pipeline)):
        print item

except Exception as e:
    print "Error trying to read collection:", type(e), e


