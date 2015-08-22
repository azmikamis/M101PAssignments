import pymongo
import sys
import json


conn = pymongo.MongoClient("mongodb://localhost")
db = conn.school
col = db.students
col.drop()

filepath = 'handouts/homework_3_1/students.json'
with open(filepath,'rb') as f:
    for line in f:
        jsondoc = json.loads(line)
        col.insert(jsondoc)

try:
    iter = col.find()
    for doc in iter:
        lowesthomeworkscore = min(s for s in doc['scores'] if s['type']=="homework")
        col.update_one({'_id' : doc['_id']},
                       {"$pull" : {"scores" : lowesthomeworkscore}})

    pipeline = [
        {"$unwind": "$scores"},
        {"$group": {"_id": "$_id", "average": {"$avg": "$scores.score"}}},
        {"$sort": { "average" : -1}},
        {"$limit" : 1}
    ]

    for item in list(col.aggregate(pipeline)):
        print item

except Exception as e:
    print "Error trying to read collection:", type(e), e


