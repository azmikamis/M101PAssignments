import pymongo
import sys
import json


conn = pymongo.MongoClient("mongodb://localhost")
db = conn.blog
col = db.posts
#col.drop()

#filepath = 'handouts/homework_5_1/posts.json'
#with open(filepath,'rb') as f:
#    for line in f:
#        jsondoc = json.loads(line.replace('$oid','_id').replace('$date','date'))
#        col.insert(jsondoc)

try:
    pipeline = [
        {"$unwind": "$comments"},
        {"$group": {"_id": "$comments.author", "count": {"$sum": 1}}},
        {"$sort": { "count" : -1}},
        {"$limit" : 1}
    ]

    for item in list(col.aggregate(pipeline)):
        print item

except Exception as e:
    print "Error trying to read collection:", type(e), e


