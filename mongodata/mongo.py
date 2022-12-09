import psycopg2
from pymongo import MongoClient
from decimal import Decimal
from bson.decimal128 import Decimal128
from bson.objectid import ObjectId
from datetime import datetime
import time
import json

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, Decimal128):
            return float(str(o))
        if isinstance(o, datetime):
            return str(o)
        return json.JSONEncoder.default(self, o)

client = MongoClient("mongodb+srv://ZoomeRanged:admin@cluster0.zbsedde.mongodb.net/?retryWrites=true&w=majority")
db=client.sample_training
col=db.zips
mdbcur=col.find()
with psycopg2.connect(host="localhost",
    database="postgres",
    user="postgres", 
    password="admin") as pgconn:
    pgcur=pgconn.cursor()

    for doc in mdbcur:
        print(doc)
        data=json.dumps(doc, cls=JSONEncoder)
        query_sql = """insert into zips VALUES ('""" + data + """'::jsonb)"""
        pgcur.execute(query_sql)
        pgconn.commit()
        time.sleep(1)
    pgcur.close()
    mdbcur.close()