from io import BytesIO
import sys
import pycurl
import time
import random
import json
from pymongo import MongoClient


if(len(sys.argv)!=3):
	print("ERROR; USAGE: python {} <dbName> <collectionName>".format(sys.argv[0]))
	sys.exit()

DB=sys.argv[1]
collection=sys.argv[2]

db = MongoClient()[DB]


PRIMARY_URL="https://api.coursera.org/api/courses.v1?fields=language,description,workload,previewLink,domainTypes,categories,certificates,startDate,specializations,id&start="

def inputData(parser):
	for entry in parser.get("elements", []):
		db[collection].insert_one(entry)
	return True

def getContent(url):
	time.sleep(3*random.random())
	try:
		c = pycurl.Curl()
		storage = BytesIO()
		c.setopt(c.URL, url)
		c.setopt(c.WRITEFUNCTION, storage.write)
		c.setopt(c.CAPATH, "/etc/ssl/certs/")
		c.setopt(c.CAINFO, "/etc/ssl/certs/ca-certificates.crt")
		c.setopt(c.FOLLOWLOCATION, True)
		print ("%s - Obtaining courses from -> %s" % (time.strftime("[%d-%m-%Y -> %H:%M:%S]"), url))
		c.perform()

		content = storage.getvalue()
		parser = json.loads(content)
		print ("Getting Content into DB")
		inputData(parser)
		return parser.get("paging", {}).get("next", None)
	except Exception as e:
		print ("%s - ERROR, Get Random Wait -> %s" % (time.strftime("[%d-%m-%Y -> %H:%M:%S]"), str(e)))
nxt=0

while nxt is not None:
	nxt=getContent(PRIMARY_URL+ str(nxt))

print ("Program ended successfully: results are stored in {}.{}".format(DB, collection))