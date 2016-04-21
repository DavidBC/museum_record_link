from __future__ import with_statement

import os
import glob
import sys
import logging
import pymongo
import json
import csv
import datetime
from bson.json_util import dumps
#from RecordLink import RecordLink

class MongoInit:

	client = pymongo.MongoClient()
	db = client.test
	path = './datasets'
	current_year = datetime.datetime.now().year

	def load_dataset(self):
		for fname in glob.glob(os.path.join(self.path, '*.json')) :
			with open(fname) as f:
				print(fname)
				source_dataset = fname.split('/')[-1]
				people = json.loads(f.read())["people"]
				for person in people:
					if 'schema:name' in person:
						#if 'schema:deathDate' in person:
						#	try:
						#		if int(person['schema:deathDate']) > current_year:
						#			person['schema:deathDate'] = 'null'
						#	except:
						#		continue
						person['dataset'] = source_dataset
						person['schema:familyName'] = person['schema:name'].split(' ')[-1]
						person['nameSplit'] = person['schema:name'].split(' ')
						result = self.db.artists.insert(person)

	def create_indexes(self):
		self.db.artists.create_index([("@id", pymongo.ASCENDING)])
		self.db.artists.create_index([("schema:name", pymongo.ASCENDING)])
		self.db.artists.create_index([("schema:familyName", pymongo.ASCENDING)])
		self.db.artists.create_index([("nameSplit", pymongo.ASCENDING)])
		self.db.artists.create_index([("schema:birthDate", pymongo.ASCENDING)])
		self.db.artists.create_index([("schema:deathDate", pymongo.ASCENDING)])
		self.db.artists.create_index([("dataset", pymongo.ASCENDING)])
	
	def output_links(self, outputFile):
		cursor = self.db.linkRecords.find()
		records = (list(cursor))
		for record in records:
			record.pop('_id', None)
		output = {"bulk": len(records), "payload": records}
		print(len(records))
		with open(outputFile, 'w') as out :
			x = json.dumps(output)
			out.writelines(x)

if __name__ == "__main__":

	mongo = MongoInit()
	#mongo.output_links('newoutput.json')
	mongo.db.artists.drop()
	mongo.load_dataset()
	mongo.create_indexes()
	
