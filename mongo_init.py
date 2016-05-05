#Cleans and loads datasets into local mongodb for local record linkage

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
from unidecode import unidecode
import re
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
						if 'schema:deathDate' in person:
							person['schema:deathDate'] = self.fixDeathDate(person['schema:deathDate'])
						if 'schema:birthDate' in person:
							person['schema:birthDate'] = self.getYearDate(person['schema:birthDate'])
						person['schema:name'] = unidecode(person['schema:name']) #change names to ASCII
						person['dataset'] = source_dataset #record dataset person is from
						person['schema:familyName'] = person['schema:name'].split(' ')[-1]
						person['nameSplit'] = person['schema:name'].split(' ') #split name into array for blocking
						result = self.db.artists.insert(person)

	#fix datasets that have an arbitrary future date as deathDate
	def fixDeathDate(self, deathDate):
		deathDate = self.getYearDate(deathDate)
		if deathDate:
			try:
				dd = int(deathDate)
				if dd > self.current_year:
					deathDate = ''
			except ValueError:
				print(deathDate)
				return deathDate
		return deathDate
	
	#extract year from dates, requires better solution for people born before 1000 AD
	def getYearDate(self, date):
		if date:
			
			try:
				return int(date)
			except ValueError:
				year = re.search('[1-2]\d\d\d', date)
				if year:
					return year.group()
		else:
			return date

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
		print(len(records))
		for record in records:
			record.pop('_id', None)
		output = {"bulk": len(records), "payload": records}
		with open(outputFile, 'w') as out :
			x = json.dumps(output)
			out.writelines(x)

			
if __name__ == "__main__":

	mongo = MongoInit()
	#mongo.output_links('newoutput.json')
	mongo.db.artists.drop()
	mongo.load_dataset()
	mongo.create_indexes()

	
