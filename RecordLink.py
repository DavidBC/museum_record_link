from __future__ import print_function
from future.builtins import next


import glob
import logging
import pymongo
import json
import os
import csv
import json
import re
import collections
import logging
import optparse
import numpy
import collections
import dedupe

class RecordLink:

	DATASET1 = 'ULAN.json' #primary dataset
	DATASET2 = 'SAAM.json'	

	VERSION_NUM = 1.1
	OUTPUT_FILE = './dedupe/recordlinks.json'
	SETTINGS_FILE = './dedupe/data_matching_learned_settings'
	TRAINING_FILE = './dedupe/data_match.json'
	COMPARE_FIELDS = ['schema:name', 'schema:birthDate', 'schema:deathDate'] #fields used for comparison
	MAX_BLOCK_SQUARE = 3000000 # size of block1 * block2, determines memory usage
	LETTERS = map(chr, range(ord('a'), ord('z')+1))

	client = pymongo.MongoClient()
	db = client.test

	def loadBlock(self, dataset, name_prefix):
		selected_fields = {'@id': 1}		
		for field in self.COMPARE_FIELDS:
			selected_fields[field] = 1

		cursor = self.db.artists.find( {"dataset": dataset, 
				"nameSplit": { '$regex':'^{0}'.format(name_prefix), '$options': 'i' }},
				selected_fields)

		data_d = {}
		for person in cursor:
			fields = {}
			#check for missing fields, make them null
			for field_name in self.COMPARE_FIELDS:
				if field_name in person:
					#dedupe comparison requires strings
					fields[field_name] = unicode(person[field_name])
				else:
					fields[field_name] = 'null'

			#create dictionary for dedupe to work with
			data_d[person['@id']] = fields
		return data_d

	def linkRecords(self, data_1, data_2) :
		if os.path.exists(self.SETTINGS_FILE):
			with open(self.SETTINGS_FILE, 'rb') as sf :
				linker = dedupe.StaticRecordLink(sf)
		else:

			fields = [
				{'field':unicode('schema:name'), 'type':'String'},
				{'field':unicode('schema:birthDate'), 'type':'ShortString', 'has missing':True},
				{'field':unicode('schema:deathDate'), 'type':'ShortString', 'has missing':True}
			]

			linker = dedupe.RecordLink(fields, num_cores=4)
			print('created linker')
			linker.sample(data_1, data_2, 10000)
			print('created linker sample')

			if os.path.exists(self.SETTINGS_FILE):
				print('reading labeled examples from ', self.SETTINGS_FILE)
				with open(self.SETTINGS_FILE) as tf :
				    linker.readTraining(tf)

			print('starting active labeling...')

			dedupe.consoleLabel(linker)

			linker.train()

			with open(self.TRAINING_FILE, 'w') as tf : #write training file
				linker.writeTraining(tf)

			with open(self.SETTINGS_FILE, 'wb') as sf : #write dedupe settings file
				linker.writeSettings(sf)
			
		for field in linker.blocker.index_fields:
			field_data1 = set(record[1][field] for record in data_1.items())
			field_data = set(record[1][field] for record in data_2.items()) | field_data1
			linker.blocker.index(field_data, field)

		blocks = collections.defaultdict(lambda : ([], []) )
		for block_key, record_id in linker.blocker(data_1.items()) :
			blocks[block_key][0].append((record_id, data_1[record_id], set([])))
		for block_key, record_id in linker.blocker(data_2.items()) :
			if block_key in blocks:
				blocks[block_key][1].append((record_id, data_2[record_id], set([])))
		for k, v in blocks.items():
			if not v[1] or not v[0]:
				del blocks[k]

		linked_records = linker.matchBlocks(blocks.values(), threshold=.5) #edit threshold for dedupe link threshold
	
		return linked_records

		
	def dbOutput(self, linked_records) :
		for record in linked_records:
			link = {'uri1': record[0][0], 'uri2': record[0][1], 
			'dedupe': {'version': unicode(self.VERSION_NUM), 'linkscore': unicode(record[1]),
			'fields': self.COMPARE_FIELDS, 'dataset': self.DATASET2 } }
			self.db.linkRecords.insert(link)

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

	
	def getLinkedRecords(self, name_prefix, dataset1, dataset2) :
		data_1 = self.loadBlock(dataset1, name_prefix)
		data_2 = self.loadBlock(dataset2, name_prefix)
		if len(data_1) == 0 or len(data_2) == 0:
			return #founds empty blocks

		if (len(data_1) * len(data_2)) > self.MAX_BLOCK_SQUARE:
			for letter in self.LETTERS:
				new_name_prefix = name_prefix + letter
				self.getLinkedRecords(new_name_prefix, dataset1, dataset2)
		else:
			linked_records = linker.linkRecords(data_1, data_2)
			self.dbOutput(linked_records)
			print(self.DATASET2, ' # linked records:', len(linked_records), 'on blocking: ', name_prefix)

	#return list of datasets to link with ULAN
	def getDatasets(self) : 
		cursor = self.db.artists.distinct('dataset')
		datasets = list(cursor)
		datasets.remove('ULAN.json')
		print(datasets)
		return datasets

	def AutryMakers(self):
		cursor = self.db.linkRecords.find({'dedupe.dataset':'AutryMakers.json'}, {'uri1': 1, 'uri2':1})
		links = list(cursor)
		foundLinks = []
		for link in links:
			foundLinks.append((link['uri1'], link['uri2']))
		autryUlan = []
		with open("./datasets/AutryMakers.json") as f:
				people = json.loads(f.read())["people"]
				for person in people:
					autryUlan.append((person["ULAN_ID"], person["@id"]))
		
		combined = [id for id in autryUlan if id in foundLinks]
		precision = float(len(combined)) / len(foundLinks)
		recall = float(len(foundLinks)) / len(autryUlan)
		print("AutryMakers: precision: ", precision, ", recall: ", recall)
			
if __name__ == "__main__":
	
	optp = optparse.OptionParser()
	optp.add_option('-v', '--verbose', dest='verbose', action='count',
			        help='Increase verbosity (specify multiple times for more)'
			        )
	(opts, args) = optp.parse_args()
	log_level = logging.WARNING 
	if opts.verbose :
		if opts.verbose == 1:
			log_level = logging.INFO
		elif opts.verbose >= 2:
			log_level = logging.DEBUG
	logging.getLogger().setLevel(log_level)

	linker = RecordLink()
	linker.db.linkRecords.drop()

	datasets = linker.getDatasets()
	for dataset in datasets: #link ULAN to every dataset in db
		linker.DATASET2 = dataset
		for letter1 in linker.LETTERS:
			#initially block by first two letter of each name
			for letter2 in linker.LETTERS:
				linker.getLinkedRecords(letter1+letter2, linker.DATASET1, linker.DATASET2)

		cursor = linker.db.linkRecords.find()
		print('Total linked records: ', len(list(cursor)))

	linker.output_links(linker.OUTPUT_FILE)
	linker.AutryMakers() #print precision and recall for AutryMakers
