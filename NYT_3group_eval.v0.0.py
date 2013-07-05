# Name: Seungwon Yang  Mar. 23, 2013
# Filename: compute_cos_sim_human_machine_4.py
# Description: this script constructs a 'topic answer sheet' based on
#              human topics, then computes P, R, F-1
# Note: Bing Azure search API is used.
# Usage: #>python compute_cos_sim_human_machine_4.py

import sys
import re
import codecs
import json
import logging
import matplotlib.pyplot as plot
import MySQLdb
import requests
import string
import urllib2

# from gensim import corpora, models, similarities
from math import log, sqrt
from nltk import pos_tag, word_tokenize, stem
from nltk.stem.wordnet import WordNetLemmatizer
from numpy import *
from operator import itemgetter # to sort dictionaries by their keys

class CosSim:
	# make a stopword set
	def __init__(self, stop_path, custom_stop_path):
	# def __init__(self):
		stop_li = open(stop_path, "r").read().split()
		custom_stop_li = open(custom_stop_path, "r").read().split()
		self.stoplist = set(stop_li + custom_stop_li)
		# self.stoplist = []
		self.wdict = {}
		self.sum_dict = {}
		self.dcount = 0	

	def removeSymLemmatize(self, string_space_sep):
		lemm = WordNetLemmatizer()
		no_symbol = re.sub(r'[^\w]', ' ', string_space_sep)
		parsedLi = no_symbol.split()
		cleanLi = []
		for w in parsedLi:
			if (w in self.stoplist) or (len(w) < 3):
				continue
			else:
				cleanLi.append(lemm.lemmatize(w))
		return cleanLi

	def computeRolling(self, a_li, b_li):
		rolling = 0.0
		numA = len(a_li)
		numB = len(b_li)

		intersect_li = list(set(a_li) & set(b_li))
		# union_li = list(set(a_li) | set(b_li))
		numIntersection = len(intersect_li)
		# numUnion = len(union_li)

		rolling = float(2*numIntersection)/float(numA + numB)
		return rolling

	def makeMicroCorpus(self, host, user, passwd, db, dbtable, dbtable2, machine_id, human_id_li):
		db = MySQLdb.connect(host, user, passwd, db)
		# create a cursor 
		cur = db.cursor()
		# dbtable = "human_lem"  # Hurricane Isaac tweet collection
		micro_corpus = []
		for i in range(1,31):
			# print "------------topics: %d ------------\n" % i
			# query = "update "+ dbtable +" set topics" + str(i) + "= replace(topics" + str(i) +", ',', ' ')"
			# query = "update "+ dbtable +" set topics" + str(i) + "=LOWER(topics" +str(i) + ")"
			# cur.execute(query)
			corpus_per_topics = []
			# for j in [1,3,4]:
			for j in human_id_li:
			  query = "select topics" + str(i) + " from "+dbtable+" where participant_id=" + str(j)
			  cur.execute(query)
			  single_doc_in_corpus = " ".join(cur.fetchone()[0].split(","))
			  corpus_per_topics.append(single_doc_in_corpus)
			# get topics data from machine
			query2 = "select topics" + str(i) + " from "+dbtable2+" where id=" + str(machine_id)
			cur.execute(query2)
			machine_doc_in_corpus = " ".join(cur.fetchone()[0].split(","))
			corpus_per_topics.append(machine_doc_in_corpus)
			micro_corpus.append(corpus_per_topics)
		return micro_corpus

def main():
	logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
	host = "spare05.dlib.vt.edu"

	user = "ctr_30"
	passwd = "03_rtc!"
	db = "ctr_30"
	dbtable = "human_lem"
	dbtable2 = "machine_lem"
	# dbtable2 = "machine_lem_tfdf"
	human_id_li = [1,3,4]

	# user = "various_30"
	# passwd = "03_suoirav!"
	# db = "various_30"
	# dbtable = "human_lem"
	
	# dbtable2 = "machine_lem"
	# # dbtable2 = "machine_lem_tfdf"

	# human_id_li = [11,12,13,14,15]

	# for machine_id in range(1,49): #   1 <= machine id <= 48
	for machine_id in range(31,32): #   1 <= id <= 49
	# for machine_id in range(10,11): #   best performing m10 in various_30 with tf*df
	# for machine_id in range(9,10): #   m8 in various_30 with tf*df
	# for machine_id in range(39,40): #   M39 - better than M31
		print "\nmachine_id: %d ------------------" % machine_id
		for num_machine_top in range(1,11): # num machine topics 1-10


			cs = CosSim("stopwords.txt", "custom_stops.txt")
			micro_corpus = cs.makeMicroCorpus(host, user, passwd, db, dbtable, dbtable2, machine_id, human_id_li)
			# print micro_corpus	
			keepNum = 1
			agreed_ratio_sum = 0.0
			precision_sum = 0.0
			recall_sum = 0.0
			f1_sum = 0.0
			for doc in micro_corpus:
				doc_tag_dic = {}
				liH_all = []
				liM = []
				# print "Doc id: %s----" % keepNum
				for inde in range(len(human_id_li)):
					liH_all += cs.removeSymLemmatize(doc[inde])
				liM = cs.removeSymLemmatize(doc[len(human_id_li)])[1:num_machine_top+1]
				# construct answers
				for tt in liH_all:

					if tt in doc_tag_dic:
						doc_tag_dic[tt] += 1
					else:
						doc_tag_dic[tt] = 1
				# print "doc_tag_dic: ----------"
				# print doc_tag_dic

				sorted_list = sorted(doc_tag_dic.iteritems(), key=itemgetter(1), reverse=True)
				topic_union = [finalist[0] for finalist in sorted_list if finalist[1] > (len(human_id_li)/2)]
				# print "sorted list-----"
				# print sorted_list

				# print "topic union:-----" 
				# print topic_union
				all_count = len(sorted_list)
				# print all_count
				agreed_count = len(topic_union)
				
				# print agreed_count
				agreed_ratio = (float(agreed_count)/float(all_count))
				# print "agreed ratio: %0.3f" % agreed_ratio
				agreed_ratio_sum += agreed_ratio

				# num intersection
				num_intersect = len(set(topic_union) & set(liM))
				precision = float(num_intersect) / float(num_machine_top)
				recall = float(num_intersect) / float(agreed_count)
				f1 = 0.0
				if num_intersect != 0:
					f1 = 2*precision*recall / (precision + recall)
				else:
					f1 = 0.0

				# print "Precision: %0.3f" % precision
				# print "Recall: %0.3f" % recall
				# print "F-1: %0.3f" % f1
				precision_sum += precision
				recall_sum += recall
				f1_sum += f1

				keepNum += 1
			# print "Ave agreed ratio: %0.3f" % (agreed_ratio_sum / float(30))
			# print "Ave Precision: %0.3f" % (precision_sum / float(30))
			# print "Ave Recall: %0.3f" % (recall_sum / float(30))
			# print "Ave F-1: %0.3f" % (f1_sum / float(30))

			print "%0.3f,%0.3f,%0.3f,%0.3f" % ((agreed_ratio_sum / float(30)),(precision_sum / float(30)), (recall_sum / float(30)), (f1_sum / float(30)))
			



if __name__ == "__main__":
	main()