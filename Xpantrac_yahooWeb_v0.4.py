# Name: Seungwon Yang  June 25, 2013
# Filename: Xpantrac_yahooWeb_v0.4.py
# Description: this script finds topics in two parts: 
# Note: 
#
# Usage: #>python Xpantrac_yahooWeb_v0.4.py
#
# ---------- Note: num_topics = 20 -----------/////////////
#
# Parameters:
#   * unit_size = [20,25,10,5]  (maybe include '1' as well)
#	* num_api_return = [50,10,1]	
#   * tf_idf = [1, 0]
#   * only_noun = [1, 0]

import ast  # to convert a list (in string type) to an actual list type
import sys
import re
import codecs
import httplib2
import json
import logging
import math
import MySQLdb
import oauth2
import requests
import simplejson
import string
import time
import urllib2

# from gensim import corpora, models, similarities
from HTMLParser import HTMLParser
from math import log, sqrt
from nltk import pos_tag, word_tokenize
from nltk.stem.wordnet import WordNetLemmatizer
from numpy import *
from operator import itemgetter # to sort dictionaries by their keys

# functions to compute intersection and union of topics
def computeIntersection(a_li, b_li):
	return list(set(a_li) & set(b_li))

def computeUnion(a_li, b_li):
	# return list(set(a_li) & set(b_li))
	return list(set(a_li) | set(b_li))

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()
	
class Xpantrac:
	# make a stopword set
	def __init__(self, stop_path, custom_stop_path):
		stop_li = open(stop_path, "r").read().split()
		custom_stop_li = open(custom_stop_path, "r").read().split()
		self.stoplist = set(stop_li + custom_stop_li)
		self.wdict = {}
		self.sum_dict = {}
		self.dcount = 0	

	# this function removes symbol characters, and ignores foreign languages
	def removeSymbols(self, any_text):	
		# first, decode already encoded text
		utf8_str = any_text.decode('utf-8')
		# then, encode with ascii only
		only_ascii = utf8_str.encode('ascii', 'ignore')
		# only_ascii = any_text.encode('ascii', 'ignore')
		# remove symbol characters
		return re.sub(r'[^\w]', ' ', only_ascii)		
	
	def parseInput(self, text):
		clean_text = self.removeSymbols(text).lower()
		# use gensim.utils.simple_preprocess(doc) -------------------
		return [t for t in clean_text.split() if (len(t) > 1) and (t not in self.stoplist)]

	def parse2Dict(self, micro_corpus):
		# words = " ".join(micro_corpus).split();
		lm = WordNetLemmatizer()
		doc_list = []
		nounsOnly = ["NN", "NNS", "NNP"]
		for doc in micro_corpus:
			# check nounsOnly value
			pos_tag_li = pos_tag(word_tokenize(doc))
			for key,val in pos_tag_li:
				if val in nounsOnly:
					# doc_list.append(key.lower())
					doc_no_symbol = re.sub(r'[^\w]', '', key)
					doc_list.append(doc_no_symbol.lower())

			for w in doc_list:
				w = lm.lemmatize(w)
				if (w in self.stoplist) or (len(w) < 3):
					continue
				elif w in self.wdict:
					self.wdict[w].append(self.dcount)
				else:
					self.wdict[w] = [self.dcount]
			self.dcount += 1   

	def makeQueryUnits(self, parsed_input_li, unit_size=5, window_overlap=1):
	    unit = []
	    query_list = []
	    list_len = len(parsed_input_li)
	    # if unit_size ==1, no windowing.
	    if unit_size==1:
	    	for item in parsed_input_li:
	    		unit = [item]
	    		query_list.append(unit)
	    	return query_list
	    i = 0
	    while (i < list_len):
	    	unit = parsed_input_li[i:i+unit_size]
	    	if len(unit) < unit_size:  # last unit
	    		query_list.append(unit)
	    		return query_list
	    	else:
	    		query_list.append(unit)
	    		i += unit_size - window_overlap

	def makeYahooCorpus(self, query_list, num_api_return, yahoo_api_type):
		# -------------- how about using only the nouns (and compound nouns) ?
		# -------------- maybe use POS tagger?  which one?  
		# -------------- also consider lemmatization: gensim.utils.lemmatize(content)
	    OAUTH_CONSUMER_KEY = "dj0yJmk9VGlaMDZxc2p0UkZNJmQ9WVdrOVJHNU5VbE5OTnpZbWNHbzlORFUyT0RNeE9UWXkmcz1jb25zdW1lcnNlY3JldCZ4PTUx"
	    OAUTH_CONSUMER_SECRET = "1b69f0a58d83a62cf9b43594dc9f4274594195ed"
	    micro_corpus = []
	    micro_corpus_0 = []
	    micro_corpus_1 = []
	    num_results_returned_li = []
	    
  		# ------------ adjust query_list size here ---------------

	    # for item in query_list[:5]: # [[query unit 1], [query unit 2], [query unit 3],...]
	    for item in query_list: # [[query unit 1], [query unit 2], [query unit 3],...]
	        query = " ".join(item).replace(" ", "%20")
	        print query

	        num_results_returned = 0
	        if query != "":
	            try:
	                url = ""
	                if yahoo_api_type == "web":
	                    url = "http://yboss.yahooapis.com/ysearch/web?q=" + query
	                else:
	                    url = "http://yboss.yahooapis.com/ysearch/news?q=" + query
	                consumer = oauth2.Consumer(key=OAUTH_CONSUMER_KEY,secret=OAUTH_CONSUMER_SECRET)
	                params = {
	                    'oauth_version': '1.0',
	                    'oauth_nonce': oauth2.generate_nonce(),
	                    'oauth_timestamp': int(time.time()),
	                }

	                oauth_request = oauth2.Request(method='GET', url=url, parameters=params)
	                oauth_request.sign_request(oauth2.SignatureMethod_HMAC_SHA1(), consumer, None)
	                oauth_header=oauth_request.to_header(realm='yahooapis.com')

				    # Get search results
	                http = httplib2.Http()
	                resp, content = http.request(url, 'GET', headers=oauth_header)
	                # print resp
	                # print content
	                results = simplejson.loads(content)
	                # combine all returned results into a single string and append it
	                group_str_0 = ""
	                group_str_1 = ""
					
	                results_li = []
	                if yahoo_api_type == 'web':
						# for M_39 configuration (50 results merged) ------//
		                results_li = results['bossresponse']['web']['results']
	                else:
		                results_li = results['bossresponse']['news']['results']

	                # keep track of the number of results for each API query
	                num_results_returned = len(results_li)

	                # for result in results_li[0:20]:
	                for result in results_li:
	                    clean_result = strip_tags(result['abstract']).replace("...", "").strip().replace("\"", "")
	                    # clean_result = " ".join(self.parseInput(result['Description']))
	                    # print clean_result
	                    group_str_0 = group_str_0 + " " + clean_result
	                micro_corpus_0.append(group_str_0)
	                # print "micro_corpus_0---------------------------"
	                # print micro_corpus_0

	                # for M_43 configuration (only max. 10 results merged) ------------//
	                for result in results_li[0:10]:
	                    clean_result = strip_tags(result['abstract']).replace("...", "").strip().replace("\"", "")
	                    # clean_result = " ".join(self.parseInput(result['Description']))
	                    # print clean_result
	                    group_str_1 = group_str_1 + " " + clean_result
	                micro_corpus_1.append(group_str_1)
	                # print "micro_corpus_1---------------------------"
	                # print micro_corpus_1

	                # add both micro_corpus_0 and micro_corpus_1 to micro_corpus
	                # micro_corpus.append(micro_corpus_0)
	                # micro_corpus.append(micro_corpus_1)

	            except Exception, err:
	                #sys.stderr.write('ERROR: %s\n' % str(err))
	                print "Something wrong............"

	                pass
                # micro_corpus.append(micro_corpus_0)
                # micro_corpus.append(micro_corpus_1)

	        else:
	        	num_results_returned = 0

	        num_results_returned_li.append(num_results_returned)

	    # print "micro_corpus_1---------------------------"
	    # print micro_corpus_1

	    micro_corpus.append(micro_corpus_0)
	    micro_corpus.append(micro_corpus_1)
	    micro_corpus.append(num_results_returned_li)

		# # print len(micro_corpus)
	 #    for corp in micro_corpus:
	 #    	print corp
	 #    	print "\n--------------------------------\n"

	    print micro_corpus[0]
	    print "\n--------------------------------\n"
	    print micro_corpus[1]
	    print "\n--------------------------------\n"
	    print micro_corpus[2]
	    print "\n--------------------------------\n"

	    # print "Size of the micro_corpus: %d" % len(micro_corpus)

	    # micro_corpus = [corpus_50, corpus_10, api_return_count_li]
	    return micro_corpus

	def makeCorpus(self, db_cursor, doc_id, dbtable):
	    micro_corpus = []
	    micro_corpus_0 = []
	    micro_corpus_1 = []

	    # construct db query to collect all query results of doc_id
	    query = "select query_id,query,results from " + dbtable + " where doc_id=" + str(doc_id)
	    db_cursor.execute(query)
	    for item in db_cursor.fetchall():
	    	# print item[0]
	    	# print item[1]
	    	result_str = item[2]
	    	
	    	# For M_39 (50 result) - use all result_li
	    	result_li = ast.literal_eval(result_str)
	    	micro_corpus_0.append(" ".join(result_li))

	    	# For M_43 (10 result) - use top 10 resulting descriptions
	    	micro_corpus_1.append(" ".join(result_li[0:10]))
	    return [micro_corpus_0, micro_corpus_1]

	def makeCorpus_progressive(self, db_cursor, doc_id, dbtable):
	    micro_corpus = []
	    micro_corpus_0 = []
	    micro_corpus_1 = []

	    # construct db query to collect all query results of doc_id
	    query = "select query_id,query,results from " + dbtable + " where doc_id=" + str(doc_id)
	    db_cursor.execute(query)
	    # compute number of total queries
	    returned_results_li = db_cursor.fetchall()
	    num_queries = len(returned_results_li)
	    for item in returned_results_li:
	    	# print item[0]
	    	# print item[1]
	    	result_str = item[2]
	    	
	    	# For M_39 (50 result) - use all result_li
	    	result_li = ast.literal_eval(result_str)
	    	micro_corpus_0.append(" ".join(result_li))

	    	# For M_43 (10 result) - use top 10 resulting descriptions
	    	micro_corpus_1.append(" ".join(result_li[0:10]))
	    half_num_queries = num_queries / 2

	    return [micro_corpus_0[0:half_num_queries],micro_corpus_0[half_num_queries:-1], micro_corpus_1[0:half_num_queries],micro_corpus_1[half_num_queries:-1]]

	def makeCorpus_weighted(self, db_cursor, doc_id, dbtable, weight_ratio):
		# weight_ratio: (e.g., 0.2 for weighting top 20% of input text)
	    micro_corpus = []
	    micro_corpus_0 = []
	    micro_corpus_1 = []

	    # construct db query to collect all query results of doc_id
	    query = "select query_id,query,results from " + dbtable + " where doc_id=" + str(doc_id)
	    db_cursor.execute(query)
	    # compute number of total queries
	    returned_results_li = db_cursor.fetchall()
	    num_queries = len(returned_results_li)
	    # compute duplication iteration
	    dup_iter = int(math.ceil(num_queries * weight_ratio))
	    print "dup_iter: %d ---------" % dup_iter

	    for item in returned_results_li:
	    	# print item[0]
	    	# print item[1]
	    	result_str = item[2]
	    	
	    	# For M_39 (50 result) - use all result_li
	    	result_li = ast.literal_eval(result_str)
	    	micro_corpus_0.append(" ".join(result_li))

	    	# For M_43 (10 result) - use top 10 resulting descriptions
	    	micro_corpus_1.append(" ".join(result_li[0:10]))

	    # weight the corpus with duplicate data -----------------
	    for item in returned_results_li[0:dup_iter]:
	    	result_str = item[2]
	    	
	    	# For M_39 (50 result) - use all result_li
	    	result_li = ast.literal_eval(result_str)
	    	micro_corpus_0.append(" ".join(result_li))

	    	# For M_43 (10 result) - use top 10 resulting descriptions
	    	micro_corpus_1.append(" ".join(result_li[0:10]))


	    return [micro_corpus_0, micro_corpus_1]

	def build(self):
		# collect keys which appear more than once in the corpus
		self.keys = [k for k in self.wdict.keys() if len(self.wdict[k]) > 1]
		self.keys.sort()
		num_keys = len(self.keys)
		min_cell_val = 1 / float(num_keys)
		# create a zero matrix of terms (i.e., keys) and document numbers (i.e., columns)
		self.A = zeros([len(self.keys), self.dcount])
		# update cell values with wdict values
		for i, k in enumerate(self.keys):
			for d in self.wdict[k]:               
				self.A[i,d] = self.A[i,d] + 1
		
		# ------ if a column sum is 0, fill it with min_cell_val
		WordsPerDoc = sum(self.A, axis=0)
		zero_col_index = where(WordsPerDoc==0)
		for index in zero_col_index:
			self.A[:,index] = min_cell_val

	# this function adds all cell values for each key (i.e., row)
	def build_sum_dict(self):
	    rows, cols = self.A.shape 
	    for i, k in enumerate(self.keys):
	        row_sum = 0
	        for j in range(cols):
	            row_sum = row_sum + self.A[i, j]        
	        self.sum_dict[k] = row_sum     
	        
	def extTopics(self, num_topics):		
	    # sort self.sum_dict in descending order
		sorted_list = sorted(self.sum_dict.iteritems(), key=itemgetter(1), reverse=True)
	    # select top num_topics and print them out
		topics_li = []
		topics_str = ""
		for item, score in sorted_list[:num_topics]:
			topics_li.append(item)
		topics_str = ",".join(topics_li)
		return topics_str

	def extTopicsFrequencyBased(self, corpus, num_topics, nounsOnly):

		doc_list = []
		for doc in corpus:
			# check nounsOnly value
			if nounsOnly != []:
				pos_tag_li = pos_tag(word_tokenize(doc))
				for key,val in pos_tag_li:
					key_str = ""
					if val in nounsOnly:
						# # doc_list.append(key.lower())
						# doc_no_symbol = re.sub(r'[^\w]', '', key)
						# doc_list.append(doc_no_symbol.lower())
						key_str = key_str + " " + key
					doc_list = re.sub(r'[^\w]', '', key_str).lower().split()
					

			else:
				doc_no_symbol = re.sub(r'[^\w]', ' ', doc)
				doc_list = doc_no_symbol.lower().split() 
			for w in doc_list:
				if (w in self.stoplist) or (len(w) < 3):
					continue
				elif w in self.wdict:
					word_freq = self.wdict[w] + 1
					self.wdict[w] = word_freq
				else:
					self.wdict[w] = 1
			 
		
	    # sort self.sum_dict in descending order
		sorted_list = sorted(self.wdict.iteritems(), key=itemgetter(1), reverse=True)
	    # select top num_topics and print them out
		topics_li = []
		topics_str = ""
		for item, score in sorted_list[:num_topics]:
			topics_li.append(item)
		topics_str = ",".join(topics_li)
		return topics_str


	def extTopicsHadoop(self, corpus, num_topics):
		# use Hadoop streaming for efficient topic frequency processing


		pass

	def extTopicsPicloud(self, corpus, num_topics):
		# use Picloud for efficient topic frequency processing


		pass




	# def printTopics2Db(self, num_topics, dbuser, dbpasswd, hostname, dbname, dbtable, iter_id, unit_size, num_api_return, tfidf, only_nouns, input_doc_id):
	def printTopics2Db(self, num_topics, dbuser, dbpasswd, hostname, dbname, dbtable, iter_id, unit_size, num_api_return, df, only_nouns, input_doc_id):
	    # sort self.sum_dict in descending order
		sorted_list = sorted(self.sum_dict.iteritems(), key=itemgetter(1), reverse=True)
	    # select top num_topics and print them out
		topics_li = []
		topics_str = ""
		for item, score in sorted_list[:num_topics]:
			topics_li.append(item)
		topics_str = ",".join(topics_li)
		print topics_str
		# connect to mysql
		db = MySQLdb.connect(host=hostname, user=dbuser, passwd=dbpasswd, db=dbname)
		cur = db.cursor() 

		query = "update "+ dbtable +" set unit_size='" +str(unit_size)+ "', num_api_return='" + str(num_api_return) + "', df='" + str(df) + "', only_nouns='" + str(only_nouns) + "', topics" + str(input_doc_id) +"='" + topics_str + "' where id=" + str(iter_id)

		cur.execute(query)		
	
	def calc_cosine_sim(self, u, v):
		return dot(u, v) / (sqrt(dot(u,u)) * sqrt(dot(v,v)))

def main():
	logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
	# parameters ------------------------------------ (Overall)
	num_topics = 20
	window_overlap = 1
	# ----------------------------------------------- (nyt_3000)
	dbuser = "nyt"
	dbpasswd = "tyn!"
	hostname = "spare05.dlib.vt.edu"
	dbname = "nyt"
	dbtable = "api_results_50"
	dbtable2 = "nyt_3000"

	# develop id list
	fi = open("NYT_3000_IDs.txt", "r")
	li = fi.read().split()
	fi.close()

    # connect to mysql
	db = MySQLdb.connect(host=hostname, user=dbuser, passwd=dbpasswd, db=dbname)
	cur = db.cursor()
	# input text ------------------------------------- (Control inputs)
	micro_corpus = []
	iter_id = 1
	# for filenum in li[0:1]:  # doc id 1-100
	for filenum in li[1:101]:  # doc id 1-101
		# compute processing time for each file
		start_time = time.time()
		print start_time


		text = ""
		m_39_topics = ""
		m_43_topics = ""
		topic_inter_str = ""
		topic_uni_str = ""
		doc_id = filenum

		print "\nDocument ID: %s is being processed.........\n" % doc_id
		# filename = str(filenum) + ".txt"
		# # text = open("..\\NYT_3000\\"+filename, "r").read()   
		# text = open("../NYT_3000/"+filename, "r").read()   

		#-----------------------------
		# xpt_0: num_api_return = 50
		# xpt_1: num_api-return = 10 
		#-----------------------------
		xpt_0 = Xpantrac("stopwords.txt", "custom_stops.txt")
		xpt_1 = Xpantrac("stopwords.txt", "custom_stops.txt")

		# parsed_input_li = xpt_0.parseInput(text)
		# query_list = xpt_0.makeQueryUnits(parsed_input_li, 5, window_overlap)

		# ------ No Weights -----------------		
		# micro_corpus = xpt_0.makeCorpus(cur, doc_id, dbtable)
		
		# ------ Weighted Input Text --------
		# micro_corpus = xpt_0.makeCorpus_weighted(cur, doc_id, dbtable, 0.2)

		# ------ Progressive (four corpus items produced) Input Text --------
		xpt_2 = Xpantrac("stopwords.txt", "custom_stops.txt")
		xpt_3 = Xpantrac("stopwords.txt", "custom_stops.txt")

		micro_corpus = xpt_0.makeCorpus_progressive(cur, doc_id, dbtable)
		# micro_corpus[0]  # m_39 part 1
		# micro_corpus[1]  # m_39 part 2
		# micro_corpus[2]  # m_43 part 1
		# micro_corpus[3]  # m_43 part 2

		# -------------------- Progressive ------------------------------------- #
		if micro_corpus[0] != []:						
			xpt_0.parse2Dict(micro_corpus[0])
			xpt_1.parse2Dict(micro_corpus[1])
			xpt_2.parse2Dict(micro_corpus[2])
			xpt_3.parse2Dict(micro_corpus[3])
			xpt_0.build()
			xpt_1.build()
			xpt_2.build()
			xpt_3.build()

			print "------- m39_20 part 1 ---------"
			xpt_0.build_sum_dict()
			m_39_topics_part1 = xpt_0.extTopics(num_topics)
			print m_39_topics_part1
			print "------- m39_20 part 2 ---------"
			xpt_1.build_sum_dict()
			m_39_topics_part2 = xpt_1.extTopics(num_topics)
			print m_39_topics_part2
			print "------- m43_20 part 1 ---------"
			xpt_2.build_sum_dict()
			m_43_topics_part1 = xpt_2.extTopics(num_topics)
			print m_43_topics_part1
			print "------- m43_20 part 2 ---------"
			xpt_3.build_sum_dict()
			m_43_topics_part2 = xpt_3.extTopics(num_topics)
			print m_43_topics_part2

			query4 = "update "+ dbtable2 +" set m_39_yahooWeb_100doc_20tag_part1='" + m_39_topics_part1 + "', m_39_yahooWeb_100doc_20tag_part2='" + m_39_topics_part2 + "', m_43_yahooWeb_100doc_20tag_part1='" + m_43_topics_part1 + "', m_43_yahooWeb_100doc_20tag_part2='" + m_43_topics_part2 + "' where doc_id=" + str(doc_id)

			cur.execute(query4)


			# # get intersection and union of m_39, m_43
			# m_39_li = m_39_topics.split(',')
			# m_43_li = m_43_topics.split(',')

			# topic_intersection = computeIntersection(m_39_li, m_43_li)
			# topic_inter_str = ",".join(topic_intersection)
			# topic_union = computeUnion(m_39_li, m_43_li)
			# topic_uni_str = ",".join(topic_union)



		# -------------------- Frequency-Based ------------------------------- #		
		# process micro_corpus ONLY when it is not empty -----//
		# if micro_corpus[0] != []:						
		# 	print "------- m39_20 ---------"
		# 	# m_39_topics = xpt_0.extTopicsFrequencyBased(micro_corpus[0], num_topics, [])
		# 	m_39_topics = xpt_0.extTopicsFrequencyBased(micro_corpus[0], num_topics, ["NN", "NNS", "NNP"])
		# 	print "------- m43_20 ---------"
		# 	# m_43_topics = xpt_1.extTopicsFrequencyBased(micro_corpus[1], num_topics, [])
		# 	m_43_topics = xpt_1.extTopicsFrequencyBased(micro_corpus[1], num_topics, ["NN", "NNS", "NNP"])

		# 	# get intersection and union of m_39, m_43
		# 	m_39_li = m_39_topics.split(',')
		# 	m_43_li = m_43_topics.split(',')

		# 	topic_intersection = computeIntersection(m_39_li, m_43_li)
		# 	topic_inter_str = ",".join(topic_intersection)
		# 	topic_union = computeUnion(m_39_li, m_43_li)
		# 	topic_uni_str = ",".join(topic_union)

		# -------------------- original ------------------------------------- #
		# if micro_corpus[0] != []:						
		# 	xpt_0.parse2Dict(micro_corpus[0])
		# 	xpt_1.parse2Dict(micro_corpus[1])
		# 	xpt_0.build()
		# 	xpt_1.build()

		# 	print "------- m39_20 ---------"
		# 	xpt_0.build_sum_dict()
		# 	m_39_topics = xpt_0.extTopics(num_topics)
		# 	print "------- m43_20 ---------"
		# 	xpt_1.build_sum_dict()
		# 	m_43_topics = xpt_1.extTopics(num_topics)

		# 	# get intersection and union of m_39, m_43
		# 	m_39_li = m_39_topics.split(',')
		# 	m_43_li = m_43_topics.split(',')

		# 	topic_intersection = computeIntersection(m_39_li, m_43_li)
		# 	topic_inter_str = ",".join(topic_intersection)
		# 	topic_union = computeUnion(m_39_li, m_43_li)
		# 	topic_uni_str = ",".join(topic_union)

		end_time = time.time()
		print end_time - start_time, "seconds"

		# print "-----------------------"
		# print doc_id
		# print m_39_topics
		# print m_43_topics
		# print topic_inter_str
		# print topic_uni_str	
		# print "-----------------------"

	    # write to mysqldb       
		# query = "update "+ dbtable +" set m_39_bing_100doc_20tag='" + m_39_topics + "', m_43_bing_100doc_20tag='" + m_43_topics + "', 39_AND_43_bing_100doc_20tag='" + topic_inter_str + "', 39_OR_43_bing_100doc_20tag='" + topic_uni_str + "', num_api_results_bing='" + num_api_results_str + "' where doc_id=" + str(doc_id)

		# cur.execute(query)

		# query2 = "update "+ dbtable +" set m_39_yahooWeb_100doc_20tag='" + m_39_topics + "', m_43_yahooWeb_100doc_20tag='" + m_43_topics + "', 39_AND_43_yahooWeb_100doc_20tag='" + topic_inter_str + "', 39_OR_43_yahooWeb_100doc_20tag='" + topic_uni_str + "', num_api_results_yahooWeb='" + num_api_results_str + "' where doc_id=" + str(doc_id)

		# cur.execute(query2)

		# query3 = "update "+ dbtable2 +" set m_39_yahooWeb_100doc_20tag_weighted='" + m_39_topics + "', m_43_yahooWeb_100doc_20tag_weighted='" + m_43_topics + "', 39_AND_43_yahooWeb_100doc_20tag_weighted='" + topic_inter_str + "', 39_OR_43_yahooWeb_100doc_20tag_weighted='" + topic_uni_str + "' where doc_id=" + str(doc_id)

		# cur.execute(query3)

		
		# --------------------------- (Update iteration)
        iter_id += 1


					

if __name__ == "__main__":
	main()