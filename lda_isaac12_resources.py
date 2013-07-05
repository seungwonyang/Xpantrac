# Filename: lda_isaac12_resources.py
# Description
# Name: Seungwon Yang <seungwon@vt.edu>
# Date: Oct. 25, 2012
# Usage: >python lda_isaac12_resources.py

import codecs
import logging
import MySQLdb
from gensim import corpora, models, similarities

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

db = MySQLdb.connect(host="spare05.dlib.vt.edu", user="twapperkeeper", passwd="tweet!", db="twapperkeeper_2")
table = "isaac12"
  # create a cursor 
cur = db.cursor()
cur.execute("select id,clean_text, resource_title_0, resource_title_1, resource_title_2, resource_title_3, content_0, content_1, content_2, content_3 from " + table + " where clean_text!='NULL' and clean_text!='' and is_rt='0'")
    
documents = []
for row in cur.fetchall():
	document = ""
	for i in range(1, 10):
		if row[i] != "NULL":
			document = document + " " + row[i]
	documents.append(document)

# documents = []
# filepath = "text/"
# for i in range(1,31):
# 	# print i
# 	filename = str(i) + ".txt"
# 	fi = open(filepath + filename, "r").read()	
# 	documents.append(fi)

# print documents
sfi = open('stopwords.txt', 'r').read()
sfi2 = open('custom_stops.txt', 'r').read()
stoplist = set(sfi.split() + sfi2.split())

texts = [[word for word in document.lower().split() if word not in stoplist] for document in documents]
all_tokens = sum(texts, [])
tokens_once = set(word for word in set(all_tokens) if all_tokens.count(word) == 1)
texts = [[word for word in text if word not in tokens_once] for text in texts]
dictionary = corpora.Dictionary(texts)
dictionary.save('isaac12_cleantext_resources.dict')

corpus = [dictionary.doc2bow(text) for text in texts]
corpora.MmCorpus.serialize('isaac12_cleantext_resources.mm', corpus)

tfidf = models.TfidfModel(corpus)
corpus_tfidf = tfidf[corpus]
# lsi = models.LsiModel(corpus_tfidf, id2word=dictionary, num_topics=30)
# lsi.save('model_ctr_30.lsi')
# corpus_lsi = lsi[corpus_tfidf]

# LDA
lda = models.LdaModel(corpus, id2word=dictionary, num_topics=50, chunksize=1000, passes=1, alpha=None, eta=None, decay=0.5)
lda.save('model_isaac12_cleantext_resources.lda')


if __name__ == "__main__":
	# print "\n---------------- LSI TOPICS -------------------\n"
	# print lsi.print_topics(20)

	# make an input doc into text
	
	input_doc = open("input_text2.txt", "r").read()
	ascii_encoded = input_doc.decode('utf-8').encode('ascii', 'ignore')
	input_text = list(set([word for word in ascii_encoded.lower().split() if word not in stoplist]))
	input_text_bow = dictionary.doc2bow(input_text)

	print "input text: %s"  % input_text
	print "topics for input text: %s" % lda[input_text_bow]
	print "\n\n---------------- LDA TOPICS -------------------\n"
	print lda.print_topics(topics=30, topn=10)

