# Filename: content_extractor.py
# Description: This script extracts contents from webpages and add to MySQL database. 
#              
# shell>python content_extractor.py
# Name: Seungwon Yang  <seungwon@vt.edu>
# Date: Oct. 7, 2012

# Note: This version will skip URLs:
#       (1) which request authentication (it makes the script hang)
#       (2) which return HTTP error

import base64
import sys 
import string
import re
import codecs
import urllib
import urllib2
import datetime
import MySQLdb
import os
import sets
from bs4 import BeautifulSoup
from nltk.stem.wordnet import WordNetLemmatizer
# from threading import Thread
# import Queue

def removeSymbols(word):
  return re.sub(r'[^\w]', ' ', word)
  
def ext_content():
  lm = WordNetLemmatizer()
  db = MySQLdb.connect(host="spare05.dlib.vt.edu", user="ctr_30", passwd="03_rtc!", db="ctr_30")
  # create a cursor 
  cur = db.cursor()
  # dbtable = "machine_lem"  # Hurricane Isaac tweet collection
  dbtable = "human_lem"  # Hurricane Isaac tweet collection

  for i in [1,2,3,4]:
  # for i in range(1,50):
    print "------------ %d ------------\n" % i
    # query = "update "+ dbtable +" set topics" + str(i) + "= replace(topics" + str(i) +", ',', ' ')"
    # query = "update "+ dbtable +" set topics" + str(i) + "=LOWER(topics" +str(i) + ")"
    # cur.execute(query)
    for j in range(1, 31):
      # query = "select topics" + str(j) + " from "+ dbtable+" where id=" + str(i)
      query = "select topics" + str(j) + " from "+ dbtable+" where participant_id=" + str(i)
      cur.execute(query)
      topics = removeSymbols(cur.fetchone()[0]).lower().split()
      topics_no_stop = [tt for tt in topics if (tt not in ['and', 'the', 'of','no', 'a', 'in', 'by', 'for', 'on']) and (len(tt)>1)]
      noun_lm_list = [lm.lemmatize(x) for x in topics_no_stop]
      verb_lm_list = [lm.lemmatize(y, 'v') for y in noun_lm_list]
      joined = ",".join(sets.Set(verb_lm_list))

      query = "update "+ dbtable +" set topics" + str(j) + "='" +joined+"' where participant_id=" + str(i)
      # query = "update "+ dbtable +" set topics" + str(j) + "='" +joined+"' where id=" + str(i)
      cur.execute(query)

  # path = "C:\\Users\\seungwon\\isaac12_data\\"
  # data_files = [x for x in os.listdir(path)]
  
  # unique_ids = []
  # for file in data_files:
  #   tweet_id,th_num = file[:-5].split('_')
  #   if tweet_id not in unique_ids:
  #     unique_ids = unique_ids.append(tweet_id)
  # for item in unique_ids:
  #   print item
  # done_ids = open("title_content_id_isaac12.txt", "r").read().split()
  
  
  # done_ids = ["84246","47538", "119182", "47263", "50023"]
  # # get a file
  # # for item in data_files[:20]:
  # for item in data_files[57000:]:
  # # for item in ["84246_0.html", "84249_0.html"]:
  #   print "item: %s \n" % item
  #   try:
  #     # print "in try"
  #     # open file, extract content in <title>, <p> tags
  #     fi = open(path+item, "r").read()
  #     tweet_id, resource_order = item[:-5].split('_')	
  #     if fi != "" and (tweet_id not in done_ids):
        
  #       soup = BeautifulSoup(fi)
        
  #       print "\n--------------- Item: %s ---------------------\n" % item
	 #    # print str(soup.title).encode('ascii', 'ignore')
  #       if soup.title != None:
  #         resource_title = soup.title.text.encode('ascii', 'ignore')
  #         title_no_symbol = removeSymbols(" ".join(resource_title.split()))
  #         print title_no_symbol
  #       print "\n-------------------------------------\n"
	 #    # body = soup.body.contents
	 #    # print body
  #       body_li = [] 
  #       para = soup.findAll('p')
  #       for elem in para:
  #         if elem != None:
  #           body_li = body_li + elem.text.encode('ascii', 'ignore').split()
	 #    # print only a selection of words
  #       body_selected = " ".join(body_li[:1000])
  #       body_no_symbol = removeSymbols(body_selected)
  #       print body_no_symbol
		#   # print elem.text.encode('ascii', 'ignore')
  #       print "\n-------------------------------------\n"
	  
	 #    # write to mysqldb
	 #    # write title
  #       query = "update "+ dbtable +" set resource_title_" + resource_order + "='" +title_no_symbol+ "', content_" + resource_order + "='" + body_no_symbol + "' where id=" + tweet_id
  #       cur.execute(query)
	
  #       print "\n\n\n\n"
  #       # print(soup.prettify())
  #   except:
  #     pass
  

if __name__ == "__main__":
  reload(sys)
  sys.setdefaultencoding("utf-8")
  ext_content()
  
