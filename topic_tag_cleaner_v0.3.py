# Filename: topic_tag_cleaner_v0.3.py
# Description: This script extracts topic tags from MySQL database, and
#              cleans them by lemmatizing, removing stopwords, etc. Then,
#              the tags are put back to the database. 
#              
# shell>python topic_tag_cleaner_v0.3.py
# Name: Seungwon Yang  <seungwon@vt.edu>
# Date: Jun 25, 2013

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
from nltk.stem.wordnet import WordNetLemmatizer

def removeSymbols(word):
  return re.sub(r'[^\w]', ' ', word)

def computeIntersection(a_li, b_li):
    return list(set(a_li) & set(b_li))
  
def ext_content(cur):
  lm = WordNetLemmatizer()
  # dbtable = "machine_lem"  # Hurricane Isaac tweet collection
  dbtable = "nyt_3000"  # Hurricane Isaac tweet collection

  for i in range(1,3001):   # loop through 1-3000 db records
  # for i in range(1,50):
    print "------------ %d ------------\n" % i
    # query = "update "+ dbtable +" set topics" + str(i) + "= replace(topics" + str(i) +", ',', ' ')"
    # query = "update "+ dbtable +" set topics" + str(i) + "=LOWER(topics" +str(i) + ")"
    # cur.execute(query)
    for j in ["opencalais", "organizations", "people", "locations", "descriptors", "names", "nyt_manual_topics"]:
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

def clean_topics(cur, stop_list):
  lm = WordNetLemmatizer()
  # dbtable = "machine_lem"  # Hurricane Isaac tweet collection
  dbtable = "nyt_3000"  # Hurricane Isaac tweet collection
 
  # for item in range(1, 1501):
  for item in range(1, 102):
  # for item in range(1, 2):
    # get the topics from various columns
    # query = "select m_39_topics, m_43_topics, 39_AND_43, 39_OR_43, opencalais, nyt_manual_topics from " + dbtable + " where id='" + str(item) + "'"
    print "--------- Index: %d ---------" % item
    tag_li = ['m_39_bing_100doc_20tag', 'm_39_yahooWeb_100doc_20tag', 'm_39_yahooNews_100doc_20tag', 'm_43_bing_100doc_20tag', 'm_43_yahooWeb_100doc_20tag', 'm_43_yahooNews_100doc_20tag', '39_AND_43_bing_100doc_20tag', '39_AND_43_yahooWeb_100doc_20tag', '39_AND_43_yahooNews_100doc_20tag', '39_OR_43_bing_100doc_20tag', '39_OR_43_yahooWeb_100doc_20tag', '39_OR_43_yahooNews_100doc_20tag']
    for tag in tag_li:
      query = "select " + tag + " from " + dbtable + " where id='" + str(item) + "'"
      cur.execute(query)

      topic_groups = cur.fetchone()
      t_li = topic_groups[0].split(",")
      # print t_li
      
      lem_t = ",".join(list(set([lm.lemmatize(x) for x in [tt for tt in t_li if (tt not in stop_list) and (len(tt) > 2)]])))
      
      query2 = "update "+ dbtable +" set " + tag + "='" + lem_t + "' where id=" + str(item)
      cur.execute(query2)
      # print lem_t


    # print "--------- Index: %d ---------" % item
    # topic_groups = cur.fetchone()
    # first = topic_groups[0].split()
    # second = topic_groups[1].split()
    # third = topic_groups[2].split()
    # fourth = topic_groups[3].split()
    # fifth = " ".join(topic_groups[4].split(";")).split()
    # sixth = " ".join(topic_groups[5].split(";")).split()

    # first_no_stop = [tt for tt in first if (tt not in stop_list) and (len(tt) > 2)]
    # first_lm = [lm.lemmatize(x) for x in first_no_stop]
    # m_39_cln = ",".join(list(set(first_lm)))
    
    # second_no_stop = [tt for tt in second if (tt not in stop_list) and (len(tt) > 2)]
    # second_lm = [lm.lemmatize(x) for x in second_no_stop]
    # m_43_cln = ",".join(list(set(second_lm)))

    # third_no_stop = [tt for tt in third if (tt not in stop_list) and (len(tt) > 2)]
    # third_lm = [lm.lemmatize(x) for x in third_no_stop]
    # AND_cln = ",".join(list(set(third_lm)))

    # fourth_no_stop = [tt for tt in fourth if (tt not in stop_list) and (len(tt) > 2)]
    # fourth_lm = [lm.lemmatize(x) for x in fourth_no_stop]
    # OR_cln = ",".join(list(set(fourth_lm)))

    # fifth_no_stop = [tt for tt in fifth if (tt not in stop_list) and (len(tt) > 2)]
    # fifth_lm = [lm.lemmatize(x) for x in fifth_no_stop]
    # opencalais_cln = ",".join(list(set(fifth_lm)))

    # sixth_no_stop = [tt for tt in sixth if (tt not in stop_list) and (len(tt) > 2)]
    # sixth_lm = [lm.lemmatize(x) for x in sixth_no_stop]
    # nyt_manual_cln = ",".join(list(set(sixth_lm)))

    # # print m_39_cln
    # # print m_43_cln
    # # print AND_cln
    # # print OR_cln
    # # print opencalais_cln
    # # print nyt_manual_cln

    # query2 = "update "+ dbtable +" set m_39_topics_lem='" + m_39_cln + "', m_43_topics_lem='" + m_43_cln + "', 39_AND_43_lem='" + AND_cln + "', 39_OR_43_lem='" + OR_cln + "', opencalais_lem='" + opencalais_cln + "', nyt_manual_topics_lem='" + nyt_manual_cln +"' where id=" + str(item)
    # cur.execute(query2)

def split_underscore(cur):
  lm = WordNetLemmatizer()
  # dbtable = "machine_lem"  # Hurricane Isaac tweet collection
  dbtable = "nyt_3000"  # Hurricane Isaac tweet collection
 
  for item in range(1, 1501):
  # for item in range(1, 20):
    # get the topics from various columns
    # query = "select m_39_topics, m_43_topics, 39_AND_43, 39_OR_43, opencalais, nyt_manual_topics from " + dbtable + " where id='" + str(item) + "'"
    query = "select opencalais_lem from " + dbtable + " where id='" + str(item) + "'"
    cur.execute(query)
 
    print "--------- Index: %d ---------" % item
    topic_groups = cur.fetchone()
    first = topic_groups[0].split(",")
    opencalais_split = []
    for ii in first:
      for jj in ii.split("_"):
        opencalais_split.append(jj)
    opencalais_split = ",".join(list(set(opencalais_split)))
    # print opencalais_split
  
    query2 = "update "+ dbtable +" set opencalais_lem_split='" + opencalais_split + "' where id=" + str(item)
    cur.execute(query2)

def count_topics(cur):
  dbtable = "nyt_3000"  # Hurricane Isaac tweet collection
 
  for item in range(1, 1501):
  # for item in range(1, 20):
    # get the topics from various columns
    query = "select m_39_topics_lem, m_43_topics_lem, 39_AND_43_lem, 39_OR_43_lem, opencalais_lem_split, nyt_manual_topics_lem from " + dbtable + " where id='" + str(item) + "'"
    cur.execute(query)
 
    print "--------- Index: %d ---------" % item
    topic_groups = cur.fetchone()
    first_len = len(topic_groups[0].split(","))
    second_len = len(topic_groups[1].split(","))
    third_len = len(topic_groups[2].split(","))
    fourth_len = len(topic_groups[3].split(","))
    fifth_len = len(topic_groups[4].split(","))
    sixth_len = len(topic_groups[5].split(","))
    
    print first_len
    print second_len
    print third_len
    print fourth_len
    print fifth_len
    print sixth_len

    query2 = "update "+ dbtable +" set num_m_39_topics_lem='" + str(first_len) + "', num_m_43_topics_lem='" + str(second_len) + "', num_39_AND_43_lem='" + str(third_len) + "', num_39_OR_43_lem='" + str(fourth_len) + "', num_opencalais_lem_split='" + str(fifth_len) + "', num_nyt_manual_topics_lem='" + str(sixth_len) +"' where id=" + str(item)
    # query2 = "update "+ dbtable +" set m_39_topics_lem='" + m_39_cln + "', m_43_topics_lem='" + m_43_cln + "', 39_AND_43_lem='" + AND_cln + "', 39_OR_43_lem='" + OR_cln + "', opencalais_lem='" + opencalais_cln + "', nyt_manual_topics_lem='" + nyt_manual_cln +"' where id=" + str(item)
    cur.execute(query2)

def compute_P(cur):
  dbtable = "nyt_3000"  # Hurricane Isaac tweet collection
  # print "---------------------------------------------------"
  # print "   Precision     Recall       F-1 "
  # print "---------------------------------------------------"

  precision_li_li = []
  # for item in range(1, 1501):
  for item in range(1, 102): 
  # for item in range(1, 11):
    # get the topics from various columns
    # query = "select m_39_topics_lem, m_43_topics_lem, 39_AND_43_lem, 39_OR_43_lem, opencalais_lem_split, nyt_manual_topics_lem from " + dbtable + " where id='" + str(item) + "'"

    # make gold standard
    query0 = "select nyt_manual_topics_lem from " + dbtable + " where id='" + str(item) + "'"
    cur.execute(query0)
    gold_standard_li = cur.fetchone()[0].split(",")
    len_gold_standard = len(gold_standard_li)

    # tag_li = ['m_39_bing_100doc_20tag', 'm_39_yahooWeb_100doc_20tag', 'm_39_yahooNews_100doc_20tag', 'm_43_bing_100doc_20tag', 'm_43_yahooWeb_100doc_20tag', 'm_43_yahooNews_100doc_20tag', '39_AND_43_bing_100doc_20tag', '39_AND_43_yahooWeb_100doc_20tag', '39_AND_43_yahooNews_100doc_20tag', '39_OR_43_bing_100doc_20tag', '39_OR_43_yahooWeb_100doc_20tag', '39_OR_43_yahooNews_100doc_20tag', 'opencalais_lem_split']

    tag_li = ['m_39_yahooWeb_100doc_20tag_weighted', 'm_43_yahooWeb_100doc_20tag_weighted', '39_AND_43_yahooWeb_100doc_20tag_weighted', '39_OR_43_yahooWeb_100doc_20tag_weighted', 'm_39_yahooWeb_100doc_20tag_part1', 'm_39_yahooWeb_100doc_20tag_part2', 'm_43_yahooWeb_100doc_20tag_part1', 'm_43_yahooWeb_100doc_20tag_part2', 'opencalais_lem_split']
    
    precision_li = []
    for tag in tag_li:
      query = "select " + tag + " from " + dbtable + " where id='" + str(item) + "'"
      cur.execute(query)
      
      topic_groups = cur.fetchone()
      each_topic_li = topic_groups[0].split(",")
      
      # Precision
      
      intersection = computeIntersection(each_topic_li, gold_standard_li)
      len_each_topic_li = len(each_topic_li)
      len_intersection = len(intersection)
      precision = float(len_intersection) / len_each_topic_li
      precision_li.append(precision)
      # print intersection
      # print precision
      # print precision_li 
      # print "%.3f\t%.3f\t%.3f\t%.3f\t%.3f" % (precision_li[0],precision_li[1],precision_li[2],precision_li[3],precision_li[4])
    precision_li_li.append(precision_li)
  # print "index\t39_bing\t39_yahooweb\t39_yahoonews\t43_bing\t43_yahooweb\t43_yahoonews\tintersection_bing\tintersection_yahooweb\tintersection_yahoonews\tunion_bing\tunion_yahooweb\tunion_yahoonews\topencalais"
  print "index\tm_39_yahooWeb_100doc_20tag_weighted\tm_43_yahooWeb_100doc_20tag_weighted\t39_AND_43_yahooWeb_100doc_20tag_weighted\t39_OR_43_yahooWeb_100doc_20tag_weighted\tm_39_yahooWeb_100doc_20tag_part1\tm_39_yahooWeb_100doc_20tag_part2\tm_43_yahooWeb_100doc_20tag_part1\tm_43_yahooWeb_100doc_20tag_part2\topencalais"
  
  ii = 1
  for pre_li in precision_li_li:
    print "%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f" % (ii, pre_li[0],pre_li[1],pre_li[2],pre_li[3],pre_li[4],pre_li[5],pre_li[6],pre_li[7],pre_li[8])
    ii += 1

def compute_R(cur):
  dbtable = "nyt_3000"  # Hurricane Isaac tweet collection
  # print "---------------------------------------------------"
  # print "   Precision     Recall       F-1 "
  # print "---------------------------------------------------"

  recall_li_li = []
  # for item in range(1, 1501):
  for item in range(1, 102):
  # for item in range(1, 11):
    # get the topics from various columns
    # query = "select m_39_topics_lem, m_43_topics_lem, 39_AND_43_lem, 39_OR_43_lem, opencalais_lem_split, nyt_manual_topics_lem from " + dbtable + " where id='" + str(item) + "'"

    # make gold standard
    query0 = "select nyt_manual_topics_lem from " + dbtable + " where id='" + str(item) + "'"
    cur.execute(query0)
    gold_standard_li = cur.fetchone()[0].split(",")
    len_gold_standard = len(gold_standard_li)

    # tag_li = ['m_39_bing_100doc_20tag', 'm_39_yahooWeb_100doc_20tag', 'm_39_yahooNews_100doc_20tag', 'm_43_bing_100doc_20tag', 'm_43_yahooWeb_100doc_20tag', 'm_43_yahooNews_100doc_20tag', '39_AND_43_bing_100doc_20tag', '39_AND_43_yahooWeb_100doc_20tag', '39_AND_43_yahooNews_100doc_20tag', '39_OR_43_bing_100doc_20tag', '39_OR_43_yahooWeb_100doc_20tag', '39_OR_43_yahooNews_100doc_20tag', 'opencalais_lem_split']

    tag_li = ['m_39_yahooWeb_100doc_20tag_weighted', 'm_43_yahooWeb_100doc_20tag_weighted', '39_AND_43_yahooWeb_100doc_20tag_weighted', '39_OR_43_yahooWeb_100doc_20tag_weighted', 'm_39_yahooWeb_100doc_20tag_part1', 'm_39_yahooWeb_100doc_20tag_part2', 'm_43_yahooWeb_100doc_20tag_part1', 'm_43_yahooWeb_100doc_20tag_part2', 'opencalais_lem_split']
 
    
    recall_li = []
    for tag in tag_li:
      query = "select " + tag + " from " + dbtable + " where id='" + str(item) + "'"
      cur.execute(query)
      
      topic_groups = cur.fetchone()
      each_topic_li = topic_groups[0].split(",")
      
      # Recall
      
      intersection = computeIntersection(each_topic_li, gold_standard_li)
      # len_each_topic_li = len(each_topic_li)
      len_intersection = len(intersection)
      recall = float(len_intersection) / len_gold_standard
      recall_li.append(recall)
      # print intersection
      # print precision
      # print precision_li 
      # print "%.3f\t%.3f\t%.3f\t%.3f\t%.3f" % (precision_li[0],precision_li[1],precision_li[2],precision_li[3],precision_li[4])
    recall_li_li.append(recall_li)
  print "index\tm_39_yahooWeb_100doc_20tag_weighted\tm_43_yahooWeb_100doc_20tag_weighted\t39_AND_43_yahooWeb_100doc_20tag_weighted\t39_OR_43_yahooWeb_100doc_20tag_weighted\tm_39_yahooWeb_100doc_20tag_part1\tm_39_yahooWeb_100doc_20tag_part2\tm_43_yahooWeb_100doc_20tag_part1\tm_43_yahooWeb_100doc_20tag_part2\topencalais"

  ii = 1
  for rec_li in recall_li_li:
    print "%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f" % (ii, rec_li[0],rec_li[1],rec_li[2],rec_li[3],rec_li[4],rec_li[5],rec_li[6],rec_li[7],rec_li[8])
    ii += 1


def compute_F1(cur):
  dbtable = "nyt_3000"  # Hurricane Isaac tweet collection
  # print "---------------------------------------------------"
  # print "   Precision     Recall       F-1 "
  # print "---------------------------------------------------"

  f1_li_li = []
  # for item in range(1, 1501):
  for item in range(1, 102):
  # for item in range(1, 11):
    # get the topics from various columns
    # query = "select m_39_topics_lem, m_43_topics_lem, 39_AND_43_lem, 39_OR_43_lem, opencalais_lem_split, nyt_manual_topics_lem from " + dbtable + " where id='" + str(item) + "'"

    # make gold standard
    query0 = "select nyt_manual_topics_lem from " + dbtable + " where id='" + str(item) + "'"
    cur.execute(query0)
    gold_standard_li = cur.fetchone()[0].split(",")
    len_gold_standard = len(gold_standard_li)

    # tag_li = ['m_39_bing_100doc_20tag', 'm_39_yahooWeb_100doc_20tag', 'm_39_yahooNews_100doc_20tag', 'm_43_bing_100doc_20tag', 'm_43_yahooWeb_100doc_20tag', 'm_43_yahooNews_100doc_20tag', '39_AND_43_bing_100doc_20tag', '39_AND_43_yahooWeb_100doc_20tag', '39_AND_43_yahooNews_100doc_20tag', '39_OR_43_bing_100doc_20tag', '39_OR_43_yahooWeb_100doc_20tag', '39_OR_43_yahooNews_100doc_20tag', 'opencalais_lem_split']
    
    tag_li = ['m_39_yahooWeb_100doc_20tag_weighted', 'm_43_yahooWeb_100doc_20tag_weighted', '39_AND_43_yahooWeb_100doc_20tag_weighted', '39_OR_43_yahooWeb_100doc_20tag_weighted', 'm_39_yahooWeb_100doc_20tag_part1', 'm_39_yahooWeb_100doc_20tag_part2', 'm_43_yahooWeb_100doc_20tag_part1', 'm_43_yahooWeb_100doc_20tag_part2', 'opencalais_lem_split']

    f1_li = []
    for tag in tag_li:
      query = "select " + tag + " from " + dbtable + " where id='" + str(item) + "'"
      cur.execute(query)
      
      topic_groups = cur.fetchone()
      each_topic_li = topic_groups[0].split(",")
      
      # Recall
      
      intersection = computeIntersection(each_topic_li, gold_standard_li)
      len_each_topic_li = len(each_topic_li)
      len_intersection = len(intersection)
      precision = float(len_intersection) / len_each_topic_li
      recall = float(len_intersection) / len_gold_standard

      f1 = 0.0
      denominator = (float(precision) + float(recall))
      if denominator != 0.0:
        f1 = float(2)*precision*recall / (float(precision) + float(recall))

      f1_li.append(f1)

      
      # print intersection
      # print precision
      # print precision_li 
      # print "%.3f\t%.3f\t%.3f\t%.3f\t%.3f" % (precision_li[0],precision_li[1],precision_li[2],precision_li[3],precision_li[4])
    f1_li_li.append(f1_li)
  print "index\tm_39_yahooWeb_100doc_20tag_weighted\tm_43_yahooWeb_100doc_20tag_weighted\t39_AND_43_yahooWeb_100doc_20tag_weighted\t39_OR_43_yahooWeb_100doc_20tag_weighted\tm_39_yahooWeb_100doc_20tag_part1\tm_39_yahooWeb_100doc_20tag_part2\tm_43_yahooWeb_100doc_20tag_part1\tm_43_yahooWeb_100doc_20tag_part2\topencalais"
  ii = 1
  for f1_li in f1_li_li:
    print "%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f" % (ii, f1_li[0],f1_li[1],f1_li[2],f1_li[3],f1_li[4],f1_li[5],f1_li[6],f1_li[7],f1_li[8])
    ii += 1

def compute_ave_num_topics(cur):
  dbtable = "nyt_3000"  # Hurricane Isaac tweet collection
  # print "---------------------------------------------------"
  # print "   Precision     Recall       F-1 "
  # print "---------------------------------------------------"

  topic_count_li_li = []
  for item in range(1, 1501):
  # for item in range(1, 20):
    # get the topics from various columns
    query = "select m_39_topics_lem, m_43_topics_lem, 39_AND_43_lem, 39_OR_43_lem, opencalais_lem_split, nyt_manual_topics_lem from " + dbtable + " where id='" + str(item) + "'"
    cur.execute(query)
 
    # print "--------- Index: %d ---------" % item

    topic_groups = cur.fetchone()
    topic_count_li = []
    for each in topic_groups:
      topic_count_li.append(len(each.split(",")))
    topic_count_li_li.append(topic_count_li)
  
  for jj in topic_count_li_li:
    # print "%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f" % (jj[0],jj[1],jj[2],jj[3],jj[4],jj[5])
    print "%d\t%d\t%d\t%d\t%d\t%d" % (jj[0],jj[1],jj[2],jj[3],jj[4],jj[5])
    # # print topic_li_li

    # # nyt_manual_topics_lem is considered as the 'Gold Standard' answer
    # gold_standard = topic_li_li[5]
    # len_gold_standard = len(gold_standard)
    
    # # Precision
    # f1_li = []
    # # for topic_li in topic_li_li[0:3]:
    # for topic_li in topic_li_li[0:5]:

    #   # opencalais -- gold standard
    #   intersection = computeIntersection(topic_li, gold_standard)
    #   len_topic_li = len(topic_li)
    #   len_intersection = len(intersection)
    #   precision = float(len_intersection) / len_topic_li
    #   recall = float(len_intersection) / len_gold_standard
    #   f1 = 0.0
    #   denominator = (float(precision) + float(recall))
    #   if denominator != 0.0:
    #     f1 = float(2)*precision*recall / (float(precision) + float(recall))

    #   f1_li.append(f1)

    # print "%.3f\t%.3f\t%.3f\t%.3f\t%.3f" % (f1_li[0],f1_li[1],f1_li[2],f1_li[3],f1_li[4])



if __name__ == "__main__":
  # create a stop list -----------------------------//
  stop_li = open("stopwords.txt", "r").read().split()
  custom_stop_li = open("custom_stops.txt", "r").read().split()
  stop_list = list(set(stop_li + custom_stop_li))
  
  # connection to mysqldb --------------------------//
  dbuser = "nyt"
  dbpasswd = "tyn!"
  hostname = "spare05.dlib.vt.edu"
  dbname = "nyt"
  dbtable = "nyt_3000"
  mysqldb = MySQLdb.connect(host=hostname, user=dbuser, passwd=dbpasswd, db=dbname)
  cur = mysqldb.cursor()

  reload(sys)
  sys.setdefaultencoding("utf-8")
  # ext_content(cur)
  # clean_topics(cur, stop_list)
  # split_underscore(cur)
  # count_topics(cur)
  # compute_P(cur)
  # compute_R(cur)
  compute_F1(cur)
  # compute_ave_num_topics(cur)