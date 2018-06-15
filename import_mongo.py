#!/usr/local/bin/python

import os,sys,glob
import time,json,codecs
os.environ['TZ'] = 'Europe/Amsterdam'

from datetime import datetime
from pymongo import MongoClient
from pymongo import ASCENDING, DESCENDING
from optparse import OptionParser
import read_bucket

VERBOSE = False

client = MongoClient() #defaults to the MongoDB instance that runs on the localhost interface on port 27017
#client = MongoClinet("mongodb://mongodb0.example.net:27017")
DB = client['nathan']
collection = None
DIFF_SECONDS = 0 
##################################################
def import_files(bucket, prefix, mcdict_file): 
  objs = read_bucket.get_bucket_files(bucket, prefix, mcdict_file)
  now = datetime.now()
  for obj in objs:
      try:
          ser = obj.key.split('/')[1]
          date= obj.key.split(ser)[1]
          TS = datetime.strptime(date, "/%Y/%m/%d/%H:%M:%S.txt")
          diff = now - TS
          if DIFF_SECONDS > 0 and diff.total_seconds() > DIFF_SECONDS:
              continue 
          jj = read_bucket.parse_bucket_object(obj)
          if 'ts' in jj and 'ser' in jj:
              collection.replace_one({"ts": jj['ts'], "ser" : jj['ser']}, jj, upsert= True)
      except:
          pass 
############################################# main function
def main():
  parser = OptionParser(usage="""\
  Import Data to MongoDB
  call read_bucket.py
  ./import_mongo.py -p nathan/K208AK0051/2018/04 -f K208AK0051/mconf/mc_dict.json

  ./import_mongo.py -p 2018/04 -d /data2/efs/usr/sftp0/jail/nathan/gw
        dir contains <nomitor>/mconf/mc_dict.json file, then bucket prefix is nathan/<nomitor>/.... 
""")
  parser.add_option('-p', '--prefix', action='store', dest="prefix",
                      help="""prefix""")
  parser.add_option('-f', '--file', action='store', dest="file",
                      help="""mc_dict file""")
  parser.add_option('-b', '--bucket', action='store', dest="bucket", default="servicedesk.yirdis.nl",
                      help="""S3 bucket (default= servicedesk.yirdis.nl)""")
  parser.add_option('-c', '--collection', action='store', dest="collection", default="nomitor",
                      help="""collection name (default= nomitor)""")
  parser.add_option('-d', '--dir', action='store', dest="dir", 
                      help="""directory has <nomitor>/mconf/mc_dict.json file""")
  parser.add_option('--exclude', action='store', dest="secs", default="0", 
                      help="""exclude files which is older than given seconds (default=0, include all)""")
  parser.add_option('-v', '--verbose', action='store_true', dest="verbose", default=False,
                      help="""Verbose debugging""")
  opts, args = parser.parse_args()

  global DIFF_SECONDS 
  global VERBOSE
  global collection 
  VERBOSE = opts.verbose
  DIFF_SECONDS = int(opts.secs)
  bucket = opts.bucket
  mcdict_file = opts.file
  prefix = opts.prefix
  
  if opts.dir is None and (mcdict_file is None or not os.path.isfile(mcdict_file)):
      print('mc_dict file is missing or "{0}" is not file!'.format(projfile))
      parser.print_help()
      sys.exit(1)
  
  collection = DB[opts.collection]
  collection.create_index([("ts", DESCENDING), ("ser", ASCENDING)], background=True, sparse=True)

  if opts.file:
      import_files(bucket, prefix, mcdict_file)
  else:
      nom_conf = {}
      conf_time = {}
      hub_file = '/data2/efs/spool/nathan/project/hubs.json'
      if os.path.isfile(hub_file):
          with codecs.open(hub_file, 'r', encoding='utf-8') as f:
              hub_dict = json.load(f)
          f.close()
          if "projects" in hub_dict:
              for proj in hub_dict["projects"]:
                  gconf = None
                  gtime = 0
                  if "mc_conf" in proj and os.path.isfile(proj["mc_conf"]):
                      gconf = proj["mc_conf"]
                      gtime = os.path.getmtime(gconf)
                  if "hubs" in proj:
                      for hub in proj["hubs"]:
                        for nom in hub.keys():
                          if "mc_conf" in hub[nom]:
                              if os.path.isfile(hub[nom]["mc_conf"]):
                                mconf = hub[nom]["mc_conf"]
                                nom_conf[nom] = mconf
                                conf_time[nom] = os.path.getmtime(mconf)
                          elif gconf is not None:
                              nom_conf[nom] = gconf
                              conf_time[nom] = gtime
      conf_dict = {}
      conf_time_file = os.path.join(opts.dir,'conf','mc_dict_time.json')
      if os.path.isfile(conf_time_file):
        try:
          with codecs.open(conf_time_file, 'r', encoding='utf-8') as f:
              conf_dict = json.load(f)
          f.close()
        except:
          pass
      fw = open(conf_time_file,'w')
      json.dump(conf_time, fw, sort_keys=True, indent=3)
      fw.close()
      parts = prefix.split('/') 
      for g in sorted(nom_conf.keys()):
         gprefix = os.path.join('nathan',g,prefix)
         ctime = conf_time[g]
         dtime = 0
         if g in conf_dict:
             dtime = conf_dict[g]
         if dtime != ctime:
             gprefix = os.path.join('nathan',g,parts[0])
         mcdict_file = nom_conf[g]
         if VERBOSE: 
             print(gprefix,mcdict_file) 
         import_files(bucket, gprefix, mcdict_file)
 
#############################################################################
if __name__ == "__main__":
    main()
