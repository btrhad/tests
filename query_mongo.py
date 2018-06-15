#!/usr/local/bin/python

import os,sys
import time,json,codecs
from pymongo import MongoClient
from pymongo import ASCENDING, DESCENDING
from optparse import OptionParser

client = MongoClient() #defaults to the MongoDB instance that runs on the localhost interface on port 27017
#client = MongoClinet("mongodb://mongodb0.example.net:27017")
DB = client['nathan']
###############################################################
def get_mc_dict(gw):
    mc_dict = {} 
    hub_file = '/data2/efs/spool/nathan/project/hubs.json'
    if os.path.isfile(hub_file):
        nom_conf = {}
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
                        elif gconf is not None:
                            nom_conf[nom] = gconf
        if gw in nom_conf:
            mcdict_file = nom_conf[gw]
            with codecs.open(mcdict_file, 'r', encoding='utf-8') as f:
                mc_dict = json.load(f)
            f.close()
    return mc_dict
###########################################################
def get_mbus_adrs(mc_dict):
    m2adrs = {}
    for k in mc_dict.keys():
        map = mc_dict[k]
        if 'mbus' in map and map['act'] == '1':
            m = map['mbus']
            if not m in m2adrs:
                m2adrs[m] = []
            adr = map['prim']
            m2adrs[m].append(adr)
    return m2adrs
###############################################################
def check_mb_adrs(m2adrs,m, alladres=False):
    status = ''
    if m not in m2adrs:
        status = 'data type "{}" is not in {} '.format(m,json.dumps(m2adrs.keys()))
    elif not alladres:
        adrs = m2adrs[m]
        if len(adrs) > 1:
            status = 'specify {} by adding adr in filter -f "data.type={},data.adr={}" adrs: {})'.format(m,m,adrs[0],json.dumps(adrs))
    return status
################################################################
def get_data(collname,gw,key, stime,etime,filter,only_match,value_only,labels,limit): 
    #print('gw {} key {} filter {} onlym {} vo {} labels {}'.format(gw,key, filter,only_match,value_only,labels))
    collection = DB[collname]
    query = {}
  
    if gw:
        query["ser"] = gw
    if stime or etime:
        query["ts"] = {}
    if stime:
        query["ts"]["$gte"] = stime 
    if etime:
        query["ts"]["$lte"] = etime 
    proj = [key]
    pend = '' 
    if filter:
        query[key] = {}
        if key == "mb":
            pend = '.$' 
            query[key]["$elemMatch"]={}
        else:
            proj.append(filter)
        parts = filter.split(",")
        i = 0
        for part in parts:
            fils = part.split("=")
            query[key]["$elemMatch"][fils[0]]=fils[1]
            if i ==0:
                proj.append(fils[0])
            i += 1
    projkey = '.'.join(proj) +pend
    #print(projkey,"query", query)
    projection = {'_id':False} 
    if only_match:
        projection['ts'] = True 
        projection[projkey] = True 
    docs = collection.find(query,projection=projection, sort=[("ts",DESCENDING)], limit=limit)
    #print('docs count',docs.count())

    data = [] 
    for doc in docs:
        if labels:
            regvals = [] 
            if value_only:
                regvals = {'ser':''} 
            ################## isinstance(doc[key], dict) 
            ################## isinstance(doc[key],list)) 
            mser = ''
            for lbl in labels:
              for elem in doc[key]:
                if 'ser' in elem:
                   mser = elem['ser']
                   if value_only:
                       regvals['ser'] = mser
                for reg in elem['data']['regs']:
                   if reg['lbl'] == lbl:
                       try:
                           v = reg['val']
                           #if pmc == mser and pval.startswith('99') and v < pval:
                           #    v = '1'+v
                           val = float(v)
                           mul = float(reg['mul'])
                           reg['v'] = val*mul 
                           reg['ser'] = mser
                       except:
                           pass
                       if value_only:
                           regvals[lbl] = reg['v']
                       else: 
                           regvals.append(reg)
            out = {'ts':doc['ts'], 'doc' : regvals}
            data.append(out)
        else:
            data.append(doc)
    return data
############################################# main function
def main():
  parser = OptionParser(usage="""\
  Query to MongoDB, 
  ./query_mongo.py -g K208AK0051 -s "2018-04-15 03:22:43" -e "2018-04-15 04:37:49" -k mb -f "data.adr=6" 
  ./query_mongo.py -g K208AK0051 -s "2018-04-15 03:22:43" -e "2018-04-15 04:37:49" -k mb -f "data.type=heat" -o 
  ./query_mongo.py -g K208AK0051 -s "2018-04-15 03:22:43" -e "2018-04-15 04:37:49" -k mb -f "data.type=heat" -l Eo,Ev 
  ./query_mongo.py -g K208AK0051 -s "2018-04-17 11:00:00" -e "2018-04-17 12:30:00" -k mb -f "data.type=wp,data.adr=5" -l Ev -v
""")
  parser.add_option('-g', '--gw', action='store', dest="gw", 
                      help="""gateway""")
  parser.add_option('-s', '--st', action='store', dest="stime",
                      help="""start time""")
  parser.add_option('-e', '--et', action='store', dest="etime",
                      help="""end time""")
  parser.add_option('-k','--key', action='store', dest="key",
                      help="""key, for example: mb io sm mod""")
  parser.add_option('-f','--filter', action='store', dest="filter",
                      help="""filter: data.type=wp,data.adr=5""")
  parser.add_option('-o', '--only', action='store_true', dest="only", default=False,
                      help="""Only matching output""")
  parser.add_option('--limit', action='store', dest="limit",
                      help="""limit""")
  parser.add_option('-c', '--collection', action='store', dest="collection", default="nomitor",
                      help="""collection name (default= nomitor)""")
  parser.add_option('-l', '--label', action='store', dest="labels", 
                      help="""regs values that you want to see, e,g: Eo, Ev""")
  parser.add_option('-v', '--value-only', action='store_true', dest="value_only", default=False,
                      help="""Only values output""")
  opts, args = parser.parse_args()

  gw  = opts.gw
  key = opts.key
  if gw is None:
      print ('nomitor serial is missing \n')
      parser.print_help()
      sys.exit(1)
  if key is None:
      print ('key (mb, io, sm, mod) is missing \n')
      parser.print_help()
      sys.exit(1)
  stime = opts.stime
  etime = opts.etime
  filter = opts.filter
  only_match = opts.only
  if opts.labels:
      only_match = True
  limit = 0
  if opts.limit:
      try:
        limit = int(opts.limit)
      except:
        pass
  labels = None
  if opts.labels:
      labels = opts.labels.split(',')

  if key == 'mb' and filter is not None:
      fils = filter.split(',')
      if len(fils) == 1 and filter.lower().startswith('data.type='):
          i = filter.find('=')
          m = filter[i+1:]
          mc_dict = get_mc_dict(gw)
          m2adrs = get_mbus_adrs(mc_dict)
          status = check_mb_adrs(m2adrs, m)
          if len(status) > 1:
              print ('\n{}\n'.format(status))
              sys.exit(1)
  
  data = get_data(opts.collection,gw,key, stime,etime,filter,only_match,opts.value_only,labels,limit)

  for d in data: 
      print(json.dumps(d))

#############################################################################
if __name__ == "__main__":
    main()
