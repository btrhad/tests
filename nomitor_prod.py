#!/usr/local/bin/python

#  ./nomitor_prod.py -g K208AK0051 -s "2018-04-17 11:00:00" -e "2018-04-17 12:30:00" -f "data.type=pv" -l Eo,Ev  --period month
#  ./nomitor_prod.py -g K208AK0051 -s "2018-04-17 11:00:00" -e "2018-04-17 12:30:00" -f "data.type=pv" -l Eo,Ev  --period week 
#  ./nomitor_prod.py -g K208AK0051 -s "2018-04-17 11:00:00" -e "2018-04-17 12:30:00" -f "data.type=wp,data.adr=5" -l Ev
#  ./nomitor_prod.py -g  K208AK0051 -s "2018-03-01 11:00:00" -k sm

import os,sys
import time,json
#os.environ['TZ'] = 'Europe/Amsterdam'

import query_mongo
from optparse import OptionParser
from datetime import datetime
from datetime import timedelta
from dateutil import tz

VERBOSE = False
collection = 'nomitor' 
ONE_DAY = 86400
today = datetime.now()
out_format = "%Y-%m-%d %H:%M"
#########################################
def check_serial(prev_doc, cur_doc, last_doc):
    lic = last_doc.copy()
    cic = cur_doc.copy()
    pic = prev_doc.copy()
    cser = cic.pop('ser', '')
    pser = pic.pop('ser', '')
    vd = {}
    if cser != pser:
        for k in lic.keys():
            if k in pic:
                lv = last_doc[k]
                pv = pic[k]
                vd[k] = lv-pv
                last_doc[k] = pv 
                if VERBOSE:
                    print('ck',cser,pser,k,'lv',lv,'pv',pv)
            if k in cic: 
                last_doc[k] = cic[k] 
    else:
        for k in lic.keys():
            lv = last_doc[k]
            pv = 0
            cv = 0
            if k in pic:
                pv = pic[k]
            if k in cic:
                cv = cic[k]
            if pv > 0 and cv > 0 and pv < cv:
                cvs = str(cv)
                cvs = cvs.replace('.', '')
                if cvs.startswith('99'):
                    vd[k] = lv-pv 
                    last_doc[k] = cic[k] 
                if VERBOSE:
                    print('ck2',k,cvs,'lv',lv,'pv',pv)
    return vd, last_doc                 
#####################################################################################
def compute_mb_prod(data, period='day', ALLDIFF=False):
    START = datetime(today.year, today.month, today.day, tzinfo=tz.tzfile('/usr/share/zoneinfo/Europe/Amsterdam'))
    prod_delta = {} 
    last_doc = None
    last_dt = None
    prev_doc = None

    inum = 0
    gap = ONE_DAY   
    dt = None 
    for doc in data:
        dt = datetime.strptime(doc['ts'], "%Y-%m-%d %H:%M:%S")
        #print('dt ',dt.strftime(out_format))
        dt = dt.replace(tzinfo=tz.tzfile('/usr/share/zoneinfo/Europe/Amsterdam'))
        #print('  [dt] ',dt.strftime(out_format))
        if last_doc is None:
            last_dt = dt
            last_doc = doc['doc'] 
            prev_doc = doc['doc'] 
            diff = (START-dt)
            t = diff.total_seconds()
            if t > 0:
                START = datetime(dt.year, dt.month, dt.day, tzinfo=tz.tzfile('/usr/share/zoneinfo/Europe/Amsterdam')) 
            if period == 'week':
                gap = ONE_DAY*2 
                wday = START.weekday()
                START = START - timedelta(wday)
            elif period == 'month':
                gap = ONE_DAY*7 
                mday = START.day -1
                START = START - timedelta(mday)
            #print('      START',START.strftime(out_format))
        diff = (START-dt)
        t = diff.total_seconds()
        if t > 0 or (ALLDIFF and inum > 0):
            inum += 1
            if t < gap:
                cur_doc = doc['doc'] ########## first doc of the next day 
                vd, ld = check_serial(prev_doc, cur_doc, last_doc)
                if len(vd) > 0:
                    prod_delta[last_dt] = vd
                    last_doc = ld
                prev_doc = cur_doc
            vd = {}
            pvv = {}
            lvv = {} 
            if last_dt in prod_delta: 
                vd = prod_delta[last_dt]
            for k in prev_doc.keys():
                if k == 'ser':
                    continue
                lv = last_doc[k]
                pv = prev_doc[k]
                if VERBOSE:
                    pvv[k] = pv
                    lvv[k] = lv 
                dv = lv-pv
                if k in vd:
                    vd[k] = vd[k]+dv
                else:
                    vd[k] = dv
            prod_delta[last_dt] = vd
            if VERBOSE:
                print(last_dt.strftime(out_format), vd, lvv,'-',pvv)
            START = datetime(dt.year, dt.month, dt.day, tzinfo=tz.tzfile('/usr/share/zoneinfo/Europe/Amsterdam')) 
            if period == 'week':
                wday = START.weekday()
                START = START - timedelta(wday)
            elif period == 'month':
                mday = START.day -1
                START = START - timedelta(mday)
            #print('     START',START.strftime(out_format),dt.strftime(out_format))
            last_doc = doc['doc'] 
            prev_doc = doc['doc'] ########## first doc of the next day 
            #START = START - timedelta(1)
            last_dt = dt
        else:
            cur_doc = doc['doc']
            vd, ld = check_serial(prev_doc, cur_doc, last_doc)
            if len(vd) > 0:
                prod_delta[last_dt] = vd
                last_doc = ld 
            prev_doc = cur_doc
    if dt is not None:
        vd = {}
        pvv = {}
        lvv = {} 
        if last_dt in prod_delta: 
            vd = prod_delta[last_dt]
        for k in prev_doc.keys():
            if k == 'ser':
                continue
            lv = last_doc[k]
            pv = prev_doc[k]
            if VERBOSE:
                pvv[k] = pv
                lvv[k] = lv 
            dv = lv-pv
            if k in vd:
                vd[k] = vd[k]+dv
            else:
                vd[k] = dv
        prod_delta[last_dt] = vd
        if VERBOSE:
            print(last_dt.strftime(out_format), vd, lvv,'-',pvv)
    return prod_delta
#############################################################################################
#####################################################################################
def compute_sm_prod(data, period='day', ALLDIFF=False):
    START = datetime(today.year, today.month, today.day, tzinfo=tz.tzfile('/usr/share/zoneinfo/Europe/Amsterdam'))
    prod_delta = {} 
    last_doc = None
    last_dt = None
    prev_doc = None
   
    gap = ONE_DAY   
    dt = None 
    for doc in data:
        if not 'sm' in doc:
            continue
        dt = datetime.strptime(doc['ts'], "%Y-%m-%d %H:%M:%S")
        dt = dt.replace(tzinfo=tz.tzfile('/usr/share/zoneinfo/Europe/Amsterdam'))
        if last_doc is None:
            last_dt = dt
            last_doc = doc['sm'] 
            prev_doc = doc['sm'] 
            diff = (START-dt)
            t = diff.total_seconds()
            if t > 0:
                START = datetime(dt.year, dt.month, dt.day, tzinfo=tz.tzfile('/usr/share/zoneinfo/Europe/Amsterdam')) 
            if period == 'week':
                gap = ONE_DAY*2 
                wday = START.weekday()
                START = START - timedelta(wday)
            elif period == 'month':
                gap = ONE_DAY*7 
                mday = START.day -1
                START = START - timedelta(mday)
            #print('      START',START.strftime(out_format))
        diff = (START-dt)
        t = diff.total_seconds()
        if t > 0:
            if t < gap:
                prev_doc = doc['sm'] ########## first doc of the next day 
            prod_delta[last_dt] = {} 
            for k in last_doc.keys():
                v2 = float(last_doc[k])
                v1 = v2
                if k in prev_doc.keys():
                    v1 = float(prev_doc[k])
                k2 = k[0:k.rfind('_')]
                diff = v2-v1
                if k2 in prod_delta[last_dt].keys():
                   diff += prod_delta[last_dt][k2]
                prod_delta[last_dt][k2] = diff 
            if VERBOSE:
                print(last_dt.strftime(out_format), json.dumps(prod_delta[last_dt]))
                print('last:{}\nprev {}'.format(json.dumps(last_doc), json.dumps(prev_doc)))
            START = datetime(dt.year, dt.month, dt.day, tzinfo=tz.tzfile('/usr/share/zoneinfo/Europe/Amsterdam')) 
            if period == 'week':
                wday = START.weekday()
                START = START - timedelta(wday)
            elif period == 'month':
                mday = START.day -1
                START = START - timedelta(mday)
            #print('     START',START.strftime(out_format),dt.strftime(out_format))
            last_doc = doc['sm'] 
            prev_doc = doc['sm'] ########## first doc of the next day 
            #START = START - timedelta(1)
            last_dt = dt
        else:
            prev_doc = doc['sm']
    if dt is not None:
        prod_delta[last_dt] = {} 
        for k in last_doc.keys():
            v2 = float(last_doc[k])
            v1 = v2
            if k in prev_doc.keys():
                v1 = float(prev_doc[k])
            k2 = k[0:k.rfind('_')]
            diff = v2-v1
            if k2 in prod_delta[last_dt].keys():
               diff += prod_delta[last_dt][k2]
            prod_delta[last_dt][k2] = diff 
        if VERBOSE:
            print(last_dt.strftime(out_format), json.dumps(prod_delta[last_dt]))
            print('#last:{}\n prev {}'.format(json.dumps(last_doc), json.dumps(prev_doc)))
    return prod_delta 
###################################################################################
def get_prod_data(gw,key, stime,etime,filter,labels,period, ALLDIFF):
    prod_delta = {}
    only_match = True
    limit = 0
    if key == 'mb': 
        value_only = True
        data = query_mongo.get_data(collection,gw,key, stime,etime,filter,only_match,value_only,labels,limit)
        prod_delta = compute_mb_prod(data, period, ALLDIFF)
    elif key == 'sm': 
        value_only = False
        filter = None
        labels = None 
        data = query_mongo.get_data(collection,gw,key, stime,etime,filter,only_match,value_only,labels,limit)
        prod_delta = compute_sm_prod(data, period, ALLDIFF)
    return prod_delta
############################################# main function
def main():
  parser = OptionParser(usage="""\
  Query to MongoDB, by default period is day
  ./nomitor_prod.py -g K208AK0051 -s "2018-04-01 00:00:00" -e "2018-05-23 00:00:00" -f "data.type=pv" -l Eo,Ev  --period month
  ./nomitor_prod.py -g K208AK0051 -s "2018-05-01 00:00:00" -e "2018-05-23 00:00:00" -f "data.type=pv" -l Eo,Ev  --period week 
  ./nomitor_prod.py -g K208AK0051 -s "2018-05-01 00:00:00" -e "2018-05-03 00:00:00" -f "data.type=wp,data.adr=5" -l Ev
  ./nomitor_prod.py -g K208AK0051 -s "2018-03-01 11:00:00" -k sm
  ./nomitor_prod.py -g K208AK0009 -s "2018-03-01 00:00:00" -k sm --period month --sum
""")
  parser.add_option('-k', '--key', action='store', dest="key", default="mb", 
                      help="""default key is 'mb'""")
  parser.add_option('--period', action='store', dest="period", default="day", 
                      help="""period: day OR week OR month (default= day)""")
  parser.add_option('-g', '--gw', action='store', dest="gw", 
                      help="""gateway""")
  parser.add_option('-s', '--st', action='store', dest="stime",
                      help="""start time (default= first data time)""")
  parser.add_option('-e', '--et', action='store', dest="etime",
                      help="""end time (default= current time)""")
  parser.add_option('-f','--filter', action='store', dest="filter",
                      help="""filter: data.type=wp,data.adr=5 """)
  parser.add_option('-l', '--label', action='store', dest="labels", 
                      help="""regs values that you want to see, e,g: Eo, Ev""")
  parser.add_option('-v', '--verbose', action='store_true', dest="verbose", default=False, 
                      help="""verbose actual values""")
  parser.add_option('--sum', action='store_true', dest="sum", default=False, 
                      help="""sum values""")
  parser.add_option('-i', '--include-all', action='store_true', dest="alladres", default=False, 
                      help="""include all addresses""")
  parser.add_option('-a', '--all-diff', action='store_true', dest="alldiff", default=False, 
                      help="""difference between each event/item""")
  opts, args = parser.parse_args()

  global VERBOSE
  VERBOSE = opts.verbose
  ALLDIFF = opts.alldiff
  _labels = opts.labels 
  labels = None 
  sum = opts.sum
  alladres = opts.alladres
  stime = opts.stime
  etime = opts.etime
  filter = opts.filter
  gw  = opts.gw
  if gw is None:
      print ('nomitor serial is missing \n')
      parser.print_help()
      sys.exit(1)
  key = opts.key
  period = opts.period 
  mc_dict = query_mongo.get_mc_dict(gw)
  m2adrs = query_mongo.get_mbus_adrs(mc_dict)
  more_adrs = False
  adrs = []
  if key == 'mb':
      if _labels is None:
          print ('mb labels are missing \n')
          parser.print_help()
          sys.exit(1)
      else:
          labels = _labels.split(',')
      if filter is None:
          print ('mbus filter is missing "data.type=xx" where xx {}\n'.format(json.dumps(m2adrs.keys())))
          parser.print_help()
          sys.exit(1)
      fils = filter.split(',')
      if len(fils) == 1 and filter.lower().startswith('data.type='):
          i = filter.find('=')
          m = filter[i+1:]
          status = query_mongo.check_mb_adrs(m2adrs, m, alladres)
          if len(status) > 1:
              print ('\n{}\n'.format(status))
              sys.exit(1)
          adrs = m2adrs[m]
          if len(adrs) > 1:
              more_adrs = True
  if more_adrs:
      prod_delta = {}
      for ad in adrs:
          filter2 = '{},data.adr={}'.format(filter,ad)
          print(filter2)
          _delta = get_prod_data(gw,key, stime,etime,filter2,labels,period,ALLDIFF) 
          keys = sorted(_delta.keys(), reverse=True)
          for dt in keys:
              print(dt.strftime(out_format), json.dumps(_delta[dt]))
              if dt in prod_delta:
                  v = prod_delta[dt]
                  v1 = _delta[dt]
                  v2 = []
                  for e in v:
                      e2 = e[:]
                      for e1 in v1:
                          if e[0] == e1[0]:
                              e2[1] = e[1]+e1[1]
                      v2.append(e2)
                  for e1 in v1:
                      e2 = e1[:]
                      ins = True 
                      for e in v:
                          if e[0] == e1[0]:
                              ins = False
                              break
                      if ins:
                          v2.append(e2)
                  prod_delta[dt] = v2     
              else:
                  prod_delta[dt] = _delta[dt]
      print('-----')
  else:            
      prod_delta = get_prod_data(gw,key, stime,etime,filter,labels,period,ALLDIFF) 
  if (key == 'mb' or key == 'sm'): 
      sv = None 
      keys = sorted(prod_delta.keys(), reverse=True)
      for dt in keys:
          if not VERBOSE:
              print(dt.strftime(out_format), json.dumps(prod_delta[dt]))
          v = prod_delta[dt]
          if sv is None:
              sv = v 
          else:
              for k in v:
                  if k in sv:
                      sv[k] = sv[k] + v[k]
                  else:
                      sv[k] = v[k]
      print('----------------')
      if sum:
          print(sv)
#############################################################################
if __name__ == "__main__":
    main()
