#!/usr/local/bin/python

#  ./nomitor_prod.py -g K208AK0051 -s "2018-04-17 11:00:00" -e "2018-04-17 12:30:00" -f "data.type=pv" -l Eo,Ev  --period month
#  ./nomitor_prod.py -g K208AK0051 -s "2018-04-17 11:00:00" -e "2018-04-17 12:30:00" -f "data.type=pv" -l Eo,Ev  --period week 
#  ./nomitor_prod.py -g K208AK0051 -s "2018-04-17 11:00:00" -e "2018-04-17 12:30:00" -f "data.type=wp,data.adr=5" -l Ev
#  ./nomitor_prod.py -g  K208AK0051 -s "2018-03-01 11:00:00" -k sm

import os,sys
import time,json,codecs
#os.environ['TZ'] = 'Europe/Amsterdam'

import query_mongo
import nomitor_prod
from optparse import OptionParser
from datetime import datetime
from datetime import timedelta
from dateutil import tz

VERBOSE = False
collection = 'nomitor' 
collection = 'nomitor' 
ONE_DAY = 86400
today = datetime.now()
out_format = "%Y-%m-%d %H:%M"
#####################################################################################
def find_max_temp(dhw_delta, mod_data):
    START = datetime(today.year, today.month, today.day, tzinfo=tz.tzfile('/usr/share/zoneinfo/Europe/Amsterdam'))
    day_temp = {}
    last_dt = None

    day_max = 0
    vo_m = 0
    vo_d = 0
    day_count = 0
    keys = sorted(dhw_delta.keys(), reverse=True)
    for dt in keys:
        doc = dhw_delta[dt]
        vo = None 
        for k in doc:
            vo = doc[k]
        if vo is None:
            continue
        if not dt in mod_data:
            continue
        md = mod_data[dt] 
        if VERBOSE:
            print(dt.strftime(out_format), doc, md)
        if last_dt is None:
            last_dt = dt
            diff = (START-dt)
            t = diff.total_seconds()
            if t > 0:
                START = datetime(dt.year, dt.month, dt.day, tzinfo=tz.tzfile('/usr/share/zoneinfo/Europe/Amsterdam'))
        diff = (START-dt)
        t = diff.total_seconds()
        if t > 0:
            if day_count > 0:
                if last_dt not in day_temp:
                    day_temp[last_dt] = {}
                day_temp[last_dt]['temp'] = day_max
                day_temp[last_dt]['vo'] = vo_m 
                if VERBOSE:
                    print(last_dt.strftime(out_format),str(day_max), 'vo {} day_vo{}'.format(vo_m,vo_d))
            START = datetime(dt.year, dt.month, dt.day, tzinfo=tz.tzfile('/usr/share/zoneinfo/Europe/Amsterdam'))
            last_dt = dt
            day_max = 0
            day_count = 0
            vo_m = 0
            vo_d = 0
        if vo > 0:
            vo_d += vo
            day_count += 1
            if VERBOSE:
                print(dt.strftime(out_format), vo, md)
            if md['Ta'] > day_max:
                day_max = md['Ta']
                vo_m = vo 
                last_dt = dt
    if day_count > 0:
        if last_dt not in day_temp:
            day_temp[last_dt] = {}
        day_temp[last_dt]['temp'] = day_max
        day_temp[last_dt]['vo'] = vo_m 
        if VERBOSE:
            print(last_dt.strftime(out_format),str(day_max), 'vo {} day_vo{}'.format(vo_m,vo_d))
    return day_temp
#####################################################################################
def get_dhw_mb_data(gw, stime, etime, filter='data.type=dhw'): 
    ALLDIFF = True
    period = 'day'
    key = 'mb'
    labels = ['Vo']
  
    dhw_data = nomitor_prod.get_prod_data(gw,key, stime,etime,filter,labels,period,ALLDIFF) 
    return dhw_data
#####################################################################################
def get_mod_data(gw, stime, etime): 
    value_only = False
    key = 'mod'
    filter = None
    labels = None
    only_match = True
    limit = 0

    data = query_mongo.get_data(collection,gw,key, stime,etime,filter,only_match,value_only,labels,limit)  
    return data
############################################# main function
def main():
  parser = OptionParser(usage="""\
  Query to MongoDB, by default period is day
  ./nomitor_prodTemp.py -g K208AK0051 -s "2018-04-01 11:00:00" -f "data.type=dhw,data.adr=12"
  ./nomitor_prodTemp.py -g K208AK0038 -s "2018-04-01 20:00:00" 
""")
  parser.add_option('-g', '--gw', action='store', dest="gw", 
                      help="""gateway""")
  parser.add_option('-s', '--st', action='store', dest="stime",
                      help="""start time (default= first data time)""")
  parser.add_option('-e', '--et', action='store', dest="etime",
                      help="""end time (default= current time)""")
  parser.add_option('-f', '--filter', action='store', dest="filter", default="data.type=dhw",
                      help="""filter data.type=dhw, data.adr=2 (default= data.type=dhw)""")
  parser.add_option('-v', '--verbose', action='store_true', dest="verbose", default=False, 
                      help="""verbose actual values""")
  opts, args = parser.parse_args()

  global VERBOSE
  VERBOSE = opts.verbose
  stime = opts.stime
  etime = opts.etime
  filter = opts.filter.strip()
  gw  = opts.gw
  if gw is None:
      print ('nomitor serial is missing \n')
      parser.print_help()
      sys.exit(1)
  mc_dict = query_mongo.get_mc_dict(gw)
  m2adrs = query_mongo.get_mbus_adrs(mc_dict)
  if not 'dhw' in m2adrs:
      print ('\nno "dhw" found in mc_dict {}\n'.format(json.dumps(m2adrs)))
      parser.print_help()
      sys.exit(1)
  fils = filter.split(',')
  if len(fils) == 1 and filter.lower().startswith('data.type='):
      i = filter.find('=')
      m = filter[i+1:]
      status = query_mongo.check_mb_adrs(m2adrs, m)
      if len(status) > 1:
          print ('\n{}\n'.format(status))
          sys.exit(1) 
  prod_delta = get_dhw_mb_data(gw, stime, etime, filter)
  #keys = sorted(prod_delta.keys(), reverse=True)
  #for dt in keys:
  #    print(dt.strftime(out_format), json.dumps(prod_delta[dt]))
  #print('----------------')
  data = get_mod_data(gw, stime, etime)
  mod_data = {}
  for doc in data:
      dt = datetime.strptime(doc['ts'], "%Y-%m-%d %H:%M:%S")
      dt = dt.replace(tzinfo=tz.tzfile('/usr/share/zoneinfo/Europe/Amsterdam'))
      if 'mod' in doc:
          mod_data[dt] = doc['mod']

  day_temp = find_max_temp(prod_delta, mod_data)
  if not VERBOSE and len(day_temp) > 0:
      vos = []
      for t in day_temp.keys():
          v = day_temp[t]
          vos.append(v['vo'])
      vos = sorted(vos)
      vmax = 0
      vh = vos[0]
      try:
          vmax = float(vos[-1])
          for x in vos:
              vh = x 
              y = float(x)
              if y > vmax/2:
                  break
      except:
          pass
      tt = 0
      tc = 0
      for t in sorted(day_temp.keys()):
          v = day_temp[t]
          print(t.strftime(out_format),str(v['temp']), 'vo {}'.format(v['vo']))
          if v['vo'] >= vh:
              tt += float(v['temp'])
              tc += 1
      print('vmax {} vh {} tt {} tc {} => year avg {}'.format(vmax,vh,tt,tc,(tt/tc))) 
           
#############################################################################
if __name__ == "__main__":
    main()
