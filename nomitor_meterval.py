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

METER_LABEL = {'cons':'Ev', 'wp':'Ev', 'wtw':'Ev', 'pv':'Eo'} 

VERBOSE = False
collection = 'nomitor' 
collection = 'nomitor' 
ONE_DAY = 86400
today = datetime.now()
out_format = "%Y-%m-%d"
#####################################################################################
def get_mb_data(gw, m, stime, etime, filter, mdata): 
    ALLDIFF = False
    period = 'day'
    key = 'mb'
    labels = METER_LABEL[m].split(',')
  
    data = nomitor_prod.get_prod_data(gw,key, stime,etime,filter,labels,period,ALLDIFF)
    for t in data.keys():
        doc = data[t]
        #print('    ',t.strftime(out_format),doc)
        v = None
        if t in mdata:
            for k in doc:
                v = doc[k]
                mdata[t][k] += v 
        else:
            mdata[t] = doc     
    return mdata
#####################################################################################
def get_sm_data(gw, stime, etime): 
    ALLDIFF = False
    period = 'day'
    key = 'sm'
    filter = None
    labels = None

    data = nomitor_prod.get_prod_data(gw,key, stime,etime,filter,labels,period,ALLDIFF)
    return data
############################################# main function
def main():
  parser = OptionParser(usage="""\
  Query to MongoDB, by default period is day
  ./nomitor_meterval.py -g K208AK0051 -s "2018-04-01 11:00:00"
  ./nomitor_meterval.py -g K208AK0038 -s "2018-04-01 20:00:00" 
""")
  parser.add_option('-g', '--gw', action='store', dest="gw", 
                      help="""gateway""")
  parser.add_option('-s', '--st', action='store', dest="stime",
                      help="""start time (default= first data time)""")
  parser.add_option('-e', '--et', action='store', dest="etime",
                      help="""end time (default= current time)""")
  parser.add_option('-v', '--verbose', action='store_true', dest="verbose", default=False, 
                      help="""verbose actual values""")
  opts, args = parser.parse_args()

  global VERBOSE
  VERBOSE = opts.verbose
  stime = opts.stime
  etime = opts.etime
  gw  = opts.gw
  if gw is None:
      print ('nomitor serial is missing \n')
      parser.print_help()
      sys.exit(1)
  mc_dict = query_mongo.get_mc_dict(gw)
  m2adrs = query_mongo.get_mbus_adrs(mc_dict)
  tfirst = None
  tlast = None
  m2vals = {}
  for m, adrs in m2adrs.iteritems():
      if not m in METER_LABEL:
          continue 
      mdata = {} 
      for ad in adrs:
          filter = 'data.type='+m+',data.adr='+ad
          #print(filter)
          mdata = get_mb_data(gw, m, stime, etime, filter, mdata)
      m2vals[m] = {}
      for t in mdata.keys():
          t2 = t.replace(hour=12,minute=0,second=0,microsecond=0)
          m2vals[m][t2] = mdata[t]
          if tfirst is None or t2 < tfirst:
              tfirst = t2
          if tlast is None or t2 > tlast:
              tlast = t2
  sdata = get_sm_data(gw, stime, etime)
  smdata = {}
  for t in sorted(sdata.keys()):
      t2 = t.replace(hour=12,minute=0,second=0,microsecond=0)
      smdata[t2] = sdata[t]
      if tfirst is None or t2 < tfirst:
          tfirst = t2
      if tlast is None or t2 > tlast:
          tlast = t2
  if not tfirst is None and not tlast is None:
      tlast = tlast.replace(hour=13,minute=0,second=0,microsecond=0)
      t = tfirst
      while t < tlast:
          sm_Ev = 0
          sm_Eo = 0
          cons_Ev = 0
          pv_Eo = 0
          wp_Ev = 0
          wtw_Ev = 0
          if t in smdata:
              if 'o_1_8' in smdata[t]:
                  sm_Ev = smdata[t]['o_1_8']
              if 'o_2_8' in smdata[t]:
                  sm_Eo = smdata[t]['o_2_8']
          if 'cons' in m2vals and t in m2vals['cons']:
              for k in m2vals['cons'][t]:
                  cons_Ev = m2vals['cons'][t][k]
          if 'pv' in m2vals and t in m2vals['pv']:
              for k in m2vals['pv'][t]:
                  pv_Eo = m2vals['pv'][t][k]
          if 'wp' in m2vals and t in m2vals['wp']:
              for k in m2vals['wp'][t]:
                  wp_Ev = m2vals['wp'][t][k]
          if 'wtw' in m2vals and t in m2vals['wtw']:
              for k in m2vals['wtw'][t]:
                  wtw_Ev = m2vals['wtw'][t][k]
          miss_v = (pv_Eo + sm_Ev - sm_Eo) - (cons_Ev + wp_Ev + wtw_Ev)
          print(t.strftime(out_format), '(Pv_Eo {} SM_Ev {} SM_Eo {}), (cons_Ev {} wp_Ev {} wtw_Ev {}) miss_Ev {}'.format(pv_Eo, sm_Ev, sm_Eo,cons_Ev,wp_Ev,wtw_Ev, miss_v)) 
          t = t + timedelta(1)

#############################################################################
if __name__ == "__main__":
    main()
