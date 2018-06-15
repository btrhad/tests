#!/usr/local/bin/python
import os,sys
import boto3
import json
import traceback
import types
import codecs
os.environ['TZ'] = 'Europe/Amsterdam'
aws_access_key_id = 'AKIAIQC2U3SR7X6AL3MA'
aws_secret_access_key = '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'
boto3.setup_default_session(region_name='eu-west-1', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

from datetime import datetime
from optparse import OptionParser
from sensor_pool import SPOOL
SPOOL.init()
s3 = boto3.resource('s3')
VERBOSE = False

# Amsterdam (K208AK0044/K208AK0045/K209AK0028/K209AK0037)
#PROJ="amsterdam"
#GW="K208AK0044"
#GW="K208AK0045"
#GW="K209AK0028"
#GW="K209AK0037"

#PROJ="oss_1"
#GW="K208AK0051"

#PROJ="groningen"
#GW="K208AK0012"
#GW="K208AK0019"
#GW="K208AK0029"

#PROJ="stadskanaal"
#GW="K209AK0019"

"""
  examples:
  show date/time, RT setpoint, WP heatrequest, RT measured:
     ./read_bucket.py -f K208AK0038/mconf/mc_dict.json -p nathan/K208AK0039/2018/04/ | jq '.ts, (.mb[] | select(.data.type | contains("pv")).data.regs[0,3])'
     python read_bucket.py | jq '.ts,.ot.RTset,.io.HREQ,.ot.RTmeas' | awk 'NR%4{printf "%s ",$0;next;}1'
"""

mc_dict = {}

def merge_dicts(*dict_args):
    """
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result

def get_mconf(gw):
    #mc_conf = json.loads(open(GW+'.conf').read())
    mc_conf = json.loads(open(PROJ+'.conf').read())
    return mc_conf

#return pool,mbus,mtype, for example: kwh, pv, 3
def get_mbus_type(ad):
    for k in mc_dict.keys():
        map = mc_dict[k]
        if 'mbus' in map and map['prim'] == ad:
            for k2 in map.keys():
                if k2.endswith('_num'):
                   j = k2.find('_')
                   pool = k2[0:j]
                   return pool, map['mbus'], map['type']
    return None,None,None

################################################################
#objs = mybucket.objects.filter(Prefix='nathan/K208AK0038/2018/03/31/23:49:41.txt')
#objs = mybucket.objects.filter(Prefix='nathan/K208AK0038/2018/03')
#objs = mybucket.objects.filter(Prefix='nathan/K208AK0051/2018/03/31/23:53:01.txt')
#objs = mybucket.objects.filter(Prefix='nathan/K208AK0051/2018/03')
#objs = mybucket.objects.all()

def get_bucket_files(bucket, prefix, mcdict_file):
    global mc_dict
    mybucket = s3.Bucket(bucket)
    with codecs.open(mcdict_file, 'r', encoding='utf-8') as f:
        mc_dict = json.load(f)
    f.close()
    objs = mybucket.objects.filter(Prefix=prefix)
    return objs
################################################################
def parse_bucket_object(obj):
    global mc_dict
    gwkey = obj.key
    serial = gwkey.split('/')[1]
    #mc=get_mconf(serial)
    # setup config
    #mbus = {}
    #for i in range(0,len(mc['mbus'])):
    #  t = { mc['mbus'][i]['adr'] : i}
    #  mbus = merge_dicts(mbus, t)

   # check if mbus 'adr' is configured active:
   # print mc['mbus'][mbus["25"]]['act']
   # get register definitions (adres 25)
   # print mc['mbus'][mbus["25"]]['regs']
   # get register definitions (adres 1)
   # print mc['mbus'][mbus["1"]]['regs'][0]

    date = gwkey.split(serial)[1]
    TS = datetime.strptime(date, "/%Y/%m/%d/%H:%M:%S.txt")

    try:
        j = json.loads(obj.get()['Body'].read())
    except:
        sys.stderr.write('FAIL- jsonloads {}\n'.format(gwkey))
        #traceback.print_exc(file=sys.stdout)
        return {}
    # topic
    top = {}
    try:
       v1 = j['topic']
       mac = v1.split('/')[2]
       # TODO:  mac to serial 
       # print mac
       # gwkey= nathan/K208AK0038/2018/03/15/13:34:07.txt
    except:
       pass

    # serial
    ser = { 'ser' : serial}
    try:
       v1 = j['payload']['NOMitor']['gateway']['serial_html']
       ser = { 'ser' : v1}
    except:
       pass

    # seq
    seq = {}
    try:
       v1 = j['payload']['NOMitor']["@seq"]
       seq = { 'seq' : v1}
    except:
       pass

    # smartmeter
    sm = {}
    try:
       v1 = j['payload']['NOMitor']['SmartmeterData']['DataRecord'][3]['Value']
       v2 = j['payload']['NOMitor']['SmartmeterData']['DataRecord'][4]['Value']
       v3 = j['payload']['NOMitor']['SmartmeterData']['DataRecord'][5]['Value']
       v4 = j['payload']['NOMitor']['SmartmeterData']['DataRecord'][6]['Value']
       # rename to jason compliant key's
       n1 = 'o_'+j['payload']['NOMitor']['SmartmeterData']['DataRecord'][3]['OBIS'].replace('.','_')
       n2 = 'o_'+j['payload']['NOMitor']['SmartmeterData']['DataRecord'][4]['OBIS'].replace('.','_')
       n3 = 'o_'+j['payload']['NOMitor']['SmartmeterData']['DataRecord'][5]['OBIS'].replace('.','_')
       n4 = 'o_'+j['payload']['NOMitor']['SmartmeterData']['DataRecord'][6]['OBIS'].replace('.','_')
       sm = { 'sm' : { n1: v1, n2: v2, n3: v3, n4: v4 }}
    except:
       pass

    # modbus
    mod = {}
    try:
       v1 = j['payload']['NOMitor']['ModBus']['ModBusData']['DataRecord'][1]['Value']
       v2 = j['payload']['NOMitor']['ModBus']['ModBusData']['DataRecord'][2]['Value']
       mod = { 'mod' : { 'Ta': v1, 'Tmax': v2}}
    except:
       pass

    # opentherm
    ot = {}
    try:
       v1 = j['payload']['NOMitor']['OpenthermData']['DataRecord'][0]['Value']
       v2 = j['payload']['NOMitor']['OpenthermData']['DataRecord'][2]['Value']
       ot = { 'ot' : { 'RTset': v1, 'RTmeas': v2}}
    except:
       pass

    # IO
    io = {}
    try:
       v1 = j['payload']['NOMitor']['IO']['DataRecord'][0]['Value']
       v2 = j['payload']['NOMitor']['IO']['DataRecord'][1]['Value']
       v3 = j['payload']['NOMitor']['IO']['DataRecord'][2]['Value']
       v4 = j['payload']['NOMitor']['IO']['DataRecord'][3]['Value']
       v5 = j['payload']['NOMitor']['IO']['DataRecord'][4]['Value']
       v6 = j['payload']['NOMitor']['IO']['DataRecord'][5]['Value']
       v7 = j['payload']['NOMitor']['IO']['DataRecord'][6]['Value']
       v8 = j['payload']['NOMitor']['IO']['DataRecord'][7]['Value']
       n1 = j['payload']['NOMitor']['IO']['DataRecord'][0]['Id']
       n2 = j['payload']['NOMitor']['IO']['DataRecord'][1]['Id']
       if n2 == '24VOK': # rename to json compliant key
          n2 = 'V24OK'
       n3 = j['payload']['NOMitor']['IO']['DataRecord'][2]['Id']
       n4 = j['payload']['NOMitor']['IO']['DataRecord'][3]['Id']
       n5 = j['payload']['NOMitor']['IO']['DataRecord'][4]['Id']
       n6 = j['payload']['NOMitor']['IO']['DataRecord'][5]['Id']
       n7 = j['payload']['NOMitor']['IO']['DataRecord'][6]['Id']
       n8 = j['payload']['NOMitor']['IO']['DataRecord'][7]['Id']

       io = { 'io' : { n1 : v1, n2: v2, n3: v3, n4: v4, n5: v5, n6: v6, n7: v7, n8: v8}}
    except:
       pass

    # mbus
    mbsize = 0
    try:
        mbsize = len( j['payload']['NOMitor']['mbus']['MBusData'])
    except:
        #print ('NO mbus data in {}'.format(gwkey))
        pass
#          #name = mc['mbus'][mbus[ad]]['type']
#          #print mc['mbus'][mbus[ad]]['regs']
#          #print ad, mc['mbus'][mbus[ad]]['regs']
#          # itterate over configured registers
#          ri = 0
#          rl = []
#          for r in mc['mbus'][mbus[ad]]['regs']:
#             prim = int(mc['mbus'][mbus[ad]]['regs'][ri]['prim'])
#             reg = mc['mbus'][mbus[ad]]['regs'][ri]['reg']
#             mul = mc['mbus'][mbus[ad]]['regs'][ri]['mul'] # mul factor for MJ 2 kwh: 0.2777777777
#
#             v1 = j['payload']['NOMitor']['mbus']['MBusData'][i]['DataRecord'][prim]['Value']
#             u1 = j['payload']['NOMitor']['mbus']['MBusData'][i]['DataRecord'][prim]['Unit']
#             ri = ri + 1
#             rl.append({'lbl' : reg, 'val': v1, 'unit': u1, 'mul' : mul})
#
#          idx = '%d' % (i)
#          mbb = {'idx': ad, 'data' : { 'type':name, 'adr': ad, 'regs': rl}}
#          mb1.append(mbb)
    mb = {}
    mb1 = []
    try:
       if mbsize > 0:
         if isinstance(j['payload']['NOMitor']['mbus']['MBusData'], types.ListType):
            for i in range(0,mbsize):
               try:
                  mdata = j['payload']['NOMitor']['mbus']['MBusData'][i]
                  ad = mdata["@address"]
                  pool,mbus,mtype = get_mbus_type(ad)
                  if VERBOSE:
                      print('address {} pool {} mtype {}'.format(ad, pool, mtype))
                  if pool is not None:
                      pool_name = pool+'_pool'
                      mbus_pool = SPOOL.get(pool_name) #heat_pool
                      dict = mbus_pool[mtype]
                      if VERBOSE:
                          print(pool_name, dict)
                      rl = []
                      for name in dict.keys():
                          reg = dict[name]
                          if isinstance(reg, types.ListType) and reg[0] > -1: # reg is array
                              dnum = reg[0]
                              mul  = reg[1]
                              drecs = mdata['DataRecord']
                              if dnum >= len(drecs):
                                  print('FAIL- mbus datarecord adres',ad,'dnum',dnum,'recs',len(drecs)) 
                              v1 = drecs[dnum]['Value']
                              u1 = drecs[dnum]['Unit']
                              rl.append({'lbl' : name, 'val': v1, 'unit': u1, 'mul' : mul})
                      if VERBOSE:
                          print('values', rl)
                      idx = '%d' % (i)
                      mbb = {'idx': ad, 'data' : { 'type':mbus, 'adr': ad, 'regs': rl}}
                      if 'SlaveInformation' in mdata:
                          sinf = mdata['SlaveInformation']
                          mser = ''
                          if 'Manufacturer' in sinf:
                              mser += sinf['Manufacturer']
                          if 'Id' in sinf:
                              mser += sinf['Id']
                          mbb['ser'] = mser
                      mb1.append(mbb)
               except:
                  print('FAIL- mbus payload',gwkey, 'mbsize',mbsize,'i',i)
                  traceback.print_exc(file=sys.stdout)
                  pass
         if isinstance(j['payload']['NOMitor']['mbus']['MBusData'], types.DictType):
           try:
              ad = j['payload']['NOMitor']['mbus']['MBusData']["@address"]
              pool,mbus,mtype = get_mbus_type(ad)
              if VERBOSE:
                  print('address {} pool {} mtype {}'.format(ad, pool, mtype))
              if pool is not None:
                  pool_name = pool+'_pool'
                  mbus_pool = SPOOL.get(pool_name) #heat_pool
                  dict = mbus_pool[mtype]
                  if VERBOSE:
                      print(pool_name, dict)
                  rl = []
                  for name in dict.keys():
                      reg = dict[name]
                      if isinstance(reg, types.ListType) and reg[0] > -1: # reg is array
                          dnum = reg[0]
                          mul  = reg[1]
                          v1 = j['payload']['NOMitor']['mbus']['MBusData']['DataRecord'][dnum]['Value']
                          u1 = j['payload']['NOMitor']['mbus']['MBusData']['DataRecord'][dnum]['Unit']
                          rl.append({'lbl' : name, 'val': v1, 'unit': u1, 'mul' : mul})
                  if VERBOSE:
                      print('values', rl)
                  mbb = {'idx': ad, 'data' : { 'type':mbus, 'adr': ad, 'regs': rl}}
                  if 'SlaveInformation' in j['payload']['NOMitor']['mbus']['MBusData']:
                      sinf = j['payload']['NOMitor']['mbus']['MBusData']['SlaveInformation']
                      mser = ''
                      if 'Manufacturer' in sinf:
                          mser += sinf['Manufacturer']
                      if 'Id' in sinf:
                          mser += sinf['Id']
                      mbb['ser'] = mser
                  mb1.append(mbb)
           except:
              print('FAIL- mbus payload',gwkey)
              traceback.print_exc(file=sys.stdout)
    except:
        print('FAIL- mbus',gwkey)
        traceback.print_exc(file=sys.stdout)
        pass
    mb = { 'mb' : mb1 }

    ts = {'ts' : str(TS)}
    jj = merge_dicts(ts, ser, seq, sm, mod, ot, io, mb)
    return jj
############################################# main function
def main():
  parser = OptionParser(usage="""\
  parse bucket files
  ./read_bucket.py -p nathan/K208AK0051/2018/04 -f K208AK0051/mconf/mc_dict.json
""")
  parser.add_option('-p', '--prefix', action='store', dest="prefix",
                      help="""prefix""")
  parser.add_option('-f', '--file', action='store', dest="file",
                      help="""mc_dict file""")
  parser.add_option('-b', '--bucket', action='store', dest="bucket", default="servicedesk.yirdis.nl",
                      help="""S3 bucket (default= servicedesk.yirdis.nl)""")
  parser.add_option('-v', '--verbose', action='store_true', dest="verbose", default=False, 
                      help="""Verbose debugging""")
  opts, args = parser.parse_args()

  global VERBOSE
  VERBOSE = opts.verbose
  mcdict_file = opts.file
  bucket = opts.bucket
  if mcdict_file is None or not os.path.isfile(mcdict_file):
      print('mc_dict file is missing or "{0}" is not file!'.format(projfile))
      parser.print_help()
      sys.exit(1)
  objs = get_bucket_files(bucket, opts.prefix, mcdict_file) 
  for obj in objs:
      jj = parse_bucket_object(obj)
      print json.dumps(jj)
#########################################################
if __name__ == "__main__":
    main()

