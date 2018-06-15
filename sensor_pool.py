import os
import sys
import json
import logging
import traceback
import codecs
import shutil


class SPOOL(object):
  current_dir = os.path.dirname(os.path.abspath(__file__))
  conf_file = os.path.join(current_dir, 'conf', 'sensor_pool.json')
  spool_conf = {}

  conf_time = 0
  ######################################################################
  @classmethod
  def init(cls):
      cls.reload()
  ######################################################################
  @classmethod
  def get(cls, key):
      try:
          return cls.spool_conf[key]
      except:
          pass
      return None
  #########################################################
  @classmethod
  def reload(cls):
      ftime = os.path.getmtime(cls.conf_file)
      if cls.conf_time == 0 or ftime > cls.conf_time:
          #print('reload conf - sensor pool')
          cls.conf_time = ftime
          conf_json = {}
          with codecs.open(SPOOL.conf_file, 'r', encoding='utf-8') as f:
             conf_json = json.load(f)
          f.close()

          keys = conf_json.keys()
          cls.spool_conf = {}
          for key in keys:
              if key.endswith('_pool'):
                  pkeys = conf_json[key].keys()
                  cls.spool_conf[key] = {}
                  for pkey in pkeys:
                      file = os.path.join(SPOOL.current_dir, conf_json[key][pkey]['file'])
                      if os.path.isfile(file):
                          with codecs.open(file, 'r', encoding='utf-8') as f:
                              cls.spool_conf[key][pkey] = json.load(f)
                          f.close()
                      else:
                          SPOOL.popkey(cls.spool_conf[key], pkey)
  #########################################################
  @classmethod
  def popkey(cls,dict, key):
      val = None
      try:
          val = dict.pop(key)
      except:
          val = None
      return val
###########################################################
def main():
  print(SPOOL.conf_file)
  print('conf time0', SPOOL.conf_time)
  SPOOL.init()
  heat_pool = SPOOL.get('heat_pool')
  print('heat_pool', heat_pool)
  print('##############')

if __name__ == "__main__":
    main()

