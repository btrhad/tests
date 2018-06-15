#!/usr/local/bin/python

import os,sys
import getpass
import shutil
from optparse import OptionParser
from datetime import datetime

VERBOSE = False


#SW_IMG="debIoT20180319.img"
# TODO: replace by hubs check for image

############################################# main function
def main():
    parser = OptionParser(usage="""\
    Update NOMitors with new software - through debug channel
    ./upd_hubs.py -l /data2/efs/usr/sftp0/jail/nathan/gw/nathan.log
""")
    parser.add_option('-l', '--log', action='store', dest="file", default="/dev/null",
                      help="""log file""")
    parser.add_option('-v', '--verbose', action='store_true', dest="verbose", default=False,
                      help="""Verbose debugging""")
    opts, args = parser.parse_args()

    VERBOSE = opts.verbose
    log_file = opts.file
    lf = open(log_file, 'a')

    os.chdir('/data2/efs/usr/sftp0/jail/nathan/gw')
    if  getpass.getuser() != 'nathan':
        print 'run script as user nathan!'
        exit(1)
    
    ready = 0
    if os.stat('project.stat').st_size > 0:
        shutil.copyfile('project.stat', 'project.stat2')
    with open('/data2/efs/spool/nathan/project/hubs.json') as pf:
        config_hubs = pf.read()
    
    with open('project.stat2') as f:
      with open('project.stat', 'w') as of:
        for line in f:
            res_line = line.strip()
            items = line.strip().split(" ")
            cnt = len(items)
            if cnt > 1:
                gw=items[0]
                mac=items[1]
                #if cnt == 5:
                #    # check if gw/mac are registered in:  
                #    # data2/efs/spool/nathan/project/hubs.json
                #    print 'check if %s with mac %s is in /data2/efs/spool/nathan/project/hubs.json' % (gw, mac)
                ostr='check if %s with mac %s is in /data2/efs/spool/nathan/project/hubs.json ' % (gw, mac)
                if not gw in config_hubs:
                    print ostr
                    lf.write(ostr+str(datetime.now())+'\n')
            else:
                ostr='empty line in project.stat: original saved in project.stat2! '
                print ostr
                lf.write(ostr+str(datetime.now())+'\n')
             
                of.write(res_line)
                of.write('\n')
                continue
            if cnt > 2:
                pk=items[2]
            else:
                print cnt,items
                print 'no pk: run mk_gw first'
                exit(1)
            if cnt > 3:
                img=items[3]
            else:
                #shutil.copyfile('.images/'+SW_IMG,gw+'/'+SW_IMG)
                #res_line = res_line + ' img'
                #of.write(res_line)
                #of.write('\n')
                ostr='TODO: copy proper image for hub %s ' % (gw)
                print ostr
                lf.write(ostr+str(datetime.now())+'\n')
                continue
            if cnt > 4:
                newimg=items[4]
            else:
                if os.path.exists('./'+gw+'/newimg'):
                    res_line = res_line + ' newimg'
                else:
                    ostr='%s: wait until img has been uploaded ' %gw
                    if VERBOSE:
                        print ostr
                        #lf.write(ostr+'\n')
                of.write(res_line)
                of.write('\n')
                continue
            if cnt > 5:
                update=items[5]
            else:
                # cat contents of newimg to update (strip all after '.')
                if os.path.exists('./'+gw+'/newimg'):
                   with open('./'+gw+'/newimg', 'r') as myimg:
                      upd = myimg.read().split('.')[0]
                      with open(gw+'/update', 'w') as f:
                         f.write(upd+'\n')
                      #shutil.copyfile('update',gw+'/update')
                      ostr='%s: update installed ' % gw
                      if VERBOSE:
                          print ostr
                      lf.write(ostr+str(datetime.now())+'\n')
                      res_line = res_line + ' update'
                      of.write(res_line)
                      of.write('\n')
                      continue
            if cnt > 6:
                if cnt == 7:
                    updated=items[6]
                    pk_file = gw+'/'+gw+'.pk'
                    with open(pk_file) as p:
                        mac = p.readline().strip().split()[1]
                    res_line = res_line + ' *'
                    ostr='gw %s became ready and updated, remove %s from s3 bucket ' %(gw, mac)
                    if VERBOSE:
                        print ostr
                    lf.write(ostr+str(datetime.now())+'\n')
                    ready = ready + 1
            else:
                if os.path.exists('./'+gw+'/updated'):
                    res_line = res_line + ' updated'
                else:
                    ostr='%s: wait until img has been installed ' %(gw)
                    if VERBOSE:
                        print ostr
                    lf.write(ostr+'\n')
                of.write(res_line)
                of.write('\n')
                continue
    
            of.write(res_line)
            of.write('\n')
    
    if ready > 0:
      ostr='%s hubs became ready and updated, remove from s3 bucket ' %(ready)
      if VERBOSE:
          print ostr
      lf.write(ostr+str(datetime.now())+'\n')
    


#############################################################################
if __name__ == "__main__":
    main()

