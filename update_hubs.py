import os
import codecs
import json

def get_mc_dict():
    hub_file = '/data2/efs/spool/nathan/project/hubs.json'
    if os.path.isfile(hub_file):
        with codecs.open(hub_file, 'r', encoding='utf-8') as f:
            hub_dict = json.load(f)
        f.close()
        if "projects" in hub_dict:
            for proj in hub_dict["projects"]:
                #print "Projects:", proj
                gconf = None
                #if "mc_conf" in proj and os.path.isfile(proj["mc_conf"]):
                #    gconf = proj["mc_conf"]
                if "hubs" in proj:
                    for hub in proj["hubs"]:
                      #print "hub:", hub
                      for nom in hub.keys():
                        try:
                            pk=nom+'/'+nom+'.pk'
                            with open(pk, 'r') as pkf:
                                mac = pkf.read().strip('\n').split(' ')[1]
                                hub[nom]["mac"]=mac
                            print "nom:", nom,hub[nom]["mac"],mac
                        except:
                            pass

        with open('henk.json', 'w') as f:
            json.dump(hub_dict,f,indent=4)


get_mc_dict()
