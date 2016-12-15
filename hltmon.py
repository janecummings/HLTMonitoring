#!/bin/env python
import os,re,sys,subprocess

from optparse import OptionParser
from pyAMI.client import AMIClient
from pyAMI.auth import AMI_CONFIG, create_auth_config
from pyAMI.query import get_configtags
CWD = os.getcwd()
import ROOT
os.chdir(CWD)

if os.getenv('TestArea'):
    webDisplayPath = '%s/DataQuality/DataQualityUtils/scripts/'%(os.getenv('TestArea'))
    if os.path.exists(webDisplayPath): sys.path.append(webDisplayPath)
else:
    print 'Cannot import DQWebDisplay: check that the \'TestArea\' environment variable is set and hosts DataQuality/DataQualityUtils/scripts/'
    sys.exit(0)

from DataQualityUtils.DQWebDisplayConfig import DQWebDisplayConfig 

def genericShellCommand(command):
    result,error=tuple(subprocess.Popen([command], shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE).communicate())
    return result,error

def eosCommand(command):
	command = '/afs/cern.ch/project/eos/installation/0.3.4/bin/eos.select root://eosatlas/ '+command
	return genericShellCommand(command)

def eosList(directory):
	command = 'ls '+directory
	result,error = eosCommand(command)
	if result.startswith('Unable to stat'): return []
	return result.split()

class mon_file:
    def __init__(self,container):
        if container.startswith('/eos/'): 
            self.eos = True
        else: self.eos = False
        self.container=container
        self.dataset=''
        self.project=''
        self.run=''
        self.HIST=''
        self.stream=''
        self.rtag=''
        self.path=''
        self.targetpath=''
        self.webpath=''
        self.targetdir=''
        self.configdirs={}
        self.key_list=[]
       
    def parse_dataset(self):
        self.dataset=self.container.split('/')[-1]
        if self.eos: self.dataset = self.dataset.split('_tid')[0]
        fields = self.dataset.split('.')
        self.project = fields[0]
        self.run = re.sub('^0*','',fields[1])
        self.stream = fields[2]
        self.HIST = fields[4]
        self.rtag = fields[5].split('_p')[0]
        
    def set_paths(self,work_dir):
        target_name = ''
        for field in [self.project,self.run,self.stream+'_'+self.HIST,
                      'merge.HIST',self.rtag]:
            target_name+=field+'.'
        target_name +='root'
        targetdir = work_dir + self.rtag + '/'
        if not os.path.exists(targetdir): os.mkdir(targetdir)
        self.targetname = target_name
        self.targetdir = targetdir
        self.targetpath = targetdir +  target_name
        if self.eos: self.path = self.targetpath
        else: self.path = os.path.realpath(self.container)
    def reset_path(self,path):
        self.path = path
    def set_webpath(self,path):
        self.webpath = path    
    def set_dirs(self,dirs):
        self.configdirs = dirs
    def extend_keys(self,keys):    
        self.key_list.extend(keys)

    def print_info(self):
        print '*'*100
        info = {'eos':self.eos,'dataset':self.dataset,'project':self.project,'run number':self.run, 
                'rtag':self.rtag,'file type':self.HIST,'stream':self.stream,
                'target dir':self.targetdir,'path':self.path,'targetpath':self.targetpath}
        for key in info:
            print key, ':  ',info[key]
    
def compare_files(config,work_dir):
    mon = mon_file(config['file'])
    ref = mon_file(config['ref'])
    #FILE HANDLING
    for f in [mon,ref]: 
        #parse datasetname & set paths
        f.parse_dataset()
        f.set_paths(work_dir)
        #download files from eos
        if f.eos: getMonitoringHistograms(f)
        #check TOP LEVEL compatible with HAN
        check_top_level(f)
        #Read directories 
        get_dir_dict(f,config['dir_config'])
    print '\n  Reprocessing                  | Reference'
    print ' '+'-'*63 
    for ml,rl in [(mon.rtag,ref.rtag),(mon.stream,ref.stream),
                  (mon.HIST,ref.HIST)]:
        print '| ' + ml  + ' '*(30 - len(ml)) +'| ' + rl + ' '*(30 - len(rl)) +'|'
    print ' '+'-'*63
    print ' '*(-5)
    #Name File for Webdisplay
    rtags = mon.rtag + '_' + ref.rtag
    webpath = mon.targetpath.split('/')[-1]
    webpath = webpath.replace(mon.rtag,rtags)
    webpath = mon.targetdir + webpath
    if not os.path.exists(webpath): os.symlink(mon.path,webpath)
    mon.set_webpath(webpath)    
    #WRITE HAN CONFIG
    print ' --> generating han configuration: %strigger_%s.config' % (mon.targetdir,rtags)
    write_han_config(mon,ref,rtags)
    ####################
    #update webdisplay
    ###################
    command = 'han-config-gen.exe {0}trigger_{1}.config'.format(mon.targetdir,rtags)
    print ' --> executing  %s' % (command)
    result,error = genericShellCommand(command)
    ntags = int((rtags).replace('r','').replace('_',''))    
    print ' --> updating web display'
    from DQWebDisplay import DQWebDisplay
    sys.argv.insert(3,ntags)
    if len(sys.argv)<5: sys.argv.extend([1,1])

    c = trig_config(mon.targetdir,rtags)
    DQWebDisplay(webpath,False,c)

    #compare tags 
    streamdir = webpath.split('.')[-5]
    #outputHtmlDir = "/afs/cern.ch/atlas/project/trigger/trigcaf/www/"
    outputHtmlDir = c.htmlDir
    #outputHtmlDir+= "%s/%s/run_%s/" % (ntags,streamdir,mon.run)
    outputHtmlDir+= "/%s/%s/" % (ntags,streamdir)
    g=open(outputHtmlDir+"tag_comp.html","w")
    g.write('<h1> HLT Reprocessing %s / Reference %s </h1>' % (mon.rtag,ref.rtag))
    print ' --> generating tag differences'
    get_tag_diffs(mon,ref,g,opts['mon_task'],opts['ref_task'])
    print ' --> looking for missing histograms'
    compare_keys(mon,ref,g)
    g.close()
    return 1

##################################
### File Handling ################
##################################
def getMonitoringHistograms(mf):
    if os.path.exists(mf.targetpath):
        print ' --> monitoring histogram found: %s' % (mf.targetpath)
        mf.reset_path(mf.targetpath)
        return 0
    mon_files = [mf.container + '/' + file_name  for file_name in eosList(mf.container)]
    if not len(mon_files): 
        print 'No monitoring files found in EOS container %s' % (mf.container)
        sys.exit(0)
    if len(mon_files) == 1: 
        command = 'xrdcp root://eosatlas/%s %s'%(mon_files[0],mf.targetpath)
        result,error = genericShellCommand(command)
        print ' --> copying %s to %s'%(mon_files[0],mf.targetpath)
    elif len(mon_files) > 2:
        for f in mon_files:
            command = 'xrdcp root://eosatlas/%s %s'%(f,mf.targetdir)
            result,error = genericShellCommand(command)
            print '--> copying %s to %s'%(f,mf.targetdir)
        hadd = 'hadd '
        print ' --> merging histograms'
        command = hadd+'-f %s %s*root*'%(mf.targetpath,mf.targetdir)#; rm $(ls * | grep -v histograms.root)'
        result,error = genericShellCommand(command)
        if not os.path.exists(mf.targetpath):
            print 'Failed to merge histograms,:\n%s' % result
            sys.exit(0)
        os.chdir(CWD)
    reset_path(mf.targetpath)
    return 1

def check_top_level(mf):
    mon_file = ROOT.TFile.Open(mf.path)
    top_level = mon_file.GetListOfKeys()
    first = top_level.At(0)
    top = 'run_'+str(mf.run)
    if first.GetName() != top or top_level.After(first):
        add_top_level(mf,top)
    mon_file.Close()
    return top

def add_top_level(mf,top_level):
    if mf.HIST == 'HIST': hist = '.HIST_HLT.'
    else: hist = '.'+mf.HIST+'.'
    targetpath = re.sub('\.HIST\.',hist,mf.targetpath)
    if os.path.exists(targetpath):
        print ' --> reformatted monitoring histogram exists: %s' % targetpath
        mf.reset_path(targetpath)
        return 0
    new_file = ROOT.TFile(targetpath,"recreate")
    print ' --> writing to %s' % (targetpath)
    source = ROOT.TFile.Open(mf.path)
    if not source:
        print 'Could not open input monitoring file'
        sys.exit(0)
    keys = source.GetListOfKeys()
    top_dir = new_file.mkdir(top_level)
    for k in keys:
        cl = k.GetClassName()
        if not cl:continue
        if cl == 'TDirectoryFile':
            copy_dir(k.ReadObj(),top_dir)
    mf.reset_path(targetpath)
    return 1

def copy_dir(source,target):
    sub_dir = target.mkdir(source.GetName())
    keys = source.GetListOfKeys()
    for k in keys:
        cl = k.GetClassName()
        if not cl: continue
        if cl == 'TDirectoryFile':
            copy_dir(k.ReadObj(),sub_dir)
        elif cl == 'TTree':
            sub_dir.cd()
            a_tree = source.Get(k.GetName())
            new_tree = a_tree.CloneTree(-1,'fast')
            new_tree.Write()
        else:
            sub_dir.cd()
            new_tobj = k.ReadObj()
            new_tobj.Write()

##################################
### Read Histograms ##############
##################################
def get_dir_dict(mf,config):
    mon_file = ROOT.TFile.Open(mf.path)
    top = mon_file.GetListOfKeys().At(0)    
    dir_keys = top.ReadObj().GetListOfKeys()
    slices = config.keys()
    config['Lost_and_Found'] = []
    key_list = []
    base = ''
    for key in dir_keys:
        if re.match('^lb|^lowStat',key.GetName()): continue 
        found = False
        for slice in slices:
            if key.GetName() in config[slice]: 
                found = True
                break
        if found == False: config['Lost_and_Found'].append(key.GetName())
    mf.set_dirs(get_dirs(mf,top,{},config,config.keys(),key_list,base,False))
    return 0
    

def get_dirs(mf,top,dirs,config,slices,key_list,base,p):
    dir_keys = top.ReadObj().GetListOfKeys()
    skip = False
    tslice = ''
    for key in dir_keys:
        if re.match('^lb|^lowStat',key.GetName()): continue 
        for slice in slices:
            if key.GetName() in config[slice]: 
                if slice.lower()=='ignore':
                    skip = True
                    break
                if slice not in dirs and not p: 
                    dirs[slice] = {}
                tslice = slice
                break
        if skip:
            skip = False
            continue
        base += key.GetName() + '/'
        if key.GetClassName() in ['TDirectory','TDirectoryFile']:
            if tslice:
                dirs[tslice][key.GetName()] = get_dirs(mf,key,{},config,slices,key_list,base,False)
                base = base[:-len(key.GetName())-1]
            else:
                dirs[key.GetName()] = get_dirs(mf,key,{},config,slices,key_list,base,True)
                base = base[:-len(key.GetName())-1]
        else:
            dirs[key.GetName()] = key.GetClassName()
            key_list.append(base[:-1])
            base = base[:-len(key.GetName())-1]
            tslice=''
    mf.extend_keys(key_list)
    return dirs


#################################
### Han Config ##################
#################################
def write_han_config(mf,rf,rtags):
    #copy template
    han_config = open('{0}trigger_{1}.config'.format(mf.targetdir,rtags), 'w') 
    #han_config = open('trigger_test.config', 'w') 
    copy_template(han_config)
    #write reference
    han_config.write('#'*30 +'\n#Reference\n'+'#'*30+'\n')
    han_config.write('reference basicRef {\n' + '''
    file = {0}
    path = {1}
    name = same_name'''.format(rf.path,'run_'+rf.run)+'\n}\n')
    #write output
    han_config.write('#'*30 +'\n#Output\n'+'#'*30+'\n')
    han_config.write('output top_level {\n  algorithm = WorstCaseSummary\n}\n')
    han_config.write('output top_level {\n')
    for key,value in mf.configdirs.items():
        #if len(mf.mon_dirs) and key not in mf.mon_dirs: continue
        #if re.match('^lb|^lowStat',key): continue ##should be able to drop this
        han_config.write("  output " + str(key) + " {\n")
        write_output_levels(value,han_config,"   ")
        han_config.write("  }\n")
    han_config.write("}\n")
    #write assessments
    han_config.write('#'*30 +'\n#Assessments\n'+'#'*30+'\n')
    for key,value in mf.configdirs.items():
        #if len(mf.mon_dirs) and key not in mf.mon_dirs: continue        
        #if re.match('^lb|^lowStat',key): continue
        #han_config.write("dir " + str(key) + " {\n")
        for k,v in value.items():
            han_config.write("dir " + str(k) + " {\n")
            write_assess_levels(key,v,han_config,"",k)
            han_config.write("}\n")

def copy_template(han_config):
    han_config.write('''
##########################
# HLTReprocessing
##########################

##########################
# Algorithms, Thresholds
##########################

algorithm WorstCaseSummary {
  libname = libdqm_summaries.so
  name = WorstCaseSummary
}

algorithm binCompare {
	libname = libdqm_algorithms.so
	name = BinContentDiff
	reference = basicRef
	NSigma = 0.0
	MaxDiffAbs = 1e-5
	thresholds = binComp_thresholds
        PublishDiff = 1
}

thresholds binComp_thresholds {
	limits NBins {
		warning = 0
		error = 1
	}
}
''')

def write_output_levels(mdir,han_config,level):
    level = level + " "
    for key,value in mdir.items():
        if type(value) == dict: 
            han_config.write(level + "output " + str(key) + " {\n")
            write_output_levels(value,han_config,level)
            han_config.write(level + "}\n")

def write_assess_levels(top,mdir,han_config,level,name):
    level = level + "  "
    for key,value in mdir.items():
        if type(value) == dict:
            name = name + '/%s' % (str(key))
            han_config.write(level + "dir " + str(key) + " {\n")
            
            write_assess_levels(top,value,han_config,level,name)
            name = name[:-1*(1+len(key))]
            han_config.write(level + "}\n")            
        else:
           
            if type(value)==str and re.match('^TH1|^TH2|^TPro',value):
                hist_name = key
            #elif re.match('^TH1|^TH2|^TPro',key.GetClassName()):
            #    hist_name = key.GetName()
            if re.findall('\s',hist_name): continue
            han_config.write(level + "hist " + hist_name + ' {' + '''
            {0} algorithm = binCompare 
            {0} output = {1} 
            {0} weight = 1.0 
            {0} reference = basicRef 
            {0} display = StatBox,NoNorm
            {0} displayResult = Draw=B
            {0} resultName = Reprocessing - Reference \n'''.format(level,top+'/'+name))
            han_config.write(level + "}\n")
    level = level + "   "

def trig_config(hcfg_dir,rtags): 
    dqconfig = DQWebDisplayConfig()
    dqconfig.config         = "Trigger Reprocessing"        
    dqconfig.hcfg           = "{0}trigger_{1}.hcfg".format(hcfg_dir,rtags)
    dqconfig.hanResultsDir  = "/afs/cern.ch/atlas/project/trigger/trigcaf/han_results"
    dqconfig.doHandi        = False
    dqconfig.htmlDir        = "/afs/cern.ch/atlas/project/trigger/trigcaf/www"
    dqconfig.htmlWeb        = "http://atlasdqm.cern.ch/tier0/trigger"
    dqconfig.runlist        = "runlist_trigger.xml"
    dqconfig.indexFile      = "results_trigger.html"
    dqconfig.lockFile       = "DQWebDisplay_trigger.lock"
    #dqconfig.server         = ["voatlas96.cern.ch"]
    return dqconfig

#################################
### WebDisplay Output############
#################################
def get_tag_diffs(mon,ref,g,mon_task,ref_task):

    client = AMIClient()
    if not os.path.exists(AMI_CONFIG):
        create_auth_config()
        client.read_config(AMI_CONFIG)
              
    mon_release=''
    ref_release=''
    mon_taginfo=get_configtags(client,mon.rtag)
    ref_taginfo=get_configtags(client,ref.rtag) 
    configtags = ['SWReleaseCache','lvl1ps','hltps','smk','enlvl1prescales']
    configcomp ={}
    for n,info in enumerate(mon_taginfo):
        ref_info = ref_taginfo[n]
        for xinfo in info.keys():
            if xinfo in configtags:
                if xinfo=='SWReleaseCache': mon_rel=info[xinfo]
                configcomp[xinfo]=[info[xinfo],ref_info[xinfo]]

    for info in ref_taginfo:
        for xinfo in info.keys():
            if xinfo=='SWReleaseCache': ref_rel=info[xinfo]
    mon_release = mon_rel.replace('_',',')
    ref_release = ref_rel.replace('_',',')
    import PyCmt.Cmt as Cmt
    diffs = Cmt.get_tag_diff(ref=ref_release,chk=mon_release,verbose=False)

    g.write('<table>\n')
    g.write('<tr><td width="250"></td><td width="250"><b>Reprocessing</b></td>'
            '<td width="250"><b>Reference</b></tr>')
    ami_link = '<a href ="https://ami.in2p3.fr/AMI/servlet/net.hep.atlas.Database.Bookkeeping.AMI.Servlet.Command?Converter=/AMIXmlToAMIProdHtml.xsl&Command=FormListConfigurationTag+-configTag=%s">%s</a></td>'
    sav_link = '<a href="https://savannah.cern.ch/task/?%s"> Task #%s </a></td>'
    g.write('<tr><td>AMI Tag </td>')
    for tag in (mon.rtag,ref.rtag):
        g.write('<td><a href ="https://ami.in2p3.fr/AMI/servlet/net.hep.atlas.Database.Bookkeeping.AMI.Servlet.Command?Converter=/AMIXmlToAMIProdHtml.xsl&Command=FormListConfigurationTag+-configTag=%s">%s</a></td>' % (tag,tag))
    g.write('</tr>')
    g.write('<tr><td> Savannah Task </td>')
    for task in (mon_task,ref_task):
        if task == None: 
            g.write('<td><a href="https://savannah.cern.ch/task/index.php?go_report=Apply&group=atlas-trig&func=browse&category_id=107&status_id=0"> Search Tasks </a></td>')
        else:
            g.write('<td><a href="https://savannah.cern.ch/task/?%s"> Task #%s </a></td>' % (task,task))
    g.write('</tr>\n')
    g.write('<tr><td> Run </td>')
    for run in (mon.run,ref.run):
        g.write('<td> %s </td>' % str(run))
    g.write('</tr><tr></tr>\n')
    g.write('<tr><td><b>Tag Configuration </b></td></tr>\n')
    for field in configtags:
        g.write('<tr><td>%s</td><td>%s</td><td>%s</td>'%(field,configcomp[field][0],configcomp[field][1]))
        g.write('</tr>\n')
    g.write('<tr></tr>')
    g.write('</table>')
      
    g.write('<h3> Release Tag Differences </h3>')
    g.write('<p> Found [%i] differences </p>\n' % len(diffs))
    
    if len(diffs):
        g.write('<table>\n')
        g.write('<tr><td width = "150"><b>Reprocessing</b></td><td width="250"><b>mon-project</b></td>')
        g.write('<td width = "150"><b>Reference</b></td><td width="250"><b>ref-project</b></td>')
        g.write('<td width = "500"><b>package name</b></td></tr>')
        for diff in diffs:
            g.write('<tr>')
            g.write('<td> %s </td>' % diff['chk'])        
            g.write('<td> %s </td>' % diff['chk_proj'])        
            g.write('<td> %s </td>' % diff['ref'])        
            g.write('<td> %s </td>' % diff['ref_proj'])        
            g.write('<td> %s </td>' % diff['full_name'])        
            g.write('</tr>\n')    
        g.write('</table>')

    return 0

def compare_keys(mf,rf,g):
    mkeys = set(mf.key_list)
    rkeys = set(rf.key_list)
    diffm = list(mkeys - rkeys)
    diffk =  list(rkeys - mkeys)
    g.write('<h3> Missing Histograms </h3>')
    if not len(diffm) and not len(diffk):
        g.write('<p>Found no missing histograms in monitoring/reference files</p>')
        return 0
        
    if len(diffm)<len(diffk):
        g.write('<p>[%i] histograms not in reprocessing file '%len(diffk))
        if len(diffm): 
            g.write('and [%i] histograms not in reference file</p>'%len(diffm))
            g.write('<table><tr><td width = "300">Reference</td><td width="300">Reprocessing</td></tr>')
        else: g.write('</p>\n<table><tr><td width = "300">Reference</td><td width="300"></td></tr>')
        diff1 = diffk
        diff2 = diffm
    else:
        g.write('<p>[%i] histograms not in reference file '%len(diffm))
        if len(diffm): 
            g.write('and [%i] histograms not in reprocessing file</p>'%len(diffk))
            g.write('<table><tr><td width = "300">Reprocessing</td><td width="300">Reference</td></tr>')
        else: g.write('</p>\n<table><tr><td width = "300">Reprocessing</td><td width="300"></td></tr>')
        diff1 = diffm
        diff2 = diffk
    for d in [diff1,diff2]:d.sort()
    row_line = '<tr><td>%s</td><td>%s</td></tr>'
    for l in range(len(diff1)):
        if l<len(diff2):
            g.write(row_line % (diff1[l],diff2[l]))
        else:
            g.write(row_line%(diff1[l],''))
    g.write('</table>')


def usage():
    cmdi = sys.argv[0].rfind("/")
    cmd = sys.argv[0][cmdi+1:]
    
    print '''Usage: %s <config> [<working_dir>] [<
       - file names are either full path to the container holding monitoring root files on EOS or a local root file
       - local files must be named as follows '<project>.<run_number>.<stream_name>.merge.HIST.<ami_tag>.root' 
       - if working_dir is not specified a directory /tmp/%s/monitoring/<rtag> is created to host root files and HAN config files 
''' % (cmd,os.getenv('USER'))
        

if __name__ == '__main__':

    usage = '''usage: %prog hlt.config [Options]
    hlt.config is a local configuration file that specifies file and ref paths and lists monitoring file histogram directories into webdisplay output folders by signature: 
    ref=path/to/reference
    file=/path/to/mon/file
    #Signature1
    dir1
    dir2
    [...]'''
    parser = OptionParser( usage = usage )
    parser.add_option("-W",
                      "--work_dir",
                      dest = 'work_dir',
                      help = "Specify work_dir to save generated monitoring and config files here. Default is /tmp/$USER/monitoring/" )
    parser.add_option("-T",
                      "--task",
                      dest = 'mon_task',
                      help = "Savannah task number for reprocessing request")
    parser.add_option("-R",
                      "--ref_task",
                      dest = 'ref_task',
                      help = "Savannah task number for reprocessing request")
    
    (options,args) = parser.parse_args()
    opts = vars(options)
    if len(args)==0:
        parser.print_help()
        sys.exit(0)

    print '*'*100
    configs = {}
    for i,config in enumerate(args):
        print config
        try:
            output_config = open(config,'r')
        except:
            print 'Could not open input configuration file: %s' % config
            sys.exit(0)
        dirs = {}
        ctype = config.split('.')[0]
        configs[ctype]={}
        for line in output_config:
            line = re.sub('\s','',line)
            if line == '': continue
            if line.startswith('file='): 
               mf=line.split('=')[1]
            elif line.startswith('ref='): 
                rf=line.split('=')[1]
            elif line.startswith('#'):
                sign = line[1:]
                dirs[sign]=[]
            else:
                dirs[sign].append(line)
        configs[ctype]['file']=mf
        configs[ctype]['ref']=rf
        configs[ctype]['dir_config'] = dirs
        print '[%i]%s:'%(i+1,config)
        print '[%i]reprocessing:  %s' % (i+1,mf) 
        print '[%i]reference:     %s' % (i+1,rf)
        
    if opts['work_dir']: work_dir = os.path.realpath(opts['work_dir'])
    else:
        work_dir =  '/tmp/%s/monitoring/'% (os.getenv('USER'))
    if not os.path.exists(work_dir):
        try:
            os.mkdir(work_dir)
        except:
            print 'Could not find work_dir: %s' % work_dir
            sys.exit(0)
    if not work_dir.endswith('/'): work_dir = work_dir + '/'
    print 'work_dir:         %s' % (work_dir)

    
    for i,c in enumerate(configs):
        print '*'*100
        print '[%i]comparing %s files' % (i+1,c.split('.')[0])
        compare_files(configs[c],work_dir)
    
        
