
from JobSpecs import JobSpecs
from ClaraErrors import ClaraErrors
from JobErrors import SlurmErrors

_COLORS =[1,  2,    4,    3,    94,  51]

_THREADS=[12, 20,   16,   24,   32]
_FILLS  =[0,  3007, 3003, 3004, 3005]

class ClaraStats:

  def __init__(self):
    # importing ROOT is slow, wait until necessary
    self.ROOT=__import__('ROOT')
    #import ROOTConfig
    self.template=self.ROOT.TH1F('h',';Average Event Time per Core (ms)',100,0,2500)
    self.template.GetYaxis().SetTickLength(0)
    self.template.GetXaxis().CenterTitle()
    self.histos={}
    self.incomplete=0
    self.successes=0
    self.ntuple=None
    self.canvas=None
    self.text=None
    self.title=None
    self.errors={}
    self.slurmerrors={}
    self.flavors={}
    self.flavorlist=JobSpecs._FLAVORS
    self.expectedfiles=[]
    self.foundfiles=[]
    for x in ClaraErrors._BITS:
      self.errors[x]=0
    for x in SlurmErrors._BITS:
      self.slurmerrors[x]=0
    for x in JobSpecs._FLAVORS:
      self.flavors[x]={'success':0,'fail':0,'total':0}

  def setFlavors(self,flavors):
    self.flavorlist=flavors

  # Set a list of histograms to all have the same maximum
  def setMaxima(self,histos):
    maximum=0
    for h in histos:
      if h.GetBinContent(h.GetMaximumBin())>maximum:
        maximum=h.GetBinContent(h.GetMaximumBin())
    for h in histos:
      h.SetMaximum(maximum*1.1)

  def __str__(self):
    ret=''
    ret+='\nTotal Jobs: '+str(self.incomplete+self.successes)+'\n'
    ret+='\nClara Fail:\n'
    fmt='  %7s : %10d : %4.1f%%\n'
    for b in ClaraErrors._BITS:
      if self.incomplete+self.successes>0:
        ret+=fmt%(b,self.errors[b],100*float(self.errors[b])/(self.incomplete+self.successes))
      else:
        ret+=fmt%(b,self.errors[b],0)
    ret+='\nSlurm Fail:\n'
    for b in SlurmErrors._BITS:
      if self.incomplete+self.successes>0:
        ret+=fmt%(b,self.slurmerrors[b],100*float(self.slurmerrors[b])/(self.incomplete+self.successes))
      else:
        ret+=fmt%(b,self.slurmerrors[b],0)
    ret+='\nFlavor Fail:\n'
    for f in JobSpecs._FLAVORS:
      ret+=f+' '
      if self.flavors[f]['total']>0:
        ret+='%7.2f%%'%(100*float(self.flavors[f]['fail'])/(self.flavors[f]['fail']+self.flavors[f]['success']))
        ret+='  (%d/%d)'%(self.flavors[f]['fail'],(self.flavors[f]['fail']+self.flavors[f]['success']))
        good=0
        if f in self.histos:
          for threads in sorted(self.histos[f].keys()):
            good+=self.histos[f][threads].GetEntries()
        ret+='  (%d)'%good
      else:
        ret+='N/A'
      ret+='\n'
    return ret

  def fill(self,jl,val):
    if not jl.flavor in self.flavorlist:
      return
    if jl.flavor in JobSpecs._FLAVORS:
      self.flavors[jl.flavor]['total']+=1
      if jl.isComplete():
      #if jl.slurmerrors.bits==0:
        self.flavors[jl.flavor]['success']+=1
      else:
        self.flavors[jl.flavor]['fail']+=1
    for x in ClaraErrors._BITS:
      if jl.errors.getBit(x):
        self.errors[x]+=1
    for x in SlurmErrors._BITS:
      if jl.slurmerrors.getBit(x):
        self.slurmerrors[x]+=1
    if jl.threads is not None and jl.flavor is not None:
      if self.ntuple is None:
        self.ntuple=self.ROOT.TNtuple("claraStats","","threads:files:events:etime1:etime2:flavor:filesize:s_errors:c_errors:c_walltime")
      self.ntuple.Fill(jl.threads,jl.nfiles,jl.events,jl.t1,jl.t2,JobSpecs._FLAVORS.index(jl.flavor),jl.filesize,jl.errors.bits,jl.slurmerrors.bits,0)
    self.expectedfiles.extend(jl.inputfiles)
    self.foundfiles.extend(jl.findOutputFiles())
    if jl.isComplete():
      if not jl.flavor in self.histos:
        self.histos[jl.flavor]={}
      if not jl.threads in self.histos[jl.flavor]:
        name='%sx%s'%(jl.flavor,jl.threads)
        color=_COLORS[JobSpecs._FLAVORS.index(jl.flavor)]
        fill=_FILLS[_THREADS.index(jl.threads)]
        self.histos[jl.flavor][jl.threads]=self.template.Clone(name)
        self.histos[jl.flavor][jl.threads].SetTitle(name)
        self.histos[jl.flavor][jl.threads].SetLineColor(color)
        #self.histos[flavor][threads].SetLineStyle(style)
        if fill>0:
          self.histos[jl.flavor][jl.threads].SetFillStyle(fill)
          self.histos[jl.flavor][jl.threads].SetFillColor(color)
      self.histos[jl.flavor][jl.threads].Fill(val)
      self.successes+=1
    else:
      self.incomplete+=1

  def save(self,prefix):
    f=self.ROOT.TFile(prefix+'.root','RECREATE')
    f.cd()
    if self.canvas is not None:
      self.canvas.Write()
      self.canvas.SaveAs(prefix+'.png')
    if self.ntuple is not None:
      self.ntuple.Write()
    for x in list(self.histos.values()):
      for y in list(x.values()):
        y.Write()
    f.Close()

  def draw(self):
    if self.successes<=0:
      return
    self.ROOT.gStyle.SetOptTitle(0)
    self.ROOT.gStyle.SetOptStat(0)
    if self.canvas is None:
      t=self.title
      if t is None: t='canvas'
      # copied from ~/.rootlogon.C, to be cleaned up:
      self.ROOT.gStyle.SetCanvasColor(0)
      self.ROOT.gStyle.SetPadColor(0)
      self.ROOT.gStyle.SetTitleFillColor(0)
      self.ROOT.gStyle.SetTitleBorderSize(0)
      self.ROOT.gStyle.SetFrameBorderMode(0)
      self.ROOT.gStyle.SetPaintTextFormat(".0f")
      self.ROOT.gStyle.SetLegendBorderSize(1)
      self.ROOT.gStyle.SetLegendFillColor(self.ROOT.kWhite)
      self.ROOT.gStyle.SetTitleFontSize(0.04)
      self.ROOT.gStyle.SetPadTopMargin(0.05)
      self.ROOT.gStyle.SetPadLeftMargin(0.11)
      self.ROOT.gStyle.SetPadBottomMargin(0.12)
      self.ROOT.gStyle.SetTitleXSize(0.05)
      self.ROOT.gStyle.SetTitleYSize(0.05)
      self.ROOT.gStyle.SetTextFont(42)
      self.ROOT.gStyle.SetStatFont(42)
      self.ROOT.gStyle.SetLabelFont(42,"x")
      self.ROOT.gStyle.SetLabelFont(42,"y")
      self.ROOT.gStyle.SetLabelFont(42,"z")
      self.ROOT.gStyle.SetTitleFont(42,"x")
      self.ROOT.gStyle.SetTitleFont(42,"y")
      self.ROOT.gStyle.SetTitleFont(42,"z")
      self.ROOT.gStyle.SetHistLineWidth(2)
      self.ROOT.gStyle.SetGridColor(15)
      self.ROOT.gStyle.SetPadGridX(1)
      self.ROOT.gStyle.SetPadGridY(1)
      self.ROOT.gStyle.SetHistMinimumZero(self.ROOT.kTRUE)
      self.ROOT.gROOT.ForceStyle()
      self.canvas=self.ROOT.TCanvas('c',t,700,500)
      self.canvas.GetPad(0).SetRightMargin(0.18)
      self.canvas.GetPad(0).SetLeftMargin(0.2)
    if self.text is None:
      self.text=self.ROOT.TText()
      self.text.SetTextSize(0.03)
    histos=[]
    for flavor in sorted(self.histos.keys()):
      for threads in sorted(self.histos[flavor].keys()):
        histos.append(self.histos[flavor][threads])
    self.setMaxima(histos)
    opt='H'
    for h in histos:
      h.Draw(opt)
      opt='SAMEH'
    toterrors,totslurmerrors=0,0
    for x in list(self.errors.keys()):
      toterrors+=self.errors[x]
    for x in list(self.slurmerrors.keys()):
      totslurmerrors+=self.slurmerrors[x]
    tot=self.successes+self.incomplete
    #tot=self.successes+toterrors
    self.text.DrawTextNDC(0.83,0.90,'%s=%.2f%% (%d)'%('TOT',float(toterrors)/tot*100,toterrors))
    for i,x in enumerate(ClaraErrors._BITS):
      self.text.DrawTextNDC(0.83,0.90-(i+1.5)*0.05,'%s=%.1f%%'%(x,float(self.errors[x])/tot*100))
    self.text.DrawTextNDC(0.01,0.90,'%s=%.2f%% (%d)'%('TOT',float(totslurmerrors)/tot*100,totslurmerrors))
    for i,x in enumerate(SlurmErrors._BITS):
      self.text.DrawTextNDC(0.01,0.90-(i+1.5)*0.05,'%s=%.1f%% (%d)'%(x,float(self.slurmerrors[x])/tot*100,self.slurmerrors[x]))
    title='Jobs:%d/%d, Files:%d/%d'%(self.successes,self.incomplete+self.successes,len(self.foundfiles),len(self.expectedfiles))
    for i,f in enumerate(JobSpecs._FLAVORS):
      ret=f+' '
      if self.flavors[f]['total']>0:
        ret+='%5.1f%% (%d)'%(100*float(self.flavors[f]['fail'])/(self.flavors[f]['fail']+self.flavors[f]['success']),self.flavors[f]['fail'])
      else:
        ret+='N/A'
      self.text.DrawTextNDC(0.01,0.40-(i+1.5)*0.05,ret)
    if self.title:
      title=self.title+'     '+title
    self.text.DrawTextNDC(0.3,0.96,title)
    self.canvas.BuildLegend(0.6,0.95-len(histos)*0.04,0.82,0.95)
    self.canvas.Update()

