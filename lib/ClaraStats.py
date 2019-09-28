
from JobSpecs import JobSpecs
from JobErrors import ClaraErrors
from JobErrors import SlurmErrors

_COLORS =[1       ,2       ,4       ,3       ,94]
_THREADS=[16, 20,   24,   32]
_FILLS  =[0,  3003, 3004, 3005]

class ClaraStats:

  def __init__(self):
    self.ROOT=__import__('ROOT')
    self.template=self.ROOT.TH1F('h',';Average Event Time per Core (ms);Jobs',100,0,2500)
    self.template.GetYaxis().SetTickLength(0)
    self.histos={}
    self.successes=0
    self.ntuple=None
    self.canvas=None
    self.text=None
    self.title=None
    self.errors={}
    self.slurmerrors={}
    self.flavors={}
    self.flavorlist=JobSpecs._FLAVORS
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
    for f in JobSpecs._FLAVORS:
      ret+=f+' '
      if self.flavors[f]['total']>0:
        ret+='%.2f%%'%(100*float(self.flavors[f]['fail'])/(self.flavors[f]['fail']+self.flavors[f]['success']))
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
    if not jl.isComplete():
      return
    if self.ntuple is None:
      self.ntuple=self.ROOT.TNtuple("claraStats","","nt:nf:ne:t1:t2:fl:fs")
    self.ntuple.Fill(jl.threads,jl.nfiles,jl.events,jl.t1,jl.t2,JobSpecs._FLAVORS.index(jl.flavor),jl.filesize)
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

  def save(self,filename):
    f=self.ROOT.TFile(filename,'RECREATE')
    f.cd()
    self.ntuple.Write()
    for x in self.histos.values():
      for y in x.values():
        y.Write()
    f.Close()

  def draw(self):
    if self.successes<=0:
      return
    self.ROOT.gStyle.SetOptTitle(0)
    self.ROOT.gStyle.SetOptStat(0)
    if self.canvas is None:
      t=self.title
      if t is None: t='none'
      self.canvas=self.ROOT.TCanvas('c',t,700,500)
      self.canvas.GetPad(0).SetRightMargin(0.18)
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
    for x in self.errors.keys():
      toterrors+=self.errors[x]
    for x in self.slurmerrors.keys():
      totslurmerrors+=self.slurmerrors[x]
    tot=self.successes+toterrors
    self.text.DrawTextNDC(0.83,0.90,'%s=%.2f%%'%('TOT',float(toterrors)/tot*100))
    for i,x in enumerate(ClaraErrors._BITS):
      self.text.DrawTextNDC(0.83,0.90-(i+1.5)*0.05,'%s=%.1f%%'%(x,float(self.errors[x])/tot*100))
    self.text.DrawTextNDC(0.12,0.90,'%s=%.2f%%'%('TOT',float(totslurmerrors)/tot*100))
    for i,x in enumerate(SlurmErrors._BITS):
      self.text.DrawTextNDC(0.12,0.90-(i+1.5)*0.05,'%s=%.1f%%'%(x,float(self.slurmerrors[x])/tot*100))
    if self.title:
      self.text.DrawTextNDC(0.4,0.96,self.title)
    self.canvas.BuildLegend(0.6,0.95-len(histos)*0.04,0.82,0.95)
    self.canvas.Update()

