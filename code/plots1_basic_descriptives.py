# %%
import pandas as pd
import numpy as np
import pathlib

import matplotlib
import matplotlib.pyplot as plt

from our_plot_config import derived_dir, fig_dir, raw_dir, setplotstyle

# Call function that sets the plot style
setplotstyle()
# %%
# Input file
f_betas = derived_dir  /'13f_sp500_unfiltered.parquet'
f_scraped = derived_dir/'13f_scraped.parquet'

# Figures
f_numowners = fig_dir  /'appfigure_a1.pdf'
fig_mgrs = fig_dir     /'figure3_nmgrs.pdf'
fig_nfirms = fig_dir   /'figure2_nfirms.pdf'
fig_ownership = fig_dir/'figure4_inst_share.pdf'



# ### Read in the (Cleaned) Parquet Files
# - One for TR $\beta$
# - One for scraped $\beta$

# %%
df=pd.read_parquet(f_betas)
dfs=pd.read_parquet(f_scraped)

# %%
# calculate number of managers overall
mgrA=df.groupby(['quarter'])['mgrno'].nunique()
mgrB=dfs.groupby(['quarter'])['mgrno'].nunique()
mgrs=pd.concat([mgrA,mgrB],axis=1)
mgrs.columns=['TR Data','Scraped Data']

# calculate number of managers per firm
mgr_tots=df.groupby(['quarter','permno'])['mgrno'].nunique().reset_index().groupby(['quarter'])['mgrno'].describe(percentiles=[.1,.5])
mgr_totsS=dfs.groupby(['quarter','permno'])['mgrno'].nunique().reset_index().groupby(['quarter'])['mgrno'].describe(percentiles=[.1,.5])

# %%
## 2 minutes# calculate number of firms
a=df[(df.sharecode_drop==False)&(df.permno_drop==False)].groupby(['quarter'])['permno'].nunique()
b=df.groupby(['quarter'])['permno'].nunique()
c=dfs[(dfs.sharecode_drop==False)&(dfs.permno_drop==False)].groupby(['quarter'])['permno'].nunique()
d=dfs.groupby(['quarter'])['permno'].nunique()

c=c[c.index>'2001-01-01']
d=d[d.index>'2001-01-01']

nfirms=pd.concat([a,b,c,d],axis=1)
nfirms.columns=['TR (Restricted)','TR (Unrestricted)','Scraped (Restricted)','Scraped (Unrestricted)']

# %%

t1=dfs[dfs.quarter=='2017-09-30']
t2=df[df.quarter=='2017-09-30']


# %%
# ### Plot Number of Firms 
# - total for entire dataset ( with and without drops)

# For matching the figures: truncate at EOY 2018
# comment this out if you want a full update! (not scraped)
nfirms = nfirms[nfirms.index <'2019-01-01']

# Figure 2
plt.clf()
ax=nfirms[['TR (Restricted)','TR (Unrestricted)']].plot(figsize=(20,10),color=['navy','maroon'])
#plt.axhline(y=500,color='0.75',linestyle = '--')
nfirms[['Scraped (Restricted)','Scraped (Unrestricted)']].plot(ax=ax,color=['navy','maroon'])
plt.xlabel("")

#plt.ylim(top=520, bottom=0)
plt.ylabel("Number of Firms in Sample")
plt.ylim(0,510)

plt.savefig(fig_nfirms,bbox_inches="tight")
# ### Plot Number of Managers
# - total for entire dataset
# - per firm
# - Figure 3

# %%
mgrs = mgrs[mgrs.index <'2019-01-01']

### Figure 3
plt.clf()
ax=mgrs['TR Data'].plot(figsize=(20,10),color='navy',style='-')
mgrs['Scraped Data'].plot(ax=ax,color='maroon',style='--')
plt.xlabel("")
plt.ylabel("Overall Number of 13f Managers")
plt.legend(['Thomson Reuters Data','Scraped 13(f) Data'])
plt.ylim(0,4100)
plt.savefig(fig_mgrs,bbox_inches="tight")

# %%
mgr_tots = mgr_tots[mgr_tots.index <'2019-01-01']
mgr_totsS = mgr_totsS[mgr_totsS.index <'2019-01-01']

### Appendix Figure A-1
plt.clf()
fig, ax = plt.subplots(figsize = (20,10))
mgr_tots[['mean', '50%', '10%', 'min']].plot(figsize=(20,10),ax=ax,style='-',color=['b', 'r', 'y', 'g'])
mgr_totsS[['mean', '50%', '10%', 'min']].plot(figsize=(20,10),ax=ax,style='--',color=['b', 'r', 'y', 'g'])
plt.xlabel("")
plt.ylabel("Number of Owners")
plt.ylim(0,900)
plt.legend(['Mean (TR)','Median (TR)','10th Percentile (TR)','Min (TR)','Mean (Scrape)','Median (Scrape)','10th Percentile (Scrape)','Min (Scrape)'],ncol=2)
plt.savefig(f_numowners,bbox_inches="tight")

# ### Percentage Institutional Ownership

# %%
### Figure 4
plt.clf()
df = df[df.quarter <'2019-01-01']
dfs = dfs[dfs.quarter <'2019-01-01']


df=df[(df.permno_drop==False)&(df.sharecode_drop==False) & (df.beta<0.5)]
dfs=dfs[(dfs.permno_drop==False)&(dfs.sharecode_drop==False) & (dfs.beta<0.5)]

a=(100*df.groupby(['permno','quarter'])['beta'].sum()).groupby(level=1).mean()
b=(100*dfs.groupby(['permno','quarter'])['beta'].sum()).groupby(level=1).mean()
pd.concat([a,b],axis=1).plot(figsize=(20,10),style=['-','--',],color=['navy','maroon'])
plt.ylabel("Percent Owned by 13(f) Investors")
plt.xlabel("")
plt.ylim(0,100)
plt.legend(['Thomson Reuters Data','Scraped 13(f) Data'])
plt.savefig(fig_ownership,bbox_inches="tight")