# %%
import pandas as pd
import numpy as np
import pathlib

import matplotlib.pyplot as plt

from our_plot_config import derived_dir, setplotstyle, fig_dir

setplotstyle()

# %%
# input file
f_investor = derived_dir / 'investor-info.parquet'

#outputs
fig_both_sim = fig_dir / 'figure8_similarity.pdf'
fig_both_sim_drops = fig_dir / 'appfigure_a8.pdf'

def wavg_l2(group):
    d = group['l2_similarity']
    w = group['aum_weight']
    return (d * w).sum() / w.sum()

def wavg_l1(group):
    d = group['l1_similarity']
    w = group['aum_weight']
    return (d * w).sum() / w.sum()

# %%
df=pd.read_parquet(f_investor)

df3=pd.concat([df.groupby('quarter').apply(wavg_l1),df.groupby('quarter').apply(wavg_l2)],axis=1)
df3.columns=['investor_l1','investor_l2']
df3=df3[['investor_l2','investor_l1']].copy()

# Without blackrock vanguard?
df2=df[~df.InvestorName.isin(['BlackRock','Vanguard'])]
df4=pd.concat([df2.groupby('quarter').apply(wavg_l1),df2.groupby('quarter').apply(wavg_l2)],axis=1)
df4.columns=['l1_drop_blackrockvanguard','l2_drop_blackrockvanguard']

# %%
# ### Make the Plots

df3.plot(figsize=(20,10),color=['navy','maroon'])
plt.legend(['Investor Similarity $(L_2)$','Investor Similarity $(L_1)$'])
plt.ylabel("Investor Similarity (AUM Weighted)")
plt.xlabel("")
plt.ylim(0, 1)
plt.savefig(fig_both_sim,bbox_inches='tight')

# %%
ax=pd.concat([df3,df4],axis=1)[['investor_l2','l2_drop_blackrockvanguard','investor_l1','l1_drop_blackrockvanguard']].plot(figsize=(20,10),color=['navy','navy','maroon','maroon'],style=['-','--','-.',':'])
plt.legend(['Investor Similarity $(L_2)$','Investor Similarity $(L_2)$ (No BlackRock/Vanguard)','Investor Similarity $(L_1)$','Investor Similarity $(L_1)$ (No BlackRock/Vanguard)'])
plt.xlabel("")
plt.ylabel("Investor Similarity (AUM Weighted)")
plt.ylim(0, 1)
plt.savefig(fig_both_sim_drops,bbox_inches='tight')



