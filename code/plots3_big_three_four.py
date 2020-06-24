# %%
import pandas as pd
import numpy as np
import pathlib

import matplotlib
import matplotlib.pyplot as plt

from our_plot_config import derived_dir, fig_dir, raw_dir, setplotstyle

setplotstyle()

from kappas import process_beta 
from investors import calc_big4

## Input file
# Get this from 2.5 Scraped Data
f_stata_k =derived_dir / 'tmp-kappas_scrape.dta'
f_betas = derived_dir / '13f_sp500_frankenbeta.parquet'
f_big4= raw_dir /'big4.csv'

f_investor = derived_dir / 'investor-info.parquet'
## Figures
fig_bigfour = fig_dir/ 'py_snp_bigfour.pdf'
fig_bigthree= fig_dir/ 'figure5_big3.pdf'

# %%
# ## Description
# 1. Load the data
# 2. Setup the plots
# 3. Plot top 4
# 4. Plot top 3


df=100.0*calc_big4(process_beta(f_betas),pd.read_csv(f_big4))
df2=df.groupby(level=0).mean()


# %%
def make_bigfour_plot(df,top3=False):    
    y=df.groupby(level=0).mean()
    y.index=[pd.to_datetime(date, format='%Y-%m-%d').date() for date in y.index]
    if top3:
        y=y.iloc[:,0:-1]
    y.plot(figsize=(20,10),color=['black','maroon','navy','green'])

    plt.ylim(0,10)
    plt.xlim('2000-01-01','2018-01-01')

    plt.xlabel("")
    plt.ylabel('Average Ownership Percentage')
    
    plt.annotate('iShares Acquisition', xy=('2010-02-15', 6.2),  xycoords='data',
                xytext=('2010-02-15',8), textcoords='data',
                arrowprops=dict(facecolor='black', shrink=0.05),
                horizontalalignment='center', verticalalignment='top',
                )
    plt.legend(['BlackRock & Barclays','Vanguard','State Street','Fidelity'])
    return plt


# %%
# Figure 5
make_bigfour_plot(df2[df2.index>='1999-12-31'],True)
plt.savefig(fig_bigthree,bbox_inches="tight")


# %%
# Alternate with Fidelity (don't use this one)
#make_bigfour_plot(df,False)



