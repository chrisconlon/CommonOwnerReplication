# %%
# %%
import pandas as pd
import numpy as np
import pathlib

import matplotlib
import matplotlib.pyplot as plt

from our_plot_config import derived_dir, fig_dir, setplotstyle

setplotstyle()

# %%
# Input file
f_betas_tr = derived_dir / '13f_sp500_unfiltered.parquet'
f_betas_sc = derived_dir / '13f_scraped.parquet'

# outputs
fig_case_study = fig_dir / 'appfigure_a2_coverage.pdf'

# %%
# ## Read in Data
# 1. Need both TR Betas and Scrape Betas
# 2. Extract the three companies
# 3. Plot

# Read in input files
df_tr = pd.read_parquet(f_betas_tr)
df_sc = pd.read_parquet(f_betas_sc)

df_scrape_subset = df_sc[df_sc.permno.isin(['24643', '27983', '88661'])]
df_scrape_subset = df_scrape_subset[df_scrape_subset.quarter > '2007-01-01']
df_scrape_subset = df_scrape_subset[df_scrape_subset.quarter < '2015-01-01']
df_scrape_holdings = 100 * \
    df_scrape_subset.groupby(['quarter', 'permno']).sum()
df_tr_subset = df_tr[df_tr.permno.isin(['24643', '27983', '88661'])]
df_tr_subset = df_tr_subset[df_tr_subset.quarter > '2007-01-01']
df_tr_subset = df_tr_subset[df_tr_subset.quarter < '2015-01-01']
df_tr_holdings = 100 * df_tr_subset.groupby(['quarter', 'permno']).sum()

# %%
fig, ax = plt.subplots(figsize=(20, 10))
df_tr_holdings['beta'].unstack().plot(
    ax=ax, color=[
        'navy', 'maroon', 'darkgreen'], style=[
            '-', '-', '-'])
matplotlib.rc('xtick', labelsize=24)
matplotlib.rc('ytick', labelsize=24)
plt.ylabel("Percent of Shares Reported in 13F Filings", {'size': '24'})

df_scrape_holdings['beta'].unstack().plot(
    ax=ax,
    style=[
        '--',
        '--',
        '--'],
    color=[
        'navy',
        'maroon',
        'darkgreen'])
plt.xlabel("")

plt.legend(['Alcoa', 'Xerox', 'Coach'], prop={'size': '24'})
plt.ylim(0, 100)

plt.show
plt.savefig(fig_case_study, bbox_inches='tight')
