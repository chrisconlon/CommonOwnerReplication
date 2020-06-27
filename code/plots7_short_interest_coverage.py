# %%
import pandas as pd
import numpy as np
import pathlib

import matplotlib
import matplotlib.pyplot as plt

from our_plot_config import derived_dir, fig_dir, raw_dir, wrds_dir, setplotstyle

setplotstyle()

# %%

# inputs
f_betas = derived_dir / '13f_sp500_frankenbeta.parquet'
f_short = wrds_dir / 'short_interest.parquet'

# figures
fig_coverage = fig_dir / 'appfigure_a4.pdf'
fig_distribution = fig_dir / 'appfigure_a5.pdf'

# %%
# ## Short Interest Checks
# - Read in the main "betas" dataset --> aggregate to firm-quarter across managers
# - Read in the COMPUSTAT Short interest dataset
# - Merge them by firm-quarter
# - Plot S&P Coverage of Short Interest
# - Plot quantiles of short interest distribution (conditional on coverage)

df = pd.read_parquet(f_betas)
df = df[df.quarter > '1980-01-01']
df_short = pd.read_parquet(f_short)


# %%
tmp = pd.merge(df.groupby(['permno',
                           'quarter']).agg({'beta': sum,
                                            'shares': sum,
                                            'shares_outstanding': max}).reset_index(),
               df_short,
               left_on=['permno',
                        'quarter'],
               right_on=['lpermno',
                         'qdate'],
               how='left')
tmp['short_coverage'] = 1.0 * (~tmp['shortint'].isnull())
tmp['coverage'] = 1.0
tmp['short_pct'] = tmp['shortint'] / (tmp['shares_outstanding'] * 1000)
tmp['short_1'] = tmp['short_pct'] > 0.01
tmp['short_2'] = tmp['short_pct'] > 0.02
tmp['short_5'] = tmp['short_pct'] > 0.05
tmp['short_10'] = tmp['short_pct'] > 0.10
tmp['short_20'] = tmp['short_pct'] > 0.20

# %%
tmp.groupby(['quarter'])[['coverage', 'short_coverage']].sum().plot(
    figsize=(20, 10), color=['navy', 'maroon'])
matplotlib.rc('xtick', labelsize=24)
matplotlib.rc('ytick', labelsize=24)
plt.legend(['Number of S&P 500 Firms in Sample',
            'Number of S&P 500 Firms with Short Interest Data'])
#plt.title('Coverage of S&P 500 Firms')
plt.xlabel('')
plt.ylim(0, 510)
plt.savefig(fig_coverage, bbox_inches='tight')
# %%
ax = tmp.groupby(['quarter'])[['short_1', 'short_2', 'short_5',
                               'short_10', 'short_20']].mean().plot(figsize=(20, 10))
ax.set_ylim(0, 1)
matplotlib.rc('xtick', labelsize=24)
matplotlib.rc('ytick', labelsize=24)
plt.legend(['Above 1%', 'Above 2%', 'Above 5%', 'Above 10%', 'Above 20%'])
#plt.title('Fraction of Firms by Short Interest (conditional on coverage) ')
vals = ax.get_yticks()
ax.set_yticklabels(['{:,.2%}'.format(x) for x in vals])
plt.xlabel('')
plt.savefig(fig_distribution, bbox_inches='tight')
