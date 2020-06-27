# %%
import pandas as pd
import numpy as np
import pathlib

import matplotlib
import matplotlib.pyplot as plt

from our_plot_config import derived_dir, fig_dir, setplotstyle

setplotstyle()

# %%
# inputs
f_kappas_combined = derived_dir / 'appendix_kappa_combined.parquet'

# outputs
fig_soleshared_tr = fig_dir / 'figure17_tr.pdf'
fig_soleshared_sc = fig_dir / 'figure17_sc.pdf'

# %%
# ## Read in Data
# 1. Only read in Sole/Shared/All columns
# 2. Do TR data for pre 2011 (afterwards this becomes unreliable).
# 3. Do Scraped data for 2013 onwards (when XML data is available).
# - We would need to re-do scraping to grap sole/shared/all/none for 1999-2013 for non-XML scraped data


col_list = [
    'from',
    'to',
    'quarter',
    'kappa',
    'kappa_all',
    'kappa_sole',
    'kappa_soleshared',
    'skappa',
    'skappa_all',
    'skappa_sole',
    'skappa_soleshared']
df = pd.read_parquet(f_kappas_combined, columns=col_list)
df = df[df.quarter > '1999-01-01']


# %%
df[df.quarter < '2011-01-01'].groupby(['quarter']).mean(
)[['kappa', 'kappa_sole', 'kappa_soleshared']].plot(figsize=(20, 10))
plt.xlabel("")
plt.legend(['All Shares', 'Sole Voting Rights', 'Sole+Shared Voting Rights'])
#plt.title('Alternative Control Assumptions: TR data')
plt.ylim(0, 1)
plt.savefig(fig_soleshared_tr, bbox_inches="tight")


# %%
df[(df.quarter > '2013-09-30') & (~df.skappa.isnull())].groupby(['quarter']
                                                                ).mean()[['skappa', 'skappa_sole', 'skappa_soleshared']].plot(figsize=(20, 10))
plt.xlabel("")
plt.legend(['All Shares', 'Sole Voting Rights', 'Sole+Shared Voting Rights'])
plt.ylim(0, 1)
#plt.title('Alternative Control Assumptions: Scraped data')
plt.savefig(fig_soleshared_sc, bbox_inches="tight")
