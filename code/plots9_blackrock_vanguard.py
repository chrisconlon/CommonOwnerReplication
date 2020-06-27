# %%
from kappas import beta_to_kappa_merger_breakup
import pandas as pd
import numpy as np
import pathlib

import matplotlib
import matplotlib.pyplot as plt

from our_plot_config import derived_dir, fig_dir, raw_dir, setplotstyle

setplotstyle()


# %%
# Input file
f_betas = derived_dir / '13f_sp500_frankenbeta.parquet'
f_big4 = raw_dir / 'big4.csv'

# Figures
f_merger = fig_dir / 'figure12_mergerbreakup.pdf'


# ## Load Data and Setup
# - Read in the Parquet File of $\beta$'s
# - Read in the csv file of big four firms
# - Setup the merger using mgrno_merger
# - Setup the breakup using InvestorName
# - Apply the $\kappa$ calculations period by period

# %%
df = pd.read_parquet(
    f_betas,
    columns=[
        'mgrno',
        'permno',
        'quarter',
        'beta',
        'permno_drop',
        'sharecode_drop'])
df = df[(df.permno_drop == False) & (
    df.sharecode_drop == False) & (df.beta < 0.5)]

big4 = pd.read_csv(f_big4)

# grab ids for Blackrock and Vanguard
is_blackrock = set(big4[big4.InvestorName == 'BlackRock'].mgrno.values)
is_vanguard = set(big4[big4.InvestorName == 'Vanguard'].mgrno.values)

# %%
# merge firms meeting the criteria
df['mgrno_merger'] = df.mgrno
df.loc[df['mgrno_merger'].isin(is_blackrock.union(
    is_vanguard)), 'mgrno_merger'] = 1139734

# Only break up firms with a name
df.loc[df.mgrno.isin(is_blackrock), 'InvestorName'] = 'BlackRock'
df.loc[df.mgrno.isin(is_vanguard), 'InvestorName'] = 'Vanguard'

# %%
# ### Do the work, Make the Plot
kappa_df = beta_to_kappa_merger_breakup(df)
x = kappa_df[kappa_df['from'] != kappa_df['to']].groupby(
    ['quarter'])[['kappa', 'kappa_merger', 'kappa_breakup', 'kappa_drop']].mean()

# %%
x.plot(figsize=(20, 10), color=['black', 'navy', 'maroon', 'darkgreen'])
plt.legend([r'$\kappa$: Actual Ownership',
            r'$\kappa$: Merger: BlackRock+Vanguard',
            r'$\kappa$: Split in Half: BlackRock+Vanguard',
            r'$\kappa$: Ignore: BlackRock+Vanguard'])
plt.xlabel("")
plt.ylim(0, 1)
plt.savefig(f_merger, bbox_inches="tight")
