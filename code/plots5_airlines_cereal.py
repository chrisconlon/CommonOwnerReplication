# %%
# %%
import pandas as pd
import numpy as np
import pathlib

import matplotlib
import matplotlib.pyplot as plt

from our_plot_config import derived_dir, fig_dir, raw_dir, setplotstyle
from kappas import do_one_period

setplotstyle()

# %%
# Input files
f_cereal = raw_dir / 'cereal.parquet'
f_airlines = raw_dir / 'airlines.parquet'
f_firm_info = derived_dir / 'firm-info.parquet'
f_kappas = derived_dir / 'official-kappas.parquet'

# Figure outputs
fig_both = fig_dir / 'figure16_airlines_cereal_banks.pdf'

# %%
# ### Read in the (Cleaned) Parquet File of Beta's
# - Read in stata file
# - Create the "quarter" variable
# - Apply the $\kappa$ calculations period by period
# - Save the output to a new parquet file
# - Write a Stata file.

# %%
# read in, create quarter and drop kappa_ff


def process_df(fn):
    df = pd.read_parquet(fn)
    df['quarter'] = pd.to_datetime(df.rdate, format='%Y%m%d')
    total_df3 = df[df.beta < 0.5].groupby(['quarter']).apply(do_one_period)
    total_df3 = total_df3[total_df3['from'] != total_df3['to']]
    return total_df3.reset_index()


df_cereal = process_df(f_cereal)
# Clean up airlines a bit more
df_airlines = process_df(f_airlines)
df_airlines = df_airlines[df_airlines.kappa < 4].copy()

df_firms = pd.read_parquet(f_firm_info)
df_firms2 = df_firms.loc[df_firms['siccd'] ==
                         6021, ['permno', 'quarter', 'comnam']].copy()

df_k = pd.read_parquet(f_kappas)

df_banks = pd.merge(pd.merge(df_k[df_k['from'] != df_k['to']], df_firms2, left_on=['quarter', 'from'], right_on=['quarter', 'permno']),
                    df_firms2, left_on=['quarter', 'to'], right_on=['quarter', 'permno'])

# %%
df_tot = pd.concat([df_cereal.groupby(['quarter'])['kappa'].median(), df_airlines.groupby(
    ['quarter'])['kappa'].median(), df_banks.groupby(['quarter'])['kappa'].median()], axis=1)

# %%
df_tot[df_tot.index >
       '1999-01-01'].plot(figsize=(20, 10), color=['navy', 'maroon', 'darkgreen'])
plt.legend(['RTE Cereal', 'Airlines', 'Banks'])
plt.ylabel(r"Median Pairwise Profit Weights $(\kappa)$")
plt.xlabel("")
plt.ylim(0, 1)
plt.savefig(fig_both, bbox_inches='tight')
