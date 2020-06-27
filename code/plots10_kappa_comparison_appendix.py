# %%
from utilities.matlab_util import coalesce
import pandas as pd
import numpy as np
import pathlib

import matplotlib
import matplotlib.pyplot as plt
from our_plot_config import derived_dir, fig_dir, setplotstyle

setplotstyle()

# %%


# Input file
f_kappas = derived_dir / 'appendix_kappa_combined.parquet'
f_firms = derived_dir / 'firm-information.parquet'

# Figures
f_profitweights_comp1 = fig_dir / 'appfigure_a3.pdf'

# %%
# ### Read in the (Cleaned) Parquet File
# - Apply the $\kappa$ calculations period by period
# - Save the output to a new parquet file

total_df = pd.read_parquet(f_kappas)
total_df['tunnel'] = (total_df['skappa'].combine_first(total_df['kappa']) > 1)
total_df = total_df[total_df['from'] != total_df['to']]
qtr_mean = total_df.groupby(['quarter']).mean()

qtr_mean = total_df.groupby(['quarter']).mean()

qtr_mean = qtr_mean[qtr_mean.index < '2019-01-01']

# %%


col_list = [
    'l1_measure',
    'kappa',
    'kappa_pow2',
    'kappa_pow3',
    'kappa_sqrt',
    'kappa_CLWY']
qtr_mean = coalesce(qtr_mean, col_list, 's', method='left')

# %%

# ## Make the plots
# ### Comparisons
#  - Compare TR Data (Solid) and Scraped 13-F Data (Dashed)


qtr_mean[['kappa', 'skappa']].plot(
    figsize=(20, 10), style=['-', '--'], color=['navy', 'maroon'])
plt.xlabel("")
plt.ylabel(r"$\kappa$ weight")
plt.legend([r'TR Data', 'Scraped Data', ])
plt.ylim(0, 1.2)
plt.savefig(f_profitweights_comp1, bbox_inches='tight')
