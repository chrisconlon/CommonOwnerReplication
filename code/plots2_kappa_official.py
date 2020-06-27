# %%
import pandas as pd
import numpy as np
import pathlib

import matplotlib.pyplot as plt
from scipy.stats.mstats import gmean

from our_plot_config import derived_dir, setplotstyle, fig_dir
setplotstyle()

# %%


# Input file
f_kappas = derived_dir / 'official-kappas.parquet'
f_firms = derived_dir / 'firm-info.parquet'

# temp output - for macro simulations
f_quarter_mean = derived_dir / 'tmp-quarter-mean.pickle'

# Figures
# Kappa
f_profitweights = fig_dir / 'figure1_kappa.pdf'
f_profitweights_all = fig_dir / 'figure13_kappa_control.pdf'
f_within_between = fig_dir / 'figure15_within_between.pdf'
f_kappa_quantile = fig_dir / 'appfigure_a6.pdf'

# Concentration
f_ihhi = fig_dir / 'figure6_ihhi.pdf'
f_cosine = fig_dir / 'figure7_cosine.pdf'
f_chhi = fig_dir / 'figure14_chhi1.pdf'
f_chhi2 = fig_dir / 'figure14_chhi2.pdf'

# Tunneling
f_tunneling = fig_dir / 'figure9_tunneling.pdf'
f_kap1 = fig_dir / 'appfigure_a7.pdf'

# compute weighted average for kappa with different weighting schemes


def weighted(x, cols):
    a1 = np.average(x[cols].values, weights=x['w_amean'].values, axis=0)[0]
    a2 = np.average(x[cols].values, weights=x['w_gmean'].values, axis=0)[0]
    a3 = np.average(x[cols].values, weights=x['mkt_cap_to'].values, axis=0)[0]
    a4 = np.average(
        x[cols].values,
        weights=x['mkt_cap_from'].values,
        axis=0)[0]
    a5 = np.average(x[cols].values, weights=x['saleq_x'].values, axis=0)[0]
    a6 = np.average(x[cols].values, weights=x['saleq_y'].values, axis=0)[0]
    a7 = np.average(x[cols].values, weights=x['w_s_gmean'].values, axis=0)[0]

    return pd.Series({'kappa_amean': a1, 'kappa_gmean': a2, 'kappa_from': a3, 'kappa_to': a4,
                      'kappa_sale_from': a4, 'kappa_sale_to': a4, 'kappa_sale_mean': a4})


# ### Read in the (Cleaned) Parquet File
# - Apply the $\kappa$ calculations period by period
# - Save the output to a new parquet file

# %%

df = pd.read_parquet(f_kappas)
df_firm = pd.read_parquet(f_firms)
ihhi = df_firm[['permno', 'quarter', 'ihhi', 'siccd', 'saleq']]

# merge to get weights (sales and market cap, from/to)
total_df = pd.merge(
    pd.merge(
        df, ihhi, left_on=[
            'from', 'quarter'], right_on=[
            'permno', 'quarter'], how='left'),
    ihhi, left_on=['to', 'quarter'], right_on=['permno', 'quarter'], how='left')
total_df['same_sic'] = (total_df['siccd_x'] == total_df['siccd_y'])
total_df[total_df['from'] != total_df['to']]

# Average of weights
total_df['w_amean'] = (total_df['mkt_cap_from'] + total_df['mkt_cap_to']) / 2.0
total_df['w_gmean'] = gmean(
    [total_df['mkt_cap_from'], total_df['mkt_cap_to']], axis=0)
total_df['w_s_gmean'] = gmean(
    [total_df['saleq_x'], total_df['saleq_y']], axis=0)

# Apply the weighted averages
y = total_df.groupby(['quarter']).apply(weighted, ["kappa"])


qtr_mean = pd.concat([total_df.groupby(['quarter']).mean(), y], axis=1)


df_cosine = total_df.groupby(
    ['quarter'])['cosine'].describe(
        percentiles=[
            0.05, 0.25, 0.5, 0.75, 0.95])

# Percentiles of Kappa and IHHI
kappa_pct = df.groupby(
    ['quarter'])['kappa'].describe(
        percentiles=[
            0.05,
            0.25,
            0.5,
            0.75,
            0.95])
ihhi_pct = ihhi[~ihhi.ihhi.isnull()].groupby(['quarter'])['ihhi'].describe(
    percentiles=[0.05, 0.25, 0.5, 0.75, 0.95])

# drop k_ff =1 cases for tunneling
tunnel_df = (df[df['from'] != df['to']].set_index('quarter')[
             ['kappa_sqrt', 'kappa', 'kappa_pow2', 'kappa_pow3']] > 1).groupby(level=0).mean()
tunnel_df2 = (df[df['from'] != df['to']].set_index(['from', 'quarter'])[
              ['kappa_sqrt', 'kappa', 'kappa_pow2', 'kappa_pow3']] > 1).groupby(level=[0, 1]).max()


# %%
# need this for the macro simulations
qtr_mean.to_pickle(f_quarter_mean)

# %%
# ### Kappas
# - Single Kappa ( Figure 1)
# - Alternative Control (Figure 13)
# - Within and Between Industry (Figure 15)


# Alternate Figure 1 (revision)
plt.clf()
qtr_mean[['kappa', 'kappa_gmean', 'kappa_sale_mean']].plot(figsize=(20, 10))
plt.legend(['Equal Weights', 'Market Cap Weighted', 'Revenue Weighted'])
plt.xlabel('')
plt.ylabel(r"$\kappa$ weight")
plt.ylim(0, 1)
plt.savefig(f_profitweights, bbox_inches="tight")

# %%


# Appendix Figure 13
plt.clf()
qtr_mean[['kappa', 'kappa_sqrt', 'kappa_pow2',
          'kappa_pow3']].plot(figsize=(20, 10))
#plt.title("Average Pairwise Profit Weights $(\kappa)$ Under Different Control Assumptions")
plt.xlabel("")
plt.ylabel(r"$\kappa$ weight")
plt.ylim(0, 1)
plt.legend([r'$\gamma = \beta$',
            r'$\gamma \propto \sqrt{\beta}$',
            r'$\gamma \propto \beta^2$',
            r'$\gamma \propto \beta^3$'])
plt.savefig(f_profitweights_all, bbox_inches="tight")

# %%


# Figure 15: Within Between
plt.clf()
total_df[(total_df.same_sic == True)].groupby(
    ['quarter'])['kappa'].mean().plot(figsize=(20, 10))
total_df[(total_df.same_sic == False)].groupby(
    ['quarter'])['kappa'].mean().plot()
#plt.title("Average Pairwise Profit Weights $(\kappa)$ Within and Between SIC code")
plt.xlabel("")
plt.ylabel(r"$\kappa$ weight")
plt.ylim(0, 1)
plt.legend([r"$\kappa$ same SIC", r"$\kappa$ different SIC"])
plt.savefig(f_within_between, bbox_inches="tight")

# %%


# Response Quantiles of Kappa
plt.clf()
kappa_pct[['95%', '75%', '50%', '25%', '5%']].plot(figsize=(20, 10))
plt.legend(['95th percentile',
            '75th percentile',
            '50th percentile',
            '25th percentile',
            '5th percentile'])
plt.ylabel(r"$\kappa$ Quantiles")
plt.xlabel("")
plt.ylim(0, 1)
plt.savefig(f_kappa_quantile, bbox_inches="tight")
# %%

# ### Concentration
# - IHHI (Figure 6)
# - Cosine Similarity (Figure 7)
# - CHHI (Figure 14 - 2 parts)


# Figure 6
ihhi_pct[['95%', '75%', '50%', '25%', '5%']].plot(figsize=(20, 10))
plt.legend(['95th percentile',
            '75th percentile',
            '50th percentile',
            '25th percentile',
            '5th percentile'])
plt.ylabel("Investor HHI")
plt.xlabel("")
plt.ylim(0, 600)
plt.savefig(f_ihhi, bbox_inches="tight")

# %%


# Figure 7
total_df.groupby(['quarter'])[['kappa', 'cosine', 'l1_measure']
                              ].mean().plot(figsize=(20, 10))
plt.xlabel("")
#plt.title("Cosine Similarity and $\kappa$")
plt.ylim(0, 1)
plt.legend([r'$\kappa_{f,g}$',
            r'$L_2$ similarity $cos(\beta_f,\beta_g)$',
            r'$L_1$ similarity $|\beta_f - \beta_g|$'])
plt.savefig(f_cosine, bbox_inches="tight")

# %%


# Figure 14a
df_firm[['quarter', 'ihhi', 'chhi_05', 'chhi_2', 'chhi_3', 'chhi_4']
        ].groupby(['quarter']).mean().plot(figsize=(20, 10))
plt.xlabel("")
plt.ylabel("Effective Control HHI")
plt.ylim(0, 3500)
plt.legend([r'$\gamma = \beta$',
            r'$\gamma \propto \sqrt{\beta}$',
            r'$\gamma \propto \beta^2$',
            r'$\gamma \propto \beta^3$',
            r'$\gamma \propto \beta^4$'])
plt.savefig(f_chhi, bbox_inches="tight")
# %%


# Figure 14b
df_firm[['quarter', 'ihhi', 'chhi_05']].groupby(
    ['quarter']).mean().plot(figsize=(20, 10))
plt.xlabel("")
plt.ylabel("Effective Control HHI")
plt.ylim(0, 350)
plt.legend([r'$\gamma = \beta$', r'$\gamma \propto \sqrt{\beta}$', ])
plt.savefig(f_chhi2, bbox_inches="tight")

# ### Tunneling
# - Figure 9: Tunneling
# - App Figure C-6: Tunneling (Alternative Control)

# %%
(100.0 * tunnel_df[['kappa']]).plot(figsize=(20, 10))
plt.xlabel("")
#plt.title("Potential Tunneling")
plt.ylabel(r"Percentage of $\kappa$ > 1")
plt.legend('')
plt.ylim(0, 12)
#plt.legend([r'$\gamma = \beta$',r'$\gamma \propto \sqrt{\beta}$',r'$\gamma \propto \beta^2$',r'$\gamma \propto \beta^3$'])
plt.savefig(f_tunneling, bbox_inches="tight")

# %%
(100.0 * tunnel_df[['kappa', 'kappa_sqrt',
                    'kappa_pow2', 'kappa_pow3']]).plot(figsize=(20, 10))
plt.xlabel("")
#plt.title("Potential Tunneling")
plt.ylabel(r"Percentage of $\kappa$ > 1")
plt.ylim(0, 20)
plt.legend([r'$\gamma = \beta$',
            r'$\gamma \propto \sqrt{\beta}$',
            r'$\gamma \propto \beta^2$',
            r'$\gamma \propto \beta^3$'])
plt.savefig(f_kap1, bbox_inches="tight")
