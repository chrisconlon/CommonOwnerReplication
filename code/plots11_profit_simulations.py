# %%
import pandas as pd
import numpy as np
import pathlib
import pyblp
import matplotlib
import matplotlib.pyplot as plt

from our_plot_config import derived_dir, fig_dir, raw_dir, setplotstyle

setplotstyle()


pyblp.options.collinear_atol = pyblp.options.collinear_rtol = 0
pyblp.options.verbose = False

# jan and jan markups input
f_jj_markups = raw_dir / 'DLE_markups_fig_v2.csv'

# temp input
f_quarter_mean = derived_dir / 'tmp-quarter-mean.pickle'
f_markup_out = derived_dir / 'markup-simulations.csv'

fig_markups = fig_dir / 'macro-simulated-markups.pdf'
fig_markups_jj = fig_dir / 'figure10_markups.pdf'
fig_profits = fig_dir / 'figure11_profits.pdf'


def combine64(years, months=1, days=1, weeks=None, hours=None, minutes=None,
              seconds=None, milliseconds=None, microseconds=None, nanoseconds=None):
    years = np.asarray(years) - 1970
    months = np.asarray(months) - 1
    days = np.asarray(days) - 1
    types = ('<M8[Y]', '<m8[M]', '<m8[D]', '<m8[W]', '<m8[h]',
             '<m8[m]', '<m8[s]', '<m8[ms]', '<m8[us]', '<m8[ns]')
    vals = (years, months, days, weeks, hours, minutes, seconds,
            milliseconds, microseconds, nanoseconds)
    return sum(np.asarray(v, dtype=t) for t, v in zip(types, vals)
               if v is not None)

# %%
# change demand parameters and number of firms
# first param (intercept -- outside good share)
# secon param (alpha -- price coefficient/elasticity)
# third param ALWAYS ZERO


betas = [25, -6.8, 0]
n_firms = 8

# Don't change MC params
gammas = [1, 0]


def run_simulation(n_firms, betas, gammas, kappa=0, maverick=False):
    config_data = pyblp.build_id_data(T=1, J=n_firms, F=n_firms)
    mutable_id_data = {k: config_data[k] for k in config_data.dtype.names}
    mutable_id_data['ownership'] = construct_ownership(
        n_firms, kappa, maverick)

    simulation = pyblp.Simulation(product_formulations=(pyblp.Formulation('1 + prices+x1'), None, pyblp.Formulation('1+x1')),
                                  beta=betas, sigma=None, gamma=gammas, xi_variance=1e-6, omega_variance=1e-6,
                                  product_data=mutable_id_data, seed=0)

    # solve the simulation for P+Q
    prod_data = simulation.replace_endogenous()

    # Construct a Problem and Solve
    # Don't estimate a model since we know the answers and only want to do
    # counterfactuals
    res = prod_data.to_problem().solve(beta=betas, gamma=gammas, sigma=None,
                                       optimization=pyblp.Optimization('return'))

    # Pull the calculated P,Q,Profits,Diversion,etc
    inside_share = np.sum(prod_data.product_data['shares'])
    prices = np.mean(prod_data.product_data['prices'])
    og_diversion = np.mean(np.diag(res.compute_diversion_ratios()))
    own_elas = np.diag(res.compute_elasticities()).mean()
    total_pi = res.compute_profits().sum()
    return (inside_share, prices, og_diversion, own_elas, total_pi)


def construct_ownership(nfirms, kappa=0, maverick=False):
    O = np.ones((n_firms, n_firms)) * kappa
    if maverick:
        O[0, :] = 0
        O[:, 0] = 0
    np.fill_diagonal(O, 1)
    return O


def run_several(kappa_list, maverick=False):
    mylist = [
        run_simulation(
            n_firms,
            betas,
            gammas,
            k,
            maverick) for k in kappa_list]
    df_out = pd.DataFrame(mylist)
    df_out.columns = [
        'inside_share',
        'prices',
        'og_diversion',
        'own_elas',
        'total_pi']
    df_out.index = df.index
    return df_out


# %%
(inside_share, prices, og_diversion, own_elas, pi_1) = run_simulation(
    n_firms, betas, gammas, kappa=0.21)
(inside_share2, prices2, og_diversion2, own_elas2,
 pi_2) = run_simulation(n_firms, betas, gammas, kappa=0.7)
(base_share, base_prices, base_og, base_elas,
 pi_base) = run_simulation(n_firms, betas, gammas, kappa=0)

print("\n\nZero Kappa (Bertrand) \n")
print("Inside Share:", base_share)
print("OG Diversion:", base_og)
print("Own Elas:", base_elas)
print("Markups:", base_prices)
print("Profits:", pi_base)

print("\n\n1980 KAPPA\n")
print("Inside Share:", inside_share)
print("OG Diversion:", og_diversion)
print("Own Elas:", own_elas)
# Price is Markup * Cost and cost ==1
print("Markups:", prices)
print("Profits:", pi_1)

print("\n\n2017 KAPPA\n")
print("Inside Share:", inside_share2)
print("OG Diversion:", og_diversion2)
print("Own Elas:", own_elas2)
print("Markups:", prices2)
print("Profits:", pi_2)


# %%
# Calibrate to our Kappas
df = pd.read_pickle(f_quarter_mean)
df_out = run_several(df.kappa)
df_out2 = run_several(df.kappa, maverick=True)
df_out2.columns = [
    'inside_mav',
    'prices_mav',
    'og_diversion_mav',
    'own_elas_mav',
    'total_pi_mav']
df_out = pd.concat([df_out, df_out2], sort=False, axis=1)

# %%
# ## Macro Quantification
# - Read in quarterly average kappa
# - Quantify Common Ownership Channel
# - Plot rising implied prices (Markups)


df_out[['prices', 'prices_mav']].plot(figsize=(20, 10))
plt.xlabel('')
plt.ylabel('Markup over Cost', size=24)
plt.legend(['Without Maverick', 'With Maverick'])
plt.ylim(1.0, 1.6)

plt.savefig(fig_markups, bbox_inches='tight')


# %%
df_jj = pd.read_csv(
    f_jj_markups,
    low_memory=False,
    names=[
        'year',
        'jj_markup'])
df_jj.year = pd.DatetimeIndex(combine64(df_jj.year))
df_jj = df_jj.set_index('year')
df_jj = df_jj.resample('Q').mean().ffill()
df_jj = df_jj[df_jj.index >= '1980-01-01']
df2 = pd.merge(df_out, df_jj, left_index=True, right_index=True, how='left')
df2.to_csv(f_markup_out)


# %%
matplotlib.rc('xtick', labelsize=24)
matplotlib.rc('ytick', labelsize=24)

df2 = df2[~df2.jj_markup.isnull()].copy()


df2[['prices', 'jj_markup', 'prices_mav']].plot(
    figsize=(20, 10), color=['black', 'navy', 'maroon'])
plt.legend(['Common Ownership Markups',
            'DeLoecker, Eeckhout, Unger (2020)',
            'Common Ownership (w/ maverick)'],
           prop={'size': 24},
           loc='upper left')
plt.xlabel('')
plt.ylabel('Markup over Cost', size=24)
plt.ylim(1, 1.65)

plt.savefig(fig_markups_jj, bbox_inches='tight')

# %%
rel_pi = df_out[['total_pi', 'total_pi_mav']] / pi_base

matplotlib.rc('xtick', labelsize=24)
matplotlib.rc('ytick', labelsize=24)

rel_pi.plot(figsize=(20, 10), color=['navy', 'maroon'])
#plt.legend(['Profits Relative to Symmetric Differentiated Bertrand (HHI=1250)','With Maverick'], prop={'size': 24})
plt.legend(['Without Maverick', 'With Maverick'])
plt.xlabel('')
plt.ylim(1, 3.5)

plt.ylabel('Profits vs. Symmetric Bertrand HHI=1250', size=24)
plt.savefig(fig_profits, bbox_inches='tight')
