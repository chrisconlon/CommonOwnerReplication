### Table 4 Correlations with Kappa
import pandas as pd
import numpy as np

from our_plot_config import derived_dir, tab_dir

import pyhdfe
from sklearn import datasets, linear_model

import statsmodels.formula.api as smf
from statsmodels.iolib.summary2 import summary_col

# %%
f_firm_info =derived_dir / 'firm-info.parquet'
f_kappas = derived_dir   /'official-kappas.parquet'
f_tab4 = tab_dir   /'tab4_cm.tex'

# %%
# Read in the Two Files: two minutes
df_kappas = pd.read_parquet(f_kappas)
df_firm = pd.read_parquet(f_firm_info)

# %%
# keep if year >= 2000 & year < 2018
# drop if from == to ;

df_kappas = df_kappas[df_kappas.quarter > '2000-01-01'].copy()
df_firm = df_firm[df_firm.quarter > '2000-01-01'].copy()

indexdrop =  (df_kappas.loc[df_kappas['from'] == df_kappas['to']]).index
df_kappas = df_kappas.drop(indexdrop) # drops basically the first of each


# %%

df_kappas = df_kappas[['from','to','kappa','cosine', 'quarter']]

## fill these with zeroes where there are NAs
df_firm['blackrock'] = df_firm['beta_BlackRock']
df_firm['vanguard'] = df_firm['beta_Vanguard']
df_firm['statestreet'] = df_firm['beta_StateStreet']
# fill the NAs with zeroes like we did in original file

df_firm[['blackrock', 'vanguard', 
         'statestreet']]=df_firm[['blackrock', 'vanguard', 
                       'statestreet']].fillna(0)

## keep some variables of interest
df_firm = df_firm[['permno','quarter','saleq', 'cogsq',
                   'num_bus_seg', 'normalized_l1', 'normalized_l2',
                   'retail_share', 'price','shares_outstanding',
                   'blackrock', 'vanguard', 'statestreet'
                   ]]

df_kappas['pair']=df_kappas.groupby(['from','to']).ngroup()
df_kappas['quarter_fe']=df_kappas.groupby(['quarter']).ngroup()


# %%
# Generate new variables

df_firm['marginsq'] = (df_firm['saleq']- df_firm['cogsq'])/df_firm['saleq']

df_firm['lcap'] = np.log(df_firm['price']*df_firm['shares_outstanding'])

df_firm['div1'] = df_firm['num_bus_seg'] >= 2 ## true or false index
df_firm['div2'] = df_firm['num_bus_seg'] >= 5 ## true or false index
df_firm['div1'] = df_firm['div1'].astype(int)
df_firm['div2'] = df_firm['div2'].astype(int)
df_firm['big3'] = df_firm['blackrock'] +df_firm['vanguard'] + df_firm['statestreet']
# 'kappa' 'from' 'to' 'year' 'quarter' 'lcap' 'retail_share' 
# 'num_bus_seg' 'div1' 'div2' 'marginsq' 'normalized_l1' 'normalized_l2'
# 'beta_BlackRock' 'beta_Vanguard' 'beta_StateStreet'

#df_kappas['pair'] = df_kappas['from'] + 100000*df_kappas['to']

# %%
# Merge firm information and calcualted kappas
keep_cols=['kappa','retail_share','lcap','marginsq', 'normalized_l2', 'quarter_fe', 'pair','big3','blackrock', 'vanguard', 'statestreet']

df_merge_nas2 = pd.merge(df_kappas,df_firm, 
                    left_on=['from', 'quarter'],
                    right_on =['permno', 'quarter'], 
                    how = 'left')[keep_cols].dropna().reset_index(drop=True)

# %%
df_merge = pd.merge(df_kappas,df_firm, left_on=['from', 'quarter'],
                    right_on =['permno', 'quarter'], how = 'left')

## Regressions!
df_merge_nas = df_merge[['kappa','retail_share','lcap', 
                      'marginsq', 'normalized_l2', 'quarter', 'pair','quarter_fe',
                      'big3','blackrock', 'vanguard', 'statestreet']].dropna()

#del df_kappas, df_firm

# %%
#fe_var_cs = variables[['quarter']]
variables = df_merge_nas[['kappa','retail_share','lcap', 
                      'marginsq', 'normalized_l2',
                      'big3','blackrock', 'vanguard', 'statestreet']]

fe_var_cs = df_merge_nas[['quarter_fe']]
fe_var_ts = df_merge_nas[['pair']]
fe_var_pa = df_merge_nas[['pair', 'quarter_fe']]
# keep even singletons; deal with them in the regression

tick()
alg_cs = pyhdfe.create(fe_var_cs, drop_singletons=False)
tock()
alg_ts = pyhdfe.create(fe_var_ts, drop_singletons=False)
tock()
alg_pa = pyhdfe.create(fe_var_pa, drop_singletons=False)
tock()

tick()
resid_cs = alg_cs.residualize(variables) 
tock()
resid_ts = alg_ts.residualize(variables) 
tock()
resid_pa = alg_pa.residualize(variables) 
tock()

# %%
# Perform Regressions
# no need for fixed effects because we've already residualized everything
# already dropped rows containing NAs

pd_vars = pd.DataFrame(resid_pa, columns=['kappa','retail_share','lcap', 
                      'marginsq', 'normalized_l2',
                      'big3','blackrock', 'vanguard', 'statestreet'])

# %%


reg1 = smf.ols(formula = 'kappa ~ retail_share + lcap + marginsq + big3', data = pd_vars).fit()

reg2 = smf.ols(formula = 'kappa ~ retail_share + lcap + marginsq + normalized_l2', data = pd_vars).fit()

reg3 = smf.ols(formula = 'kappa ~ retail_share + lcap + marginsq + big3 + normalized_l2', data = pd_vars).fit()

reg4 = smf.ols(formula = 'kappa ~ retail_share + lcap + marginsq + normalized_l2 + blackrock + vanguard + statestreet', data = pd_vars).fit()


# %%
# Print Output
# NOTE: the R-squared here is TOO LOW 
# does not incorporate fixed effects
info_dict={'R\sq' : lambda x: f"{x.rsquared:.4f}",
           'N' : lambda x: f"{int(x.nobs):d}"}

dfoutput = summary_col(results=[reg1,reg2,reg3,reg4],
                            float_format='%0.4f',
                            stars = True,
                            model_names=['(1)',
                                         '(2)', 
                                         '(3)',
                                         '(4)'],
                            info_dict=info_dict,
                            regressor_order=['retail_share',
                                             'lcap',
                                             'marginsq',
                                             'big3',
                                             'normalized_l2',
                                             'blackrock',
                                             'vanguard',
                                             'statestreet'
                                             ],
                             drop_omitted=True)

tab_reg = dfoutput.as_latex()
with open(f_tab4,'w') as file:
    file.write(tab_reg)
    
print(dfoutput)