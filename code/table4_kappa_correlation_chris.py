#!/usr/bin/env python3
# -*- coding: utf-8 -*-
### Table 4 Correlations with Kappa

import our_plot_config
from our_plot_config import derived_dir, tab_dir
import pandas as pd
import numpy as np

# for regressions
import pyhdfe
from sklearn import datasets, linear_model
import statsmodels.formula.api as smf
from statsmodels.iolib.summary2 import summary_col

# Input
f_regression = derived_dir / 'regression_data.parquet'

# Output
f_tab4 = tab_dir   /'tab4.tex'

# Read data
cols=['from','to','quarter','kappa','retail_share','market_cap','marginsq', 'normalized_l2','big3','beta_BlackRock', 'beta_Vanguard', 'beta_StateStreet']
df=pd.read_parquet(f_regression,columns=cols).rename(columns={'beta_BlackRock':'blackrock','beta_Vanguard':'vanguard','beta_StateStreet':'statestreet'})

# Filter on dates
df = df[(df.quarter > '2000-01-01')].copy()

# Calculate derived columns
df['lcap'] = np.log(df['market_cap'])
# Code the FE first: This speeds things up to avoid type converting 13 million dates
df['pair_fe']=df.groupby(['from','to']).ngroup()
df['quarter_fe']=df.groupby(['quarter']).ngroup()


# Drop any missings
df2=df[var_list+['pair_fe','quarter_fe']].dropna()

## Regressions!
# We will need to absorb: do that first
# This is comically slow and uses 30+GB
var_list=['kappa','retail_share','lcap', 'marginsq', 'normalized_l2','big3','blackrock', 'vanguard', 'statestreet']
alg_pa = pyhdfe.create(df2[['pair_fe', 'quarter_fe']].values,drop_singletons=False)
resid_pa=alg_pa.residualize(df2[var_list].values)

# Perform Regressions
# no need for fixed effects because we've already residualized everything
# drop rows containing NAs
pd_vars = pd.DataFrame(resid_pa, columns=['kappa','retail_share','lcap', 
                      'marginsq', 'normalized_l2',
                      'big3','blackrock', 'vanguard', 'statestreet'])


reg1 = smf.ols(formula = 'kappa ~ retail_share + lcap + marginsq + big3', data = pd_vars).fit()
reg2 = smf.ols(formula = 'kappa ~ retail_share + lcap + marginsq + normalized_l2', data = pd_vars).fit()
reg3 = smf.ols(formula = 'kappa ~ retail_share + lcap + marginsq + big3 + normalized_l2', data = pd_vars).fit()
reg4 = smf.ols(formula = 'kappa ~ retail_share + lcap + marginsq + normalized_l2 + blackrock + vanguard + statestreet', data = pd_vars).fit()

# Print Output
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
#print(tab_reg)
# 

# %%
# Recalculating the R-squared
## SSRES: sum of errors of 
## SS

rsq = np.zeros((4,1))

# if the dataset is pd_vars, I get the r-squared from dfoutput
# so how can i incoroporate the fixed effects?
## reg
y = df_merge_nas[['kappa']]

x1 = df_merge_nas[['retail_share', 'lcap', 'marginsq', 'big3']]
x2 = df_merge_nas[['retail_share', 'lcap', 'marginsq' ,'normalized_l2']]
x3 = df_merge_nas[['retail_share', 'lcap', 'marginsq' ,'big3', 'normalized_l2']]
x4 = df_merge_nas[['retail_share', 'lcap', 'marginsq' ,
              'normalized_l2','blackrock', 'vanguard', 'statestreet']]



def rsq_calc(reg, x, y):
    yhat = reg.predict(x)
    yhat = pd.DataFrame(yhat.copy())
    df_rsq = pd.DataFrame(y.copy())
    df_rsq['yhat'] = yhat
    df_rsq.columns = ['y', 'yhat']
    
    df_rsq['diff'] = (df_rsq['y'] - df_rsq['yhat'])
    df_rsq['mdiff'] = (df_rsq['y'] - df_rsq['y'].mean())

    df_rsq['diffsq'] = df_rsq['diff']*df_rsq['diff']
    df_rsq['mdiffsq']  = df_rsq['mdiff']*df_rsq['mdiff']
    ssres = df_rsq['diffsq'].sum()
    sstot = df_rsq['mdiffsq'].sum()
    return 1 - ssres/sstot
    

rsq[0] = rsq_calc(reg1, x1, y)
rsq[1] = rsq_calc(reg2, x2, y)
rsq[2] = rsq_calc(reg3, x3, y)
rsq[3] = rsq_calc(reg4, x4, y)

