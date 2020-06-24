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
f_tab4 = tab_dir   /'table4.tex'

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

# Adjust R^2 for the FE
def rsq_update(reg):
    reg.rsquared=np.var(reg.predict()+(df2['kappa'].values-resid_pa[:,0]))/np.var(df2['kappa'])
    return
for r in [reg1,reg2,reg3,reg4]:
    rsq_update(r)


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
                            regressor_order=[('retail_share','Retail Share'),
                                             ('lcap','Log(Market Cap)'),
                                             ('marginsq','Operating Margin'),
                                             ('normalized_l2','Indexing'),
                                             ('big3','Big 3 Share'),
                                             ('blackrock','BlackRock'),
                                             ('vanguard','Vanguard')
                                             ('statestreet','StateStreet')
                                             ],
                             drop_omitted=True)

# Clean up the TeX by hand for the table
tab_reg2=re.sub(r'\*\*\*', '*', dfoutput.as_latex())
tab_reg3=re.sub(r'hline','toprule', tab_reg2,count=1)
tab_reg4=re.sub(r'hline','bottomrule', tab_reg3,count=1)
tab_reg5=re.sub(r'retail\\_share','Retail Share', tab_reg4)


# Display table and save
print(tab_reg5)
with open(f_tab4,'w') as file:
    file.write(tab_reg5)
    



