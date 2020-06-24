#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import our_plot_config
from our_plot_config import derived_dir, tab_dir
import pandas as pd
import numpy as np
import pyhdfe

# Input
f_regression = derived_dir / 'regression_data.parquet'

# Output
f_table3 = tab_dir /'table3.tex'

# this helper decomposes the variance
def do_decomp(x):
	var=np.nanvar(x,axis=0)
	rel_hhi=var[2]/var[0]
	similarity=var[1]/var[0]
	return np.array([similarity,rel_hhi,1.0-rel_hhi-similarity])

# Read in the Kappas: get only columns we need
df_kappas2 = pd.read_parquet(f_regression,columns=['from','to','pair_fe','quarter','quarter_fe','kappa','cosine'])

# Kappa  = cosine * IHHI ratio
df_kappas2['irat'] = df_kappas2['kappa']/df_kappas2['cosine']
# Fixed Effects
df_kappas2['pair_fe']=df_kappas2.groupby(['from','to']).ngroup()
df_kappas2['quarter_fe']=df_kappas2.groupby(['quarter']).ngroup()

# Report the size of everything
print("N of Overall Dataframe:",len(df_kappas2))
print("N Quarter FE:",len(df_kappas2.quarter_fe.unique()))
print("N Pair FE:",len(df_kappas2.pair_fe.unique()))

# Take the logs of everything and get a NumPy array
variables = np.log(df_kappas2[['kappa','cosine','irat']]).values

# Use pyhdfe for high-dimensional fixed effects absorption
# This takes 13min on my iMac
resid_cs = pyhdfe.create(df_kappas2[['quarter_fe']].values).residualize(variables)
resid_ts = pyhdfe.create(df_kappas2[['pair_fe']].values).residualize(variables)
resid_pa = pyhdfe.create(df_kappas2[['pair_fe', 'quarter_fe']].values).residualize(variables)

# Do the Variance Decomposition for each case
tab_mat=np.vstack([do_decomp(variables),do_decomp(resid_cs),do_decomp(resid_ts),do_decomp(resid_pa)])*100.0
table3=pd.DataFrame(tab_mat,index=['Raw', 'Cross-Section', 'Time-Series', 'Panel'],
	columns=['Overlapping Ownership', 'Relative IHHI', 'Covariance'])
print(table3)

# Write the latex table to disk (skip zero covariance column)
table3.iloc[:,0:2].to_latex(f_table3,float_format=lambda x: '%.2f'% x + str('%'),column_format='l cc')