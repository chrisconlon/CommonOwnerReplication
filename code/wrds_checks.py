import pandas as pd
import numpy as np
import os,sys
import matplotlib
import matplotlib.pyplot as plt
from wrds_cleaning import expand_splist
#
# These are checks (WRITE MORE)
#

def check_bigbeta(df,fn):
    df[df.beta > 0.5].to_excel(fn)
    return

def check_s34(df, f_shares_out, f_prc_zero, f_duplicates):
    df[df.shrout1==0].to_excel(f_shares_out)
    df[df.prc==0].to_excel(f_prc_zero)
    df[df[['permno','fdate','rdate','mgrno']].duplicated(keep=False)].to_excel(f_duplicates)
    return

def check_names(df_sp500,df_names, f_names_missing):
    df_spe=expand_splist(df_sp500)
    x=pd.merge(df_spe,df_names,on=['permno'],how='left')
    x=x[(x.qdate >= x.namedt) & (x.qdate <= x.nameenddt)]
    y=pd.merge(df_spe,x,on=['permno','qdate'],how='left')
    y=y[y.ticker.isnull()][['permno','qdate']]
    pd.merge(y,df_names,on=['permno']).to_excel(f_names_missing)

def check_blackrock(df, fig_blackrock1, fig_blackrock2,fig_blackrock3):
    blackrock=blackrock_fix(df[df.mgrname.str.contains('BLACKROCK')].copy())
    blackrock['aum'] = blackrock['prc'] * blackrock['shares']
    blackrock[blackrock.rdate==blackrock.fdate].groupby('rdate')['aum'].sum().plot(figsize=(20,10),title="RDATE==FDATE")
    plt.savefig(fig_blackrock1)
    blackrock.groupby(['rdate'])['aum'].sum().plot(figsize=(20,10),title="BY RDATE")
    plt.savefig(fig_blackrock2)
    blackrock.groupby(['fdate'])['aum'].sum().plot(figsize=(20,10),title="BY FDATE")
    plt.savefig(fig_blackrock3)
    return blackrock

def check_s34_coverage(df,df_sp500,df_names, f_s34_coverage):
    totals=df.groupby(['permno','quarter'])['mgrno'].nunique().reset_index()
    x=pd.merge(expand_splist(df_sp500),totals,left_on=['permno','qdate'],right_on=['permno','quarter'],how='left')
    y=x[x.mgrno.isnull()][['permno','qdate']]
    z=pd.merge(y,df_names,on=['permno'],how='left')
    z[(z.qdate <= z.nameenddt)&(z.qdate >= z.namedt)].to_excel(f_s34_coverage)

def check_multiple_cusip(df, f_multiple_cusips, f_multiple_cusips_summary):
    x=df.groupby(['permno','cusip','rdate'])['shares'].sum()
    y=x[x.groupby(level=[0,2]).transform('count')>1].reset_index().sort_values(['rdate','permno','shares'])
    y['share_pct']=y['shares']/y.groupby(['permno','rdate'])['shares'].transform(sum)
    y.sort_values(['permno','rdate','cusip']).to_excel(f_multiple_cusips)
    z=y.groupby(['permno','rdate'])['share_pct'].min().sort_values().reset_index().to_excel(f_multiple_cusips_summary)

def check_fundamental_coverage(df,df_fund2,df_names2, f_missing_betas, f_missing_atq, f_missing_segments):
    df3=df[['permno','quarter']].drop_duplicates().reset_index(drop=True)
    df3['betas_observed']=1
    x=pd.merge(df3,df_fund2,on=['permno','quarter'],how='outer').sort_values(['quarter','permno'])
    y1=x[x.betas_observed.isnull()]
    pd.merge(y1[['permno','quarter']].drop_duplicates(),df_names2).sort_values(['permno','quarter']).to_excel(f_missing_betas)
    pd.merge(x[x['atq'].isnull()],df_names2,on=['permno','quarter']).sort_values(['permno','quarter']).to_excel(f_missing_atq)
    pd.merge(x[x['num_bus_seg'].isnull()],df_names2,on=['permno','quarter']).sort_values(['permno','quarter']).to_excel(f_missing_segments)


