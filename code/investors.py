import pandas as pd
import numpy as np
from utilities.matlab_util import matlab_sparse
from sklearn.metrics.pairwise import cosine_similarity, manhattan_distances

def compute_investor_info(df,f_big4):
    tmp=df.groupby(['quarter']).apply(do_one_investor_similarity).reset_index(drop=True)
    return pd.merge(tmp,pd.read_csv(f_big4),how='left',on=['mgrno'])

def calc_big4(df,big4):
    df2=pd.merge(df,big4,on=['mgrno'],how='inner').groupby(['quarter','permno','InvestorName'])['beta'].sum().unstack()
    df2.columns=['beta_BlackRock','beta_Fidelity','beta_StateStreet','beta_Vanguard']
    return df2[['beta_BlackRock','beta_Vanguard','beta_StateStreet','beta_Fidelity']].fillna(0)

def investor_helper(betas):
    # weights for market porfolio
    mkt=betas.sum(axis=0)/betas.sum()
    # "AUM" weights to aggregate market portfolio
    x=betas.sum(axis=1)
    aum=x/x.sum()    
    nbetas = betas/x[:,None]

    # distance to AUM weighted market portfolio
    l2=cosine_similarity(X=betas,Y=np.expand_dims(mkt, axis=0)).flatten()    
    l1=1-manhattan_distances(X=nbetas,Y=np.expand_dims(mkt, axis=0),sum_over_features=True).flatten()/2
    return(aum,l2,l1)

def do_one_investor_similarity(df):
    [betas,mgr_keys,permno_keys]=matlab_sparse(df.mgrno,df.permno,df.beta)
    # Market portfolio weights
    (aum,l2,l1)=investor_helper(betas)
    out_df=pd.DataFrame({'mgrno':mgr_keys.astype(int),'aum_weight':aum,'l2_similarity':l2,'l1_similarity':l1,'cov_aum_l1':np.cov(l1,aum)[1][0]})
    out_df['quarter'] = df.quarter.iloc[0]
    return out_df

def do_one_firm_similarity(df):
    [betas,mgr_keys,permno_keys]=matlab_sparse(df.mgrno,df.permno,df.beta)
    (aum,l2,l1)=investor_helper(betas)
    
    norm_l2=y=(l2 @ (betas/betas.sum(0)))
    norm_l1=y=(l1 @ (betas/betas.sum(0)))
    nonnorm_l2=y=(l2 @ betas)
    nonnorm_l1=y=(l1 @ betas)

    out_df=pd.DataFrame({'permno':permno_keys.astype(int),'normalized_l1':norm_l1, 'nonnormalized_l1':nonnorm_l1, 'normalized_l2':norm_l2, 'nonnormalized_l2':nonnorm_l2})
    out_df['quarter'] = df.quarter.iloc[0]
    return out_df
