import pandas as pd
import numpy as np
from utilities.matlab_util import matlab_sparse
from utilities.groupby import applyParallel
from sklearn.metrics.pairwise import cosine_similarity, manhattan_distances


def fix_scrape_cols(df):
    # fix the names in the scraped data
    df=df.set_index(['from','to','quarter'])
    df.columns = ['s'+x for x in df.columns]
    return df.reset_index()

def process_beta(fn):
    df=pd.read_parquet(fn)
    df['mgrno']=df['mgrno'].astype(int)
    return df[(df.permno_drop==False)&(df.sharecode_drop==False) & (df.beta<0.5)]


# This is the main function
def beta_to_kappa(df):
    df=df[(df.quarter>='1980-01-01')]

    df.loc[df.price<0,'price']=0
    df['mkt_cap']=df['shares_outstanding'] *df['price']
    df_m=df.groupby(['permno','quarter'])['mkt_cap'].median()

    total_df=applyParallel(df.groupby(['quarter']), do_one_period)
    total_df2=applyParallel(df.groupby(['quarter']), do_one_l1)
    total_df3=applyParallel(df[(df.quarter>='1999-01-01')].groupby(['quarter']), do_one_robustness)

    # merge and clean up missings
    total_df=pd.merge(total_df,total_df2,on=['quarter','from','to'],how='left').reset_index(drop=True)
    total_df=pd.merge(total_df,total_df3,on=['quarter','from','to'],how='left').reset_index(drop=True)
    total_df[['kappa','kappa_CLWY','kappa_pow2','kappa_pow3','kappa_sqrt','cosine','kappa_sole','kappa_soleshared']]=total_df[['kappa','kappa_CLWY','kappa_pow2','kappa_pow3','kappa_sqrt','cosine','kappa_sole','kappa_soleshared']].fillna(0)

    # Add the market cap
    total_df=pd.merge(pd.merge(total_df,
        df_m,left_on=['from','quarter'],right_on=['permno','quarter']),
        df_m,left_on=['to','quarter'],right_on=['permno','quarter']
        ).rename(columns={'mkt_cap_x':'mkt_cap_from','mkt_cap_y':'mkt_cap_to'})
    
    return total_df

def do_one_robustness(df):
	[betas_soleshared,mgr_keys,permno_keys]=matlab_sparse(df.mgrno,df.permno,df.beta_soleshared,compress=False)
	[betas_sole,mgr_keys,permno_keys]=matlab_sparse(df.mgrno,df.permno,df.beta_sole,compress=False)

	[betas,mgr_keys,permno_keys]=matlab_sparse(df.mgrno,df.permno,df.beta,compress=False)

	kappa_sole=raw_kappa(betas,betas_sole)
	kappa_soleshared=raw_kappa(betas,betas_soleshared)
	kappa_all=raw_kappa(betas,betas)
	#kappa_drop=raw_kappa(betas_drop,betas_drop)

	idx=kappa_all.nonzero()
	out_df=pd.DataFrame({'from':permno_keys[idx[0]],'to':permno_keys[idx[1]],'kappa_all':kappa_all[idx].flatten(),
	'kappa_sole':kappa_sole[idx].flatten(), 'kappa_soleshared':kappa_soleshared[idx].flatten()})
	out_df['quarter'] = df.quarter.iloc[0]
	return out_df

	
def beta_to_kappa_merger_breakup(df):
    return applyParallel(df.groupby(['quarter']), do_one_merger_breakup)

# handler for L1 Measure
# input: long dataframe of Manager, Firm, Beta_fs
# Output: long dataframe of Quarter, Firm_from, Firm_to, L1 Distance
def do_one_l1(df):
    [betas,mgr_keys,permno_keys]=matlab_sparse(df.mgrno,df.permno,df.beta)
    l1_measure=calc_l1_measure(betas)

    idx=l1_measure.nonzero()
    out_df=pd.DataFrame({'from':permno_keys[idx[0]],'to':permno_keys[idx[1]],'l1_measure':l1_measure[idx].flatten()})
    out_df['quarter'] = df.quarter.iloc[0]
    return out_df




def do_one_merger_breakup(df2):
    # breakup in three blocks
    blockA=df2.loc[~df2['InvestorName'].isnull(),['mgrno','permno','beta']]
    blockB=df2.loc[df2['InvestorName'].isnull(),['mgrno','permno','beta']]
    blockA.beta=0.5*blockA.beta
    blockC=blockA.copy()
    blockC.mgrno=-blockC.mgrno
    df3=pd.concat([blockA,blockB,blockC],axis=0,ignore_index=True)

    # first do the regular case
    [betas,mgr_keys,permno_keys]=matlab_sparse(df2.mgrno,df2.permno,df2.beta)
    k1=calc_kappa(betas)

    # now do the breakup case using the augmented data
    [betas_b,mgr_keys_b,permno_keys_b]=matlab_sparse(df3.mgrno,df3.permno,df3.beta)
    k2=calc_kappa(betas_b)

    df4=df2.groupby(['mgrno_merger','permno']).sum().reset_index()
    # finally do the merger using the merger mgrno's instead of the real ones
    [betas_m,mgr_keys_m,permno_keys_m]=matlab_sparse(df4.mgrno_merger,df4.permno,df4.beta)
    k3=calc_kappa(betas_m)

    # Ignore BlackRock+Vanguard
    df4=df2[~(df2['InvestorName'].isin(['BlackRock','Vanguard']))]
    [betas_drop,mgr_keys_drop,permno_keys_drop]=matlab_sparse(df4.mgrno,df4.permno,df4.beta,compress=False)
    k4=calc_kappa(betas_drop)

    # put it all together and return
    idx=k1.nonzero()
    out_df=pd.DataFrame({'from':permno_keys[idx[0]],'to':permno_keys[idx[1]],'kappa':k1[idx].flatten(),
        'kappa_breakup':k2[idx].flatten(), 'kappa_merger':k3[idx].flatten(),'kappa_drop':k4[idx].flatten()})
    out_df['quarter'] = df2.quarter.iloc[0]
    return out_df

# handler for L2 Measures (Rotemberg Weights, CLWY Weights, etc.)
# input: long dataframe of Manager, Firm, Beta_fs
# Output: long dataframe of Quarter, Firm_from, Firm_to, kappa_fg, ihhi_f, ihhi_g, cosine_fg  
def do_one_period(df):
    [betas,mgr_keys,permno_keys]=matlab_sparse(df.mgrno,df.permno,df.beta)
    kappa=calc_kappa(betas)
    kappa2=calc_kappa(betas,2)
    kappa3=calc_kappa(betas,3)
    kappa4=calc_kappa(betas,0.5)
    kappa5=calc_kappa(betas,'CLWY')
    cosine=cosine_similarity(betas.transpose())

    idx=kappa.nonzero()
    out_df=pd.DataFrame({'from':permno_keys[idx[0]],'to':permno_keys[idx[1]],'kappa':kappa[idx].flatten(),
        'kappa_pow2':kappa2[idx].flatten(), 'kappa_pow3':kappa3[idx].flatten(), 'kappa_sqrt':kappa4[idx].flatten(),
        'kappa_CLWY':kappa5[idx].flatten(),'cosine':cosine[idx].flatten()})
    out_df['quarter'] = df.quarter.iloc[0]
    return out_df

	
# This does the work for L1 measure
# Input beta: S x F matrix
# Output L1: F x F matrix
# Subtract beta_f from each column of beta and sum of absolute deviations, stack for L1.
def calc_l1_measure(betas):
    y=manhattan_distances(betas.transpose())
    tot=betas.sum(axis=0)
    return (-y+tot[np.newaxis,:]+tot[:,np.newaxis])/2

# Calculate Summary Stats of Control Weights
# Compute Convex Power Gamma:
# CHHI: Control HHI
# IHHI: Investor HHI
# Retail Share
#
# This is the main function that takes a DF of betas and calculates all of the CHHI measures
def calc_chhis(df):
    # apply to multiple groups here
    df['inv_total']=df.groupby(['mgrno','quarter'])['beta'].transform(sum)
#    y=applyParallel(df[['permno','quarter','beta','inv_total']].groupby(['permno','quarter']), agg_chhi)
    y=df[['permno','quarter','beta','inv_total']].groupby(['permno','quarter']).apply(agg_chhi)
    x=df.groupby(['permno','quarter']).agg({'shares_outstanding':np.max,'price':np.median})
    return pd.merge(x,y,left_index=True,right_index=True,how='outer')

# this is unitary function that takes in a vector Beta_f that is S x 1
def chhi(beta,power):
    gamma=(beta**power)
    # scalar adjustment factor
    adj=10000*((beta.sum()/gamma.sum())**2)
    return (gamma**2).sum()*adj

# This calculates all of the CHHI measures and returns a (horizontal) series
def agg_chhi(x):
    out=[chhi(x['beta'],a) for a in [0.5,1,2,3,4]]
    tmp=x['beta']/x['inv_total']
    clwy=chhi(tmp,1)
    clwy_alt=10000*(tmp**2).sum() 

    names = {
        'retail_share': 1-x['beta'].sum(),
        'chhi_05': out[0],
        'ihhi': out[1],
        'chhi_2': out[2],
        'chhi_3': out[3],
        'chhi_4': out[4],
        'chhi_clwy': clwy,
        'chhi_clwy2': clwy_alt
        }
    return pd.Series(names, index=['retail_share','ihhi','chhi_05','chhi_2','chhi_3','chhi_4','chhi_clwy','chhi_clwy2'])


# This calculates profit weights
#
# Input beta: S x F matrix
# Output kappa: F x F matrix
# Options: Gamma 'CLWY', 'default' (Rotemberg), numeric: convexity parameter "a" for gamma=beta^a
def calc_kappa(betas,gamma_type='default'):
    # CLWY normalize the gammas
    if gamma_type=='CLWY':
        gamma=betas/np.maximum(betas.sum(axis=1),1e-10)[:,None]
    elif isinstance(gamma_type, (int, float)):
        if gamma_type >0:
            tmp=betas**(gamma_type)
            gamma=tmp#*(betas.sum(axis=0)/tmp.sum(axis=0))
        else:
            print("Must provide Positive Parameter")
    # proportional control: do we normalize to sum to one?
    else:
        gamma=betas #/betas.sum(axis=0)
    
    return raw_kappa(betas,gamma)

def raw_kappa(betas,gamma):
    # F x F matrix
    numer=gamma.T @ betas
    # F x 1 vector
    denom=np.diag(numer)
    # this is a F x F  matirx
    return numer/denom[:,None]
