# problems
from our_plot_config import checks_dir
import pandas as pd
import numpy as np
f_no_permnos = checks_dir / 's34_nopermno.xlsx'

idx=['permno','cusip','mgrno','rdate']
data=['fdate','shares','prc','shrout1','shrout2','sole','shared','no']

#
# Use CRSP names file to construct mapping from CUSIP/NCUSIP --> Permno
# - Mapping should be N-->1 unique
#
def make_cusip_list(df_names):
    cusip_list=pd.concat([df_names[['ncusip','permno']].drop_duplicates().rename(columns={'ncusip':'cusip'}), df_names[['cusip','permno']].drop_duplicates()]).drop_duplicates()
    x=cusip_list.groupby('cusip')['permno'].count()
    if x.max()==1:
        print("CUSIP to Permno mapping unique")
    else:
        print("CUSIP to Permno mapping not unique")
        x=cusip_list.groupby('cusip').count()
        print(x[x>1])
    return cusip_list

####
# S-34 Cleaning
###
# This is S34 Data cleaning/merging
# - df is always the S-34 data
# NB: edited this to include Barclays per Mike's note on 2004 -- MRB

def blackrock_fix(df):
    # Use Fdates instead of rdates for BlackRock Inc only
    df['blackrock_fix']=False
    df.loc[(df.mgrno.isin([7900,9385]))&(df.rdate!=df.fdate),'blackrock_fix'] =True
    df.loc[(df.mgrno.isin([7900,9385])),'rdate'] = df.loc[(df.mgrno.isin([7900,9385])),'fdate']
    return df

def read_s34(fn):
    df=pd.read_parquet(fn,columns=['fdate', 'mgrno', 'rdate', 'cusip', 'shares', 'sole','shared', 'no', 'prc', 'shrout1','shrout2'])
    df['cusip']=df['cusip'].astype('category')
    return blackrock_fix(df)

# Merge to get only the Permo Quarters for S&P 500
def filter_s34(df,df_sp):
    return pd.merge(df,df_sp,left_on=['cusip','rdate'],right_on=['cusip','qdate'],how='inner').drop(columns=['qdate'])

def get_sp_quarters(df_sp500,cusip_list):
    x=pd.merge(expand_splist(df_sp500),cusip_list,on=['permno'])
    x['cusip']=x['cusip'].astype('category')
    return x.dropna()


## Consolidate managers (various BlackRock entities, FMR, etc.)
def consolidate_mgrs(main_df,f_consolidation):
    mgr_consolidations=pd.read_csv(f_consolidation)
    merged=pd.merge(main_df,mgr_consolidations,left_on=['mgrno'],right_on=['mgrno_from'],how='left')
    # don't consolidate these
    part1=merged.loc[merged.mgrno_to.isnull(),main_df.columns]
    # consolidate these
    x=merged[~merged.mgrno_to.isnull()].copy()
    x['mgrno']=x['mgrno_to']
    part2=x.drop(columns=['mgrno_from','mgrno_to']).groupby(['permno','cusip','mgrno','quarter','fdate']).agg({'shares':sum,'prc':max,'shrout1':max,'shrout2':max,'sole':sum,'shared':sum,'no':sum,'share_split':max}).reset_index()
    return pd.concat([part1,part2],axis=0)

#
# Use f_drops (file) to tag observations
def add_drops(df,f_drops,df_names2):
    keep_cols= list(set(list(df.columns) + ['no_managers','permno_drop','sharecode_drop']))
    # count number of managers
    df['no_managers']=df.groupby(['permno','quarter'])['mgrno'].transform('nunique')
    # dual class shares
    drops=pd.read_csv(f_drops)
    drops['start'] = pd.to_datetime(drops['start'])
    drops['end'] = pd.to_datetime(drops['end'])
    df=pd.merge(df,drops,on=['permno'],how='left')
    df.loc[(df['quarter']>=df['start'])&(df['quarter']<=df['end']),'permno_drop']=True

    df=pd.merge(df,df_names2[['permno','quarter','shrcd']].drop_duplicates(), on=['permno','quarter'])
    # ADR's and REITs
    df.loc[~df.shrcd.isin([10,11,12,18]),'sharecode_drop']=True
    df[['permno_drop','sharecode_drop']]=df[['permno_drop','sharecode_drop']].fillna(False)
    return df[keep_cols]

#
# Use MSF data to add a stock split dummy to each 13-F filing date
#
def add_stock_splits(df,df_msf):
    merged=pd.merge(
        pd.merge(df[idx+data],
            df_msf[['permno','qdate','cfacshr']],left_on=['permno','rdate'],right_on=['permno','qdate'],how='left'),
            df_msf[['permno','qdate','cfacshr']],left_on=['permno','fdate'],right_on=['permno','qdate'],how='left').drop(columns=['qdate_x','qdate_y'])
    merged.loc[(merged.cfacshr_x!=merged.cfacshr_y)&(~merged.cfacshr_x.isnull())&(~merged.cfacshr_y.isnull()),'is_split']=1
    merged['is_split'].fillna(0,inplace=True)
    merged['share_split']=merged.groupby(['permno','mgrno','rdate'])['is_split'].transform(max)
    return merged

def construct_fundamentals(df_fund,df_names2):
    return pd.merge(df_names2[['permno','quarter']],df_fund.rename(columns={'datadate':'quarter'}),on=['permno','quarter'],how='left')\
    [['permno','quarter','oibdpq','atq','niq','saleq','cogsq']].drop_duplicates()

def construct_bus_segments(df_seg,df_sp500):
    df_spe=expand_splist(df_sp500).rename(columns={'qdate':'quarter'})
    z=pd.merge(df_spe,df_seg.groupby(['permno','quarter'])['stype'].max().reset_index(),on=['permno','quarter'],how='left').sort_values(['permno','quarter'])
    z['num_bus_seg']=z.groupby(['permno'])['stype'].ffill().bfill()
    return z[['permno','quarter','num_bus_seg']].copy()

def expand_names(df_names,df_sp500):
    x=pd.merge(expand_splist(df_sp500),df_names,on=['permno'])
    return x[(x['qdate']>= x['namedt'])& (x['qdate'] <= x['nameenddt'])].drop(columns=['namedt','nameenddt','st_date','end_date','final_date']).rename(columns={'qdate':'quarter'})

def expand_splist(df_sp):
    df_sp['key']=0
    alldates=pd.DataFrame({'qdate':pd.date_range('01-01-1980',pd.to_datetime('today'),freq='Q')})
    alldates['key']=0
    x=pd.merge(df_sp,alldates,on='key')
    return x[(x['qdate'] >= x['start'])& (x['qdate'] <= x['ending'])][['permno','qdate']]
#
# Return dataset with single fdate associated with each rdate
# Need split data in the S-34 data
#    - 24,432,318 Obs have single observation
#    -  2,608,149 Obs have multiple filings with same shares (different prices)
#    - 84,159 Obs have a known share split: take the first filing (before share split)
#    - 44,874 Obs have no known share split: take the last filing (assume these are corrections)
def dedup_s34(df):
    # keep these fields only
    data2=data+['share_split']

    dups=df[idx+data+['share_split']].duplicated(subset=idx,keep=False)
    dups_df=df.loc[dups,idx+data2]
    dups_df['min_shares']=dups_df.groupby(idx)['shares'].transform(min)
    dups_df['max_shares']=dups_df.groupby(idx)['shares'].transform(max)
    dups_df['min_price']=dups_df.groupby(idx)['prc'].transform(min)
    dups_df['max_price']=dups_df.groupby(idx)['prc'].transform(max)

    # These have one observation per rdate
    part1=df.loc[~dups,idx+data2]
    # All fdates have the same shares
    part2=dups_df.loc[dups_df.min_shares ==dups_df.max_shares,idx+data2].groupby(idx).last().reset_index()

    # Choosing different Fdates gives different answers -- these are more challenging
    problems=dups_df[dups_df.min_shares !=dups_df.max_shares]
    problems=problems.sort_values(idx+['fdate'])

    # If a split, take the first fdate for each rdate (usually fdate==rdate)
    part3=problems[problems.share_split==1].groupby(idx).first()[data2].reset_index()

    # If a not split, take the last fdate for each rdate (this is riskier)
    part4=problems[problems.share_split==0].groupby(idx).last()[data2].reset_index()
    print("Removing duplicate Fdates within each Rdate...")
    print("Observations with one fdate per rdate: ", len(part1))
    print("Observations with multiple fdates but same shares: ", len(part2))
    print("Observations with known split (take first): ", len(part3),len(problems[problems.share_split==1]))
    print("Other observations (update?) (take last): ", len(part4),len(problems[problems.share_split==0]))

    return pd.concat([part1,part2,part3,part4]).rename(columns={'rdate':'quarter'})

## Merge the CRSP MSF data to the S-34 (13F) data
# - Use MSF data for price and shares out when available
# - Otherwise use median self-reported 13-F values
# - Use shrout2 before shrout1
# - Calculate the betas
def compute_betas(df,df_msf):
    print('Before 99 Missings:\n', df[(df.quarter < '1999-01-01')][['sole','shared','no','shares']].isnull().mean())
    print('After 99 Missings:\n', df[(df.quarter > '1999-01-01')][['sole','shared','no','shares']].isnull().mean())
    df.loc[:,['no','sole','shared']].fillna(0,inplace=True)

    y=pd.merge(df,df_msf[['permno','qdate','prc','shrout']],left_on=['permno','quarter'],right_on=['permno','qdate'],how='left')
    y.loc[:,['shrout1','shrout2','shrout']]=y[['shrout1','shrout2','shrout']].replace(0,np.nan)

    y['med_price']=y.groupby(['permno','quarter'])['prc_x'].transform(np.median)
    y['med_shares']=y.groupby(['permno','quarter'])['shrout2'].transform(np.median).combine_first(1e3*y.groupby(['permno','quarter'])['shrout1'].transform(np.median))
    y[['shared','no','sole']]=y[['shared','no','sole']].fillna(0)

    y['price']=y['prc_y'].combine_first(y.med_price)
    y['shares_outstanding'] = y['shrout'].combine_first(y.med_shares)
    y=alt_betas(y)
    return y[['permno','mgrno','quarter','shares','shares_outstanding','price','beta','beta_sole','beta_soleshared','sole','shared','no']]

def process_scraped(fn_scrape,fn_big4):
    df = pd.read_parquet(fn_scrape)
    df['quarter']=pd.to_datetime(df.rdate, format='%Y%m%d')
    df=df.rename(columns={'prc':'price','none':'no'}).drop(columns=['rdate'])
    return alt_betas(pd.merge(df,pd.read_csv(fn_big4),how='left',on=['mgrno']))

# Compute the betas : shares / 1000 x Shares Outstanding
# Compute sole+shared and sole as well (only valid post 99)
def alt_betas(y):
    y['beta']  = y['shares']/ (1e3*y['shares_outstanding'])
    y['beta_soleshared']  = (y['shares']-y['no'])/ (1e3*y['shares_outstanding'])
    y['beta_sole']  = (y['shares']-y['no']-y['shared'])/ (1e3*y['shares_outstanding'])
    return y

# Combine betas
def combine_betas(df,dfs,cut_date='2000-01-01'):
    cols = df.columns
    return pd.concat([df[df.quarter<=cut_date],dfs.loc[dfs.quarter>cut_date,cols]],axis=0,ignore_index=True)
