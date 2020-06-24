import pandas as pd

def regression_merge(df_kappas,df_firm):
	firm_cols=['permno','quarter','saleq', 'cogsq', 'normalized_l2',
                   'retail_share', 'market_cap', 'beta_BlackRock', 'beta_Vanguard', 'beta_StateStreet']
	keep_cols=['from','to','quarter','kappa','cosine','retail_share','market_cap','marginsq','saleq','cogsq', 'normalized_l2',
					 'big3','beta_BlackRock', 'beta_Vanguard', 'beta_StateStreet']

	# Read things in and Merge
	df=pd.merge(
		df_kappas.loc[(df_kappas['from']!=df_kappas['to'])&(df_kappas['quarter']<='2017-10-01'),['from','to','kappa','quarter','cosine']],
		df_firm[firm_cols],left_on=['from', 'quarter'],right_on =['permno', 'quarter'],how='left'
	).reset_index(drop=True)

	# Calculate derived columns
	df['big3'] = df['beta_BlackRock'] +df['beta_Vanguard'] + df['beta_StateStreet']
	df['marginsq'] = (df['saleq']- df['cogsq'])/df['saleq']
	return df[keep_cols]

# merge it all together
def firm_info_merge(df_names2,df_fund2,firm_similarity,big4,chhi):
	df_firm2=pd.merge(pd.merge(pd.merge(pd.merge(
        df_names2,df_fund2,on=['permno','quarter'],how='inner'),
        firm_similarity,on=['permno','quarter'],how='left'),
        big4,on=['permno','quarter'],how='left'),
        chhi,on=['permno','quarter'],how='left'
        )
	df_firm2['market_cap'] = df_firm2['shares_outstanding']*df_firm2['price']
	df_firm2[['beta_BlackRock', 'beta_Vanguard', 'beta_StateStreet', 'beta_Fidelity']]=df_firm2[['beta_BlackRock', 'beta_Vanguard', 'beta_StateStreet', 'beta_Fidelity']].fillna(0)
	return df_firm2[(df_firm2.quarter>='1980-01-01')&(df_firm2.quarter<='2017-10-01')].drop_duplicates()


### This block is for incoming and outgoing kappa
# note: not sure this made it into the paper (keep the code anyway)
def weighted_from(df):
    a1=np.ma.average(df['kappa'].values, weights=df['saleq_x'].values)
    return pd.Series({'kappa_in':a1})
def weighted_to(df):
    a1=np.ma.average(df['kappa'].values, weights=df['saleq_y'].values)
    return pd.Series({'kappa_out':a1})

def kappa_in_out(df,df_firm):
	dfk=df.loc[df['from']!=df['to'],['from','to','quarter','kappa']]
	tmp=pd.merge(pd.merge(dfk,
        df_firm[['permno','quarter','saleq']],left_on=['from','quarter'],right_on=['permno','quarter']),
        df_firm[['permno','quarter','saleq']],left_on=['to','quarter'],right_on=['permno','quarter']
        ).fillna(0)

	g1=tmp.groupby(['quarter','to']).apply(weighted_from)
	g2=tmp.groupby(['quarter','from']).apply(weighted_to)

	return pd.merge(pd.merge(df_firm,
        g1,left_on=['quarter','permno'],right_on=['quarter','to'],how='left'),
        g2,left_on=['quarter','permno'],right_on=['quarter','from'],how='left')