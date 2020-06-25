from our_plot_config import derived_dir, raw_dir
import pandas as pd

from kappas import process_beta, beta_to_kappa, calc_chhis, fix_scrape_cols
from investors import compute_investor_info, calc_big4, do_one_firm_similarity
from firminfo import regression_merge, firm_info_merge, kappa_in_out

from utilities.quantiles import weighted_quantile

## Inputs
# Betas
f_betas = derived_dir / '13f_sp500_frankenbeta.parquet'
f_betas_tr = derived_dir /'13f_sp500_unfiltered.parquet'
f_betas_sc = derived_dir /'13f_scraped.parquet'

# Other inputs
f_names_expanded = derived_dir / 'expanded_names.parquet'
f_comp_info = derived_dir / 'compustat_info.parquet'
f_big4= raw_dir /'big4.csv'

## Outputs
# main outputs (kappas)
f_kappas = derived_dir   /'official-kappas.parquet'
f_kappas_tr = derived_dir/'appendix_kappa_tr.parquet'
f_kappas_scrape = derived_dir/ 'appendix_kappa_scrape.parquet'
f_kappas_combined = derived_dir /'appendix_kappa_combined.parquet'

# Firm and Investor Output
f_investor_info =derived_dir /'investor-info.parquet'
f_firm_info =derived_dir / 'firm-info.parquet'
f_regression = derived_dir / 'regression_data.parquet'

### Calculate $\kappa$ for combined $\beta$ (Frankenstein version)
# - Apply the $\kappa$ calculations period by period
# - This includes (L2, L1, Sole/Shared, and various options for gamma)
# - Save the output to a new parquet file
df = process_beta(f_betas)
df_kappa=beta_to_kappa(df)
df_kappa.to_parquet(f_kappas,compression='brotli')

### Calculate alternate Kappas (these are for Appendix)
# - Apply $\kappa$ calculations period by period
# - Do this for the pure TR data and pure scrape data
total_dft=beta_to_kappa(process_beta(f_betas_tr))
total_dfs=beta_to_kappa(process_beta(f_betas_sc))
final_df=pd.merge(total_dft,fix_scrape_cols(total_dfs),on=['from','to','quarter'],how='outer')

total_dft.to_parquet(f_kappas_tr,compression='brotli')
total_dfs.to_parquet(f_kappas_scrape,compression='brotli')
final_df.to_parquet(f_kappas_combined,compression='brotli')

# save some memory
del total_dft, total_dfs, final_df

# Investor Info: How indexed is each manager? (including big4 information)
df_investor=compute_investor_info(df,f_big4)
df_investor.to_parquet(f_investor_info,compression='brotli')

### Do the Firm-Level Descriptives 
# - Build the fundamentals, names, and business segments for all S&P entries
# - Compute the firm level similarity measure
# - Compute CHHI, IHHI from Betas
# - Combine everything in the firm (permno-quarter) info file
# - Write the file for regressions (merged firm info and kappa)

df_fund2=pd.read_parquet(f_comp_info)
df_names2=pd.read_parquet(f_names_expanded)
firm_similarity=df.groupby(['quarter']).apply(do_one_firm_similarity).reset_index(drop=True)
big4=calc_big4(df,pd.read_csv(f_big4))
chhi=calc_chhis(df)

df_firm2=firm_info_merge(df_names2,df_fund2,firm_similarity,big4,chhi)
df_firm2.to_parquet(f_firm_info,compression='brotli')

df_reg=regression_merge(df_kappa,df_firm2)
df_reg.to_parquet(f_regression, compression='brotli')

# add in-bound and outbound kappa --this isn't in final draft of paper
# df_firm3=kappa_in_out(df_kappa,df_firm2)