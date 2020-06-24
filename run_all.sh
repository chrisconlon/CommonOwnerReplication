#!/usr/bin/env bash
set -e

# Install Packages
pip install -r requirements.txt

## If you are in main directory with run_all.sh
# you will need to go to code to run everything
cd code


## Python block
# data generating block

python 1_Download_WRDS_Data.py
python 2_Process_WRDS_Data.py 
python 3_Calculate_Kappas.py 

# plot creating block

python plots1_basic_descriptives.py

python plots2_kappa_official.py

python plots3_big_three_four.py

python plots4_investor_similarity.py

python plots5_airlines_cereal.py

python plots6_sole_vs_shared.py

python plots7_short_interest_coverage.py

python plots8_individual_firm_coverage.py

python plots9_blackrock_vanguard.py

python plots10_kappa_comparison_appendix.py

python plots11_profit_simulations.py

# table creating block
python table3_variance_decomp.py

#python table4_kappa_correlations.py

