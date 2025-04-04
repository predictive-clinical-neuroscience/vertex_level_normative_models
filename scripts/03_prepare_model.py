import os
import pandas as pd
import numpy as np
import pcntoolkit as ptk 
from pcntoolkit.util.utils import create_design_matrix

# globals
root_dir = '/project/4180000.52/projects/cortex_blr/'
data_dir = os.path.join(root_dir,'data/')

n_vertices = 10242

phenotype = 'lh_thickness'

proc_dir = os.path.join(root_dir, 'models', 'fsaverage5')
os.makedirs(proc_dir, exist_ok=True)

# load covariates
df_file = os.path.join(data_dir, phenotype + '.pkl')

print('loading dataframe:',df_file, '...')
df = pd.read_pickle(df_file)

# remove some duplicate rows 
df.drop_duplicates(inplace=True)

# make sure the index is a string
df.index = df.index.map(str)

print('dataframe:', df.shape)

#########################
# split training and test
#########################
tr_id_file = os.path.join(proc_dir,'subid_tr.txt')
te_id_file = os.path.join(proc_dir,'subid_te.txt')

if os.path.exists(tr_id_file) and os.path.exists(te_id_file):
    print('using existing train/test split')
    
    #te_id = pd.read_csv(te_id_file, index_col=0)
    #tr_id = pd.read_csv(tr_id_file, index_col=0)
    with open(tr_id_file,'r') as f:
        tr_id = f.read().splitlines()
    with open(te_id_file,'r') as f:
        te_id = f.read().splitlines()
    
    df_tr = df.loc[df.index.isin(tr_id)]
    df_te = df.loc[df.index.isin(te_id)]
else:
    print('creating random train/test split')
    tr = np.random.uniform(size=df.shape[0]) > 0.5
    te = ~tr

    df_tr = df.iloc[tr]
    df_te = df.iloc[te]
    
    with open(tr_id_file,'w') as f:
        for l in df_tr.index.to_list():
            f.write(f'{l}\n')
    with open(te_id_file,'w') as f:
        for l in df_te.index.to_list():
            f.write(f'{l}\n')

    ######################
    # Configure covariates
    ######################
    # design matrix parameters
    xmin = 0 # boundaries for ages of participants +/- 5
    xmax = 100
    cols_cov = ['age','sex']
    site_ids =  sorted(set(df_tr['site'].to_list()))
    
    print('configuring covariates ...')
    X_tr = create_design_matrix(df_tr[cols_cov], site_ids = df_tr['site'],
                                basis = 'bspline', xmin = xmin, xmax = xmax)
    X_te = create_design_matrix(df_te[cols_cov], site_ids = df_te['site'], all_sites=site_ids,
                                basis = 'bspline', xmin = xmin, xmax = xmax)
    
    cov_file_tr = os.path.join(proc_dir, 'cov_bspline_tr.txt')
    cov_file_te = os.path.join(proc_dir, 'cov_bspline_te.txt')
    ptk.dataio.fileio.save(X_tr, cov_file_tr)
    ptk.dataio.fileio.save(X_te, cov_file_te)
    

print('tr:',len(df_tr),'te:',len(df_te))

#########################
# configure response data
#########################
cols_resp = range(n_vertices)
x_tr = df_tr[cols_resp].to_numpy()
x_te = df_te[cols_resp].to_numpy()

# remove non-zero values
x = np.concatenate((x_tr,x_te))
nz = np.where(np.bitwise_and(np.isfinite(x_tr).any(axis=0), np.var(x_tr, axis=0) != 0))[0]

# and write out as pkl
resp_file_tr = os.path.join(proc_dir,'resp_' + phenotype + '_tr.pkl')
resp_file_te = os.path.join(proc_dir,'resp_' + phenotype + '_te.pkl')
ptk.dataio.fileio.save(x_tr[:,nz], resp_file_tr)
ptk.dataio.fileio.save(x_te[:,nz], resp_file_te)

# save indices for valid vertices
with open(os.path.join(proc_dir,'valid_vertices.txt'),'w') as f:
    for s in list(nz):
        f.write(f"{s}\n")
        
###############
# save site ids
###############
site_ids = list(df_tr['site'].unique())
with open(os.path.join(proc_dir,'site_ids.txt'),'w') as f:
    for s in site_ids:
        f.write(f"{s}\n")