#%% config
import os
from pcntoolkit.normative_parallel import execute_nm, collect_nm, delete_nm

# globals
root_dir = '/project/4180000.52/projects/cortex_blr/'
phenotype = 'lh_thickness'

proc_dir = os.path.join(root_dir, 'models', 'fsaverage5')
w_dir = os.path.join(proc_dir,'vertex_' + phenotype + '/')
os.makedirs(w_dir, exist_ok=True)

py_path = '/home/preclineu/andmar/sfw/anaconda3/envs/py310/bin/python'
log_path = os.path.join(root_dir, 'logs/')
job_name = 'blrw'
batch_size = 200 
memory = '30gb'
duration = '05:00:00'
alg = 'blr'
warp ='WarpSinArcsinh'
warp_reparam = 'True'

cluster = 'slurm'

resp_file_tr = os.path.join(proc_dir,f'resp_{phenotype}_tr.pkl')
resp_file_te = os.path.join(proc_dir,f'resp_{phenotype}_te.pkl')

cov_file_tr = os.path.join(proc_dir, 'cov_bspline_tr.txt')
cov_file_te = os.path.join(proc_dir, 'cov_bspline_te.txt')


#%% fit
os.chdir(w_dir)
nm_func = 'estimate'
outputsuffix = '_' + nm_func

execute_nm(processing_dir = w_dir,
           python_path = py_path, 
           job_name = job_name,
           covfile_path = cov_file_tr,
           respfile_path = resp_file_tr, 
           batch_size = batch_size,
           memory = memory,
           duration = duration,
           alg = 'blr',
           savemodel='True',
           optimizer = 'powell',
           #optimizer = 'l-bfgs-b',
           #l = '0.1',
           warp = warp, 
           warp_reparam = warp_reparam, 
           testcovfile_path = cov_file_te,
           testrespfile_path = resp_file_te,
           cluster_spec = cluster,
           log_path = log_path,
           binary = True)

#%% transfer

# note that this just does a dummy test, effectively running transfer on a  
# subset of data that were used to train the model. In practice it is necessary 
# to generate new covariate files etc. 

os.chdir(w_dir)

nm_func = 'transfer'
outputsuffix = '_' + nm_func
model_path = os.path.join(w_dir,'Models')

cov_file_txfr_tr = os.path.join(proc_dir,'cov_bspline_txfr_tr.txt')
cov_file_txfr_te = os.path.join(proc_dir,'cov_bspline_txfr_te.txt')

resp_file_txfr_tr = os.path.join(proc_dir,f'resp_{phenotype}_txfr_tr.pkl')
resp_file_txfr_te = os.path.join(proc_dir,f'resp_{phenotype}_txfr_te.pkl')

be_file_tr = os.path.join(proc_dir,'sitenum_txfr_tr.pkl')
be_file_te = os.path.join(proc_dir,'sitenum_txfr_te.pkl')

#output_path = os.path.join(w_dir,'Transfer')

execute_nm(processing_dir = w_dir, 
           python_path = py_path, 
           job_name = nm_func + job_name, 
           covfile_path = cov_file_txfr_tr,
           respfile_path = resp_file_txfr_tr, 
           batch_size = batch_size,
           memory = memory,
           duration = duration, 
           func='transfer',
           alg='blr', 
           binary=True, 
           trbefile=be_file_tr,
           savemodel='True', 
           outputsuffix=outputsuffix,
           inputsuffix='_estimate',
           #inscaler=inscaler, 
           #outscaler=outscaler,
           testcovfile_path=cov_file_txfr_te, 
           testrespfile_path=resp_file_txfr_te,
           tsbefile = be_file_te, 
           #output_path=output_path,
           model_path=model_path, 
           log_path=log_path,
           cluster_spec='slurm')



#%% collect
#collect_nm(w_dir, job_name, collect=False, binary=True)

collect_nm(w_dir,
           nm_func + job_name,
           func=nm_func, 
           collect=True, 
           binary=True, 
           batch_size=batch_size, 
           outputsuffix=outputsuffix)

#%% delete
delete_nm(w_dir, binary=True)