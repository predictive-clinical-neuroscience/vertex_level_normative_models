#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 09:47:13 2025

@author: andmar
"""

import os 
import pandas as pd
import numpy as np
from tqdm import tqdm
import subprocess

rootdir = '/project_cephfs/3022017.06'
wdir = os.path.join(rootdir,'projects','cortex_blr')
logdir = os.path.join(wdir, 'logs')
os.makedirs(logdir, exist_ok=True)

fstemplate = 'fsaverage5'
n_vertices = 10242

df_tr = pd.read_csv(os.path.join(wdir,'phenotypes', 'lifespan_big_controls_extended_tr.csv'),index_col=0)
df_te = pd.read_csv(os.path.join(wdir,'phenotypes', 'lifespan_big_controls_extended_te.csv'),index_col=0)
df_all = pd.concat((df_tr, df_te))

do_processing = False 
use_cluster = False
cmd_qsub_base = ['/home/preclineu/andmar/DCCN/Scripts/Torque/SubmitToCluster.py',
                 '-length', '10:00',
                 '-memory', '30gb',
                 '-logfiledir', logdir,
                 '-clusterspec', 'slurm'
                ]


datasets = {
    'ABCD'               : ['ABCD_10', 'ABCD_20', 'ABCD_17', 'ABCD_03', 'ABCD_06', 'ABCD_12',
                            'ABCD_19', 'ABCD_16', 'ABCD_02', 'ABCD_11', 'ABCD_08', 'ABCD_15',
                            'ABCD_18', 'ABCD_14', 'ABCD_13', 'ABCD_05', 'ABCD_01', 'ABCD_21',
                            'ABCD_07', 'ABCD_09', 'ABCD_04'],
    #'AOMIC_ID1000'       : ['AOMIC_1000'],
    #'AOMIC_PIOP2'        : ['AOMIC_PIPO2'],
    'CAMCAN'             : ['cam'],
    'IXI'                : ['ixi'],
    'HCP_S1200_processed': ['hcp_ya'], 
    'HCP_Aging'          : ['HCP_A_UM', 'HCP_A_UCLA', 'HCP_A_WU', 'HCP_A_MGH'],
    'HCP_Dev'            : ['HCP_D_WU', 'HCP_D_UCLA', 'HCP_D_MGH', 'HCP_D_UM'],
    'HCP_EP'             : ['HCP_EP_IU', 'HCP_EP_BWH', 'HCP_EP_MGH', 'HCP_EP_McL'],
    'OASIS2'             : ['Oasis2'],
    'OASIS3'             : ['Oasis3'],    
    'PING'               : ['ping_c', 'ping_i', 'ping_f', 'ping_a',  'ping_d', 'ping_h', 'ping_j'],    
    'PNC'                : ['pnc'],
    'TOP'                : ['top'],
    'UKB'                : ['ukb-11027.0', 'ukb-11025.0']
    }
    
#idx = df_all.loc[df_all['site'] == 'hcp_ya'].index
#df_all.index[idx] =idx.str.strip('sub-')

N = 0
for ds in datasets:
    for site in datasets[ds]:
        n = len(df_all.loc[df_all['site'] == site])
        #print(ds, site, n)
        N = N + n
print('Total:', N)

X = np.zeros((N, n_vertices))
c = 0
for ds in datasets: #['ABCD']: #datasets:
    fsdir = os.path.join(rootdir, ds, 'freesurfer')

    for site in datasets[ds]:
        print('dataset:', ds, site)
        data = 0
        nodata =0
        for sub in tqdm(df_all.loc[df_all['site'] == site].index):
            cmd = []
            # hack to fix the subject names 
            if site == 'hcp_ya' or ds == 'HCP_EP':
                sub = sub.strip('sub-')
            if ds == 'PING':
                sub = sub + '_1'
            
            subdir = os.path.join(fsdir,str(sub))
            if not os.path.exists(subdir):
                #print(sub, 'does not exist')
                nodata += 1
                continue
            else:
                data += 1
            for hem in ['lh','rh']:
                for meas in ['thickness', 'area']:
                    outfile = os.path.join(subdir,'surf',f'{hem}.{meas}.{fstemplate}')
                    cmd_p = ['module load freesurfer ;',
                             f'export SUBJECTS_DIR={fsdir} ; '
                             'mris_preproc',
                             '--s', str(sub),
                             '--hemi', hem,
                             '--meas', meas,
                             '--target', fstemplate,
                             '--out', outfile + '.mgz'
                             ';'
                             ]
                    cmd_c = ['mri_convert --ascii',
                             outfile + '.mgz',
                             outfile + '.asc',
                             ';']
                    if not os.path.exists(outfile + '.asc'):                        
                        cmd += cmd_p 
                        cmd += cmd_c

            log_name = '_'.join((site,str(sub)))
            cmd_str = f'"{' '.join(cmd)}"'
            cmd_qsub = cmd_qsub_base + ['-name', log_name,
                                        '-command', str(cmd_str),
                                        '-clusterspec','slurm',
                                        '-scriptfile', os.path.join(wdir,'tmpscripts',log_name + '.sh')
                                        ]
            if do_processing and len(cmd) > 0:
                if use_cluster:
                    subprocess.Popen(' '.join(cmd_qsub), shell=True)
                else:
                   subprocess.call(' '.join(cmd), shell=True) 

        print('data:',data,'nodata:',nodata)

#mris_preproc --s 100307 --hemi lh  --meas thickness --target fsaverage5 --out ./lh.thickness.fsaverage5.mgz
#mri_convert lh.thickness.fsaverage5.mgz lh.thickness.fsaverage5.asc --ascii