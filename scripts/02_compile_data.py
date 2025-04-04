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

datadir = '/project_cephfs/3022017.06'
rootdir = '/project/4180000.52'
wdir = os.path.join(rootdir,'projects','cortex_blr')

fstemplate = 'fsaverage5'
n_vertices = 10242
cols = ['age','sex','site','avg_en','eTIV']
mod_to_process = ['rh_area']

df_tr = pd.read_csv(os.path.join(wdir,'phenotypes', 'lifespan_big_controls_extended_tr.csv'),index_col=0)
df_te = pd.read_csv(os.path.join(wdir,'phenotypes', 'lifespan_big_controls_extended_te.csv'),index_col=0)
df_all = pd.concat((df_tr, df_te))

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
    
N = 0
for ds in datasets:
    for site in datasets[ds]:
        n = len(df_all.loc[df_all['site'] == site])
        #print(ds, site, n)
        N = N + n
print('Total:', N)

combined_data = {
                'lh_thickness' : pd.DataFrame(data=None, columns=range(n_vertices)),
                'rh_thickness' : pd.DataFrame(data=None, columns=range(n_vertices)),
                'lh_area' : pd.DataFrame(data=None, columns=range(n_vertices)),
                'rh_area' : pd.DataFrame(data=None, columns=range(n_vertices))
                }
                                 
c = 0
for ds in datasets: #['ABCD']: #datasets:
    fsdir = os.path.join(datadir, ds, 'freesurfer')

    for site in datasets[ds]:
        print('dataset:', ds, site)
        
        filenames = {    
            'lh_thickness' : os.path.join(wdir,'data',f'{site}_lh_thickness.pkl'),
            'rh_thickness' : os.path.join(wdir,'data',f'{site}_rh_thickness.pkl'),
            'lh_area' : os.path.join(wdir,'data',f'{site}_lh_area.pkl'),
            'rh_area' : os.path.join(wdir,'data',f'{site}_lh_area.pkl')
        }
        data = {}
        datasets_exist = True 
        for k in filenames.keys():
            if os.path.exists(filenames[k]):
                data[k] = pd.read_pickle(filenames[k])
            else:
                datasets_exist = False
            
        if not datasets_exist:
            data = {
                'lh_thickness' : pd.DataFrame(data=None, columns=range(n_vertices)),
                'rh_thickness' : pd.DataFrame(data=None, columns=range(n_vertices)),
                'lh_area' : pd.DataFrame(data=None, columns=range(n_vertices)),
                'rh_area' : pd.DataFrame(data=None, columns=range(n_vertices))
            }
            subid = {
                'lh_thickness' : [],
                'rh_thickness' : [],
                'lh_area' : [],
                'rh_area' : [],
            }
            hasdata = 0
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
                    hasdata += 1
                for hem in ['lh','rh']:
                    for meas in ['thickness', 'area']:
                        infile = os.path.join(subdir,'surf',f'{hem}.{meas}.{fstemplate}.asc')
            
                        if os.path.exists(infile):                        
                            x = pd.read_csv(infile, header=None)
                            phenotype = '_'.join((hem,meas))
                            data[phenotype] = pd.concat((data[phenotype],
                                                         x.transpose()))
                            subid[phenotype].append(sub)
            print('data:',hasdata,'nodata:',nodata)
            print('Saving output ...')
    
            data['lh_thickness'].index = subid['lh_thickness']
            data['rh_thickness'].index = subid['rh_thickness']
            data['lh_area'].index = subid['lh_area']
            data['rh_area'].index = subid['rh_area']
    
            data['lh_thickness'].to_pickle(os.path.join(wdir,'data',f'{site}_lh_thickness.pkl'))
            data['rh_thickness'].to_pickle(os.path.join(wdir,'data',f'{site}_rh_thickness.pkl'))
            data['lh_area'].to_pickle(os.path.join(wdir,'data',f'{site}_lh_area.pkl'))
            data['rh_area'].to_pickle(os.path.join(wdir,'data',f'{site}_lh_area.pkl'))
            
        print('All necessary dataframes have been computed.')
        for k in mod_to_process: #data.keys():
            dfs = df_all.join(data['lh_area'], how='inner')
            combined_data[k] = pd.concat((combined_data[k],dfs))

for k in mod_to_process:
    combined_data[k].to_pickle(os.path.join(wdir,'data',f'{k}.pkl'))
        
