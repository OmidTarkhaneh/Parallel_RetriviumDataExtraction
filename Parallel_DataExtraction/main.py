import multiprocessing 
import pandas as pd
from xml.dom import minidom
from tqdm import tqdm
from multiprocessing import Pool, TimeoutError  # for parallel computing
import numpy as np
import warnings

from GetFileList import GetFileList
warnings.filterwarnings("ignore")


import CreateAtomTypedf
import GetCartesian
import GetAtomTypes
import Collect_Final_Matrix
import generate_dzfiles
import GetFileList
import delete_cmls



# Main function
# Main func to extract all necessary items from cml files and then saving in a dataframe
def mainFunc(Formula):

    '''
     In this function we tried to apply multiprocessing on two time-consuming function named GetCartesian and
     CreateAtomTypedf. We utilized pool in multiprocessing in python. In Addition, we applied multiprocessing on
     Calculate distance function (see DataGathering.py) which is very time consuming.
    '''

    filesize = len(Filenames)
    df_cartesian = pd.DataFrame()
    dframes = []
    dfcarts = []
    lstenergy = []
    with Pool(processes=multiprocessing.cpu_count()) as pool:  # parallel computing
        for i in tqdm(range(filesize)):
            mydoc = minidom.parse(Filenames[i])

            df_Cart = pool.apply_async(GetCartesian.GetCartesian, (mydoc, Filenames[i], Formula))  # parallel
            df_Cart.wait(timeout=2)
            if df_Cart.ready():
                df_Cart = df_Cart.get(timeout=2)
                if df_Cart is not None:
                    dfcarts.append(df_Cart)
                    df_cartesian = pd.concat(dfcarts)


            getatomstype,smile, energy= GetAtomTypes.GetAtomTypes(mydoc)
            df_atomtypes=pool.apply_async(CreateAtomTypedf.CreateAtomTypedf,(getatomstype,smile,energy, Filenames[i],Formula))  # parallel 
            
            df_atomtypes=df_atomtypes.get(timeout=2)
            if df_atomtypes is not None:
               dframes.append(df_atomtypes)
               df_atomt=pd.concat(dframes)
               df_atomt.drop_duplicates(inplace=True)

        pool.close()   # close the pool here

    return df_cartesian, df_atomt


if __name__ == "__main__":

    Filenames=GetFileList.GetFileList()
    df_cartesian, df_atomt = mainFunc('C9H13NO')
    # print(df_cartesian.head())
    df_atomt.sort_values(by=['file_id','id'], inplace=True)
    df_cartesian.sort_values(by=['file_id','id'], inplace=True)
    df_final=df_cartesian.merge(df_atomt, on=['file_id','id'])
    df_final=df_final.astype({'type': int, 'valence':int, 'energy':float})
    # replace element type with atomic number
    replace_atoms = {"elementType": {"C": 6, "N": 7, "O": 8, "H": 1}}
    df_final=df_final.replace(replace_atoms)
    df_final=df_final.astype({'x3': np.float64, 'y3':np.float64, 'z3':np.float64})
    for i in range(len(df_final)):
       df_final['id'][i]=int(df_final['id'][i][1:])
    df_final.to_csv('Final_Ret.csv')
    
    Matrix_overall= Collect_Final_Matrix.Collect_Final_Matrix(df_final)
    data, label=generate_dzfiles.generate_dzfiles(Matrix_overall,df_final)
    
    delete_cmls.delete_cmls()
