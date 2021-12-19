
import pandas as pd
from xml.dom import minidom
from tqdm import tqdm
import numpy as np

import DataGathering

def Collect_Final_Matrix(df_final):
    filesize=len(df_final.file_id.unique())
    files=df_final['file_id'].unique().tolist()
    
    sample_size=len(df_final[df_final['file_id']==files[0]])
    Main_Array=np.zeros((filesize,sample_size+4,sample_size+4))

    for i in tqdm(range(filesize)):
         df_sampledistance=df_final[df_final['file_id']==files[i]]
         df_sampledistance.reset_index(inplace=True)
        
         arr_dist=DataGathering.DataGathering(df_sampledistance)
         Main_Array[i]=arr_dist

    return Main_Array