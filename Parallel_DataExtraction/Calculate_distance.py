
import pandas as pd
from xml.dom import minidom
import numpy as np


# Calculate the distance for the retrivium Matrix
def Calculate_distance(df_file, Ret_Mat):
  filesize=len(df_file)
  for row in range(4,filesize+4):
    id_temp=Ret_Mat[row][0]
    df_temp=df_file[df_file['id']==id_temp]
    
    distance=0
    
    # print('*************************')
    for col in range (4,filesize+4):
      id_col=Ret_Mat[0][col]
      df_col=df_file[df_file['id']==id_col]
      if id_col!=id_temp:
        distance=np.sqrt((df_col.iloc[0]['x3']-df_temp.iloc[0]['x3'])**2 + (df_col.iloc[0]['y3']-df_temp.iloc[0]['y3'])**2 + (df_col.iloc[0]['x3']-df_temp.iloc[0]['z3'])**2)
      else:
        distance=0  
        
      Ret_Mat[row][col]= distance
  return Ret_Mat  