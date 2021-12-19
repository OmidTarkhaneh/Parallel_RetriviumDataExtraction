
import pandas as pd
from xml.dom import minidom
from multiprocessing import Pool, TimeoutError
import numpy as np
from decimal import  Decimal

# Initialize the Reterivium Matrix
def Initialize_Retrivium_Matrix(df_file):
    filesize=len(df_file)
    Mat_Init=np.zeros((filesize+4,filesize+4))
   
    rowcounter=0
    colcounter=0
    for row in range(filesize+4):
          if (row>3):
                Mat_Init[row][0]= df_file.iloc[rowcounter]['id']
                Mat_Init[row][1]= df_file.iloc[rowcounter]['elementType']
                Mat_Init[row][2]= df_file.iloc[rowcounter]['type']
                Mat_Init[row][3]= df_file.iloc[rowcounter]['valence']
                rowcounter+=1
    for col in range(filesize+4):
      if (col>3):
        Mat_Init[0][col]= df_file.iloc[colcounter]['id']
        Mat_Init[1][col]= df_file.iloc[colcounter]['elementType']
        Mat_Init[2][col]= df_file.iloc[colcounter]['type']
        Mat_Init[3][col]= df_file.iloc[colcounter]['valence']
        colcounter+=1

    return Mat_Init   

