
import pandas as pd
from xml.dom import minidom
import numpy as np
import  Initialize_Retrivium_Matrix
import Calculate_distance
import multiprocessing as mp
from multiprocessing import Pool


def DataGathering(df_sampledata):
    filesize=len(df_sampledata)
    # initialize the matrix
    Ret_Mat=Initialize_Retrivium_Matrix.Initialize_Retrivium_Matrix(df_sampledata)

    # calculate the distance of cartesian positions
    with Pool(processes=mp.cpu_count()) as pool:   # apply parallel
        Ret_MainMat=pool.apply_async(Calculate_distance.Calculate_distance,(df_sampledata, Ret_Mat))
        Ret_MainMat.wait(timeout=5)
        if Ret_MainMat.ready():
            Ret_MainMat = Ret_MainMat.get(timeout=5)

        pool.close()
    return Ret_MainMat



    