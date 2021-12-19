import pandas as pd
from xml.dom import minidom
import tarfile
import numpy as np
import warnings
warnings.filterwarnings("ignore")

import glob
import itertools




def GetFileList():
    # Get list of Tar.gz files
    TarFilenames=[]
    # All files ending with .gz
    TarFilenames.append(glob.glob("./TarFiles/*.gz")) 
    TarFilenames = list(itertools.chain.from_iterable(TarFilenames))

    TarFilenamessize=len(TarFilenames)
    for i in range(TarFilenamessize):
        my_tar = tarfile.open(TarFilenames[i])
        my_tar.extractall('./cmlFiles') # specify which folder to extract to
        my_tar.close()      

    Filenames=[]
    # All files ending with .cml
    Filenames.append(glob.glob("./cmlFiles/*.cml")) 
    Filenames = list(itertools.chain.from_iterable(Filenames))

    return Filenames