
import pandas as pd
from xml.dom import minidom
import tarfile
from tqdm import tqdm
import numpy as np


# Function to get Atom types
def GetAtomTypes(mydoc):
    getatomstype=None
    text=None
    smile=None
    flag=True
    atoms=mydoc.getElementsByTagName('property')
    for i in range(len(atoms)):
        x=atoms[i].attributes['dictRef'].value
        if x=='retrievium:atomTypes':
            getatomstype=mydoc.getElementsByTagName('atomType')

        if (x=='retrievium:inputSMILES') & (flag):
            text=atoms[i].getElementsByTagName('scalar')
            for i in range(1):
                smile=text[0].firstChild.data
                flag=False

        if (x=='retrievium:Energy_Total'):
            text=atoms[i].getElementsByTagName('scalar')
            for i in range(1):
                energy=text[0].firstChild.data
                       

    return  getatomstype,smile, energy 