
import pandas as pd
from xml.dom import minidom
import numpy as np

# create a dataframe of AtomTypes
def CreateAtomTypedf(getatomstype, smile, energy, filenames, Formula):
    lst1=[]
    lst2=[]
    lst3=[]
    lst4=[]
    atom_df=pd.DataFrame()

    if Formula in filenames:
      for i in range(len(getatomstype)):
          if ('id' in (getatomstype[i].attributes)):

              lst1.append(getatomstype[i].attributes['id'].value)
              lst3.append(getatomstype[i].attributes['type'].value)
              lst4.append(getatomstype[i].attributes['valence'].value)

      df_atomtype=pd.DataFrame() 
      df_atomtype['id']=lst1
      df_atomtype['type']=lst3
      df_atomtype['valence']=lst4       
      df_atomtype['smile']=smile    
      df_atomtype['energy']= energy
      df_atomtype['file_id']=filenames


      return df_atomtype   