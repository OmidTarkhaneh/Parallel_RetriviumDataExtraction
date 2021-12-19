
import pandas as pd
from xml.dom import minidom
import numpy as np


# This function help to extract cartesian position from cml files
def GetCartesian(mydoc, Filenames, Formula):

        atoms=mydoc.getElementsByTagName('molecule')
        lst1=[]
        lst2=[]
        lst3=[]
        lst4=[]
        lst5=[]
        lst6=[]
        lst7=[]
        checklst=[]
        df_Cart=pd.DataFrame()
        Main_Df=pd.DataFrame()
        flag=True

        if Formula in Filenames:
          for k in range(len(atoms)):

              x=atoms[k].attributes['id'].value
              if(x=='final') & (flag):
              
                  atom=atoms[k].getElementsByTagName('atom')
                  for j in range(len(atom)):
                          x=atom[j].attributes['id'].value
                      
                          if (x not in checklst):
                              checklst.append(x)
                              
                              lst1.append(atom[j].attributes['id'].value)
                              lst2.append(atom[j].attributes['elementType'].value)
                              lst3.append(atom[j].attributes['x3'].value)
                              lst4.append(atom[j].attributes['y3'].value)
                              lst5.append(atom[j].attributes['z3'].value)
                              lst6.append(Filenames)
                              lst7.append(Filenames[38:49])
                  flag=False          
                  df_cartesian=pd.DataFrame()
                  df_cartesian['id']=lst1
                  df_cartesian['elementType']=lst2
                  df_cartesian['x3']=lst3
                  df_cartesian['y3']=lst4
                  df_cartesian['z3']=lst5
                  df_cartesian['file_id']=lst6
                  df_cartesian['Formula']=lst7
                  df_Cart=pd.concat([Main_Df, df_cartesian]) 

                  return df_Cart   