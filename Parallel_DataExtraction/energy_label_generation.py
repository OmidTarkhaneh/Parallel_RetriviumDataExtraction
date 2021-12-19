
import pandas as pd
from xml.dom import minidom
import numpy as np
from decimal import  Decimal
from numpy import asarray
from numpy import savez_compressed
from numpy import load

def energy_label_generation(df_final):
    
  energy_files=df_final.file_id.unique()
  energy_label=df_final[df_final.file_id.isin(energy_files)]
  energy_label=energy_label[['file_id','energy']]
  df_energy=energy_label.drop_duplicates(subset='file_id', keep='first')
  df_energy=df_energy['energy']
  # Magnify the difference in energy values
  df_energy_magnify=(df_energy-min(df_energy))*2625.5
  energy_target=df_energy_magnify.to_numpy()

  return energy_target, df_energy