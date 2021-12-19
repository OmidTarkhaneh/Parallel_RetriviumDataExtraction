from numpy import asarray
from numpy import savez_compressed
from numpy import load
import energy_label_generation

def generate_dzfiles(Matrix_overall, df_final):
    data_all = Matrix_overall
    # save to npz file format
    savez_compressed('data.npz', data_all)
    energy_label, df_energy=energy_label_generation.energy_label_generation(df_final)
    savez_compressed('label_data.npz', energy_label)
    savez_compressed('energydf_data.npz', df_energy)
    # extract the data samples
    samples_data = load('data.npz')
    data=samples_data['arr_0']

    # extract the labels
    engdata = load('label_data.npz')
    label=engdata['arr_0']

    return data,label