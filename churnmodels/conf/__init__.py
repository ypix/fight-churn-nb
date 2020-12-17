import glob
import os
import sys
import pandas as pd

folder = os.path.dirname(os.path.abspath(__file__))


def get_model(model_name) -> dict:
    """
    retruns a dictionary of all model (csv) files belongig to the model "model"
    :param model_name:
    :return:
    """
    conf_files = get_files(model_name)
    res = {}
    for version, filename in conf_files.items():
        df = get_csv(model_name, version)
        df = df.set_index(df.columns[0])
        res[version] = {}
        res[version]["data"] = df
        res[version]["filename"] = filename
    return res


def get_csv(model_name, channel):
    filename = f"{folder}/{model_name}_{channel}.csv"
    if not os.path.exists(filename):
        print(f"Could not find file {filename}")
    df = pd.read_csv(filename)
    return df


def get_files(model_name):
    pattern = f"{folder}/{model_name}_*.csv"
    nn = len(f"{folder}/{model_name}_")
    erg = {}
    for fname in glob.glob(pattern):
        key = fname[nn:-4]
        erg[key] = fname
    return erg
