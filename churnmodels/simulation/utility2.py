from pandas import DataFrame

from .utility import UtilityModel as UtilityModelBase
from churnmodels.conf import folder as conf_folder
import pandas as pd


class UtilityModel(UtilityModelBase):
    def __init__(self, name):
        self.name = name
        data = pd.read_csv(conf_folder + '/' + name + '_utility.csv', index_col=0)
        self.linear_utility = data['util']
        self.behave_names = data.index.values

    def __init__(self, name, df: DataFrame):
        self.name = name
        self.linear_utility = df['util']
        self.behave_names = df.index.values
