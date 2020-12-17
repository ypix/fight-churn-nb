from pandas import DataFrame

from .utility import UtilityModel as UtilityModelBase
from churnmodels import conf
import pandas as pd


class UtilityModel(UtilityModelBase):
    def __init__(self, name, df: DataFrame = None):
        self.name = name
        if df is None:
            df = conf.get_csv(name, "utility")
            df = df.set_index(df.columns[0])
        self.linear_utility = df['util']
        self.behave_names = df.index.values
