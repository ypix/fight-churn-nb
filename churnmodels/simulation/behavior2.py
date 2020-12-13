from .behavior import BehaviorModel, is_pos_def
from .behavior import GaussianBehaviorModel as GaussianBehaviorModelBase
# from churnmodels.conf import folder as conf_folder
from churnmodels.simulation.customer import Customer
import pandas as pd
import numpy as np

from .. import conf


class GaussianBehaviorModel(GaussianBehaviorModelBase):

    def __init__(self, name, random_seed=None, version='model'):
        '''
        This behavior model uses a mean and (pseudo) covariance matrix to generate customers with event rates.
        The parameters are passed on a csv file that should be located in a `conf` directory adjacent to the code.
        The format of the data file is:
            first column of the data file should be the names and must have the heading 'behavior'
            second colum is the mean rates and must have the heading 'mean'
            the remaining columns should be a pseudo-covariance matrix for the behaviors, so it must be real valued and
            have the right number of columns with column names the same as the rows
        These are loaded using pandas.
        :param name:
        '''
        self.name = name
        self.version = version
        # model_path='../conf/'+name + '_' + version + '.csv'
        # model_path = conf_folder + '/' + name + '_' + version + '.csv'
        # model = pd.read_csv(model_path)
        # model.set_index(['behavior'], inplace=True)

        model = conf.getcsv(name, version)
        model = model.set_index(model.columns[0])

        self.behave_means = model['mean']
        self.behave_names = model.index.values
        self.behave_cov = model[self.behave_names]
        self.min_rate = 0.01 * self.behave_means.min()
        corr_scale = (np.absolute(self.behave_cov.to_numpy()) <= 1.0).all()
        if random_seed is not None:
            np.random.seed(random_seed)
        if not is_pos_def(self.behave_cov):
            if input("Matrix is not positive semi-definite: Multiply by transpose? (enter Y to proceed)") in ('y', 'Y'):
                # https://stackoverflow.com/questions/619335/a-simple-algorithm-for-generating-positive-semidefinite-matrices
                self.behave_cov = np.dot(self.behave_cov, self.behave_cov.transpose())
            else:
                exit(0)

        if corr_scale:
            self.scale_correlation_to_covariance()



class FatTailledBehaviorModel(GaussianBehaviorModel):

    def __init__(self, name, random_seed=None, version=None):
        self.exp_base = 1.6
        self.log_fun = lambda x: np.log(x) / np.log(self.exp_base)
        self.exp_fun = lambda x: np.power(self.exp_base, x)

        super(FatTailledBehaviorModel, self).__init__(name, random_seed, version)

    def scale_correlation_to_covariance(self):
        self.log_means = self.log_fun(self.behave_means)
        rectified_means = np.array([max(m, 0.0) for m in self.log_means])
        # print('Scaling correlation by behavior means...')

        scaling = np.sqrt(rectified_means)
        self.behave_cov = np.matmul(self.behave_cov, np.diag(scaling))
        self.behave_cov = np.matmul(np.diag(scaling), self.behave_cov)

    def behave_var(self):
        return self.exp_fun(np.diagonal(self.behave_cov))

    def generate_customer(self, start_of_month):
        '''
        Given a mean and covariance matrix, the event rates for the customer are drawn from the multi-variate
        gaussian distribution.
        subtract 0.5 and set min at 0.5 per month, so there can be very low rates despite 0 (1) min in log normal sim
        :return: a Custoemr object
        '''
        customer_rates = np.random.multivariate_normal(mean=self.log_means, cov=self.behave_cov)
        # customer_rates=self.exp_fun(customer_rates)
        customer_rates = np.maximum(self.exp_fun(customer_rates) - 0.667, 1)
        # customer_rates = np.maximum(customer_rates-0.667,0.333)
        new_customer = Customer(customer_rates, channel_name=self.version, start_of_month=start_of_month)
        # print(customer_rates)
        return new_customer
