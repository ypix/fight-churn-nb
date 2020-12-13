import concurrent.futures
import glob
import os
from datetime import date
from math import ceil

import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, func, event
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm
#
from churnmodels.schema import create_tables, create_lookups, Account, Subscription, Event
from churnmodels.simulation.customer import Customer
# from churnmodels.simulation.utility import UtilityModel
from churnmodels.simulation.utility2 import UtilityModel

from churnmodels.helpers import required_envvar
from churnmodels.simulation.behavior2 import FatTailledBehaviorModel
from churnmodels.simulation.churnsim import ChurnSimulation as ChurnSimulationBase
from churnmodels.conf import folder as conf_folder


class ChurnSimulation(ChurnSimulationBase):

    def __init__(self, model, start, end, init_customers, seed, engine, schema=None):
        '''
        Creates the behavior/utility model objects, sets internal variables to prepare for simulation, and creates
        the database connection

        :param model: name of the behavior/utility model parameters
        :param start: start date for simulation
        :param end: end date for simulation
        :param init_customers: how many customers to create at start date
        :param seed: set a random seed to compare simulation results
        :param engine: db engine object
        '''

        self.model_name = model
        self.engine = engine
        self.schema = schema
        self.start_date = start
        self.end_date = end
        self.init_customers = init_customers
        self.monthly_growth_rate = 0.1

        self.util_mod = UtilityModel(self.model_name)

        # path = '../conf/'
        path = conf_folder + '/'
        # behavior_versions = glob.glob('../conf/' + self.model_name + '_*.csv')
        behavior_versions = glob.glob(path + self.model_name + '_*.csv')
        self.behavior_models = {}
        self.model_list = []
        for b in behavior_versions:
            version = b[(b.find(self.model_name) + len(self.model_name) + 1):-4]
            if version in ('utility', 'population', 'country', 'plans'):
                continue
            behave_mod = FatTailledBehaviorModel(self.model_name, seed, version)
            self.behavior_models[behave_mod.version] = behave_mod
            self.model_list.append(behave_mod)

        if len(self.behavior_models) > 1:
            # self.population_percents = pd.read_csv('../conf/' + self.model_name + '_population.csv', index_col=0)
            self.population_percents = pd.read_csv(path + '/' + self.model_name + '_population.csv', index_col=0)
        self.util_mod.setChurnScale(self.behavior_models, self.population_percents)
        self.population_picker = np.cumsum(self.population_percents)

        self.plans = pd.read_csv(path + '/' + self.model_name + '_plans.csv')
        self.country_lookup = pd.read_csv(path + '/' + self.model_name + '_country.csv')

        self.subscription_count = 0
        # the data base engine canbe sqlite, postgres, ... everything that sqlalchemy knows
        # self.engine = engine

    def run_simulation(self):
        '''
        Simulation test function. First it prepares the database by truncating any old events and subscriptions, and
        inserting the event types into the database.  Next it creeates the initial customers by calling
        create_customers_for_month, and then it advances month by month adding new customers (also using
        create_customers_for_month.)  The number of new customers for each month is determined from the growth rate.
        Note that churn is not handled at this level, but is modeled at the customer level.
        :return:
        '''

        # database setup
        # if not self.truncate_old_sim():
        #     return
        # Any model can insert the event types
        # ...the event_types have been added already
        # self.behavior_models[next(iter(self.behavior_models))].insert_event_types(self.model_name, self.db)

        # Initial customer count
        print('\nCreating %d initial customers for month of %s' % (self.init_customers, self.start_date))
        self.create_customers_for_month(self.start_date, self.init_customers)
        print('Created %d initial customers with %d subscriptions for start date %s' % (
            self.init_customers, self.subscription_count, str(self.start_date)))

        # return

        # Advance to additional months
        next_month = self.start_date + relativedelta(months=+1)
        n_to_add = int(ceil(self.init_customers * self.monthly_growth_rate))  # number of new customers in first month
        while next_month < self.end_date:
            print('\nCreating %d new customers for month of %s:' % (n_to_add, next_month))
            self.create_customers_for_month(next_month, n_to_add)
            print('Created %d new customers for month %s, now %d subscriptions\n' % (
                n_to_add, str(next_month), self.subscription_count))
            next_month = next_month + relativedelta(months=+1)
            n_to_add = int(ceil(n_to_add * (1.0 + self.monthly_growth_rate)))  # increase the new customers by growth

        # self.remove_tmp_files()

    def create_customers_for_month(self, month_date, n_to_create):
        '''
        Creates all the customers for one month, by calling simulate_customer and copy_customer_to_database in a loop.
        :param month_date: the month start date
        :param n_to_create: number of customers to create within that month
        :return:
        '''

        items1 = range(n_to_create)
        pbar1 = tqdm(total=n_to_create, desc="Simulated Customers", ascii=True, position=0,
                     bar_format="{l_bar}{bar:50}{r_bar}{bar:-10b}")

        def substep():
            customer0 = self.simulate_customer(month_date)
            customer0.delta_subscriptions = len(customer0.subscriptions)
            customer0.delta_events = len(customer0.events)

            # showing the progress bar...
            pbar1.update(1)
            return customer0

        customers_sim = mainloop_threading(substep, items1)
        total_subscriptions = sum(x.delta_subscriptions for x in customers_sim)
        total_events = sum(x.delta_events for x in customers_sim)
        self.subscription_count += total_subscriptions

        options = {"schema": self.schema, "product": self.model_name, "bill_period_months": 1}
        assign_customer_pre(customers_sim, self.engine, options=options)


def assign_customer_pre(customers, engine, options):
    """
    Putting the simulated data into the database
    customers are completely stored in an array of Python objects

    :param customers:
    :type
    :param engine:
    :type engine:
    :param options:
    :type options:
    :return:
    :rtype:
    """
    """
    :arg
    """
    session = sessionmaker(bind=engine)()

    n_to_create = len(customers)

    pbar0 = tqdm(total=n_to_create, desc="Writing Accounts", ascii=True, position=0,
                 bar_format="{l_bar}{bar:50}{r_bar}{bar:-10b}")

    df = pd.DataFrame({
        'channel': pd.Series([], dtype='str'),
        'date_of_birth': pd.Series([], dtype='datetime64[ns]'),
        'country': pd.Series([], dtype='str')})

    for customer in customers:
        df = df.append(
            [{"channel": customer.channel, "date_of_birth": customer.date_of_birth, "country": customer.country}])
        # showing the progress bar...
        pbar0.update(1)

    # getting the last id (primary key)
    maxidx = session.query(func.max(Account.id)).one()[0] or 0
    session.commit()
    maxidxs = session.query(func.max(Subscription.id)).one()[0] or 0
    session.commit()
    maxidxe = session.query(func.max(Event.id)).one()[0] or 0
    session.commit()

    subs_delta = sum(len(x.subscriptions) for x in customers)
    events_delta = sum(len(x.events) for x in customers)
    # print(subs_delta)
    mm = subs_delta + events_delta
    pbar1 = tqdm(total=mm, desc="Writing Subs/Events", ascii=True, position=0,
                 bar_format="{l_bar}{bar:50}{r_bar}{bar:-10b}")

    df_s0 = []
    df_e0 = []
    index = maxidx + 1
    for customer in customers:
        account_id = index
        for subs in customer.subscriptions:
            df_s0.append({
                "account_id": account_id, "start_date": subs[0], "end_date": subs[1],
                "product": options["product"], "mrr": subs[2],
                "bill_period_months": options["bill_period_months"]
            })
            # showing the progress bar...
            pbar1.update(1)
        for subs in customer.events:
            df_e0.append(
                {
                    "account_id": account_id, "event_time": subs[0],
                    "event_type_id": subs[1] + 1
                })
            # showing the progress bar...
            pbar1.update(1)
        index += 1

    # accounts
    df.index = pd.RangeIndex(maxidx + 1, maxidx + 1 + len(df.index))

    db_opts = {
        "if_exists": "append",
        "index": False
    }
    print(options)
    if options["schema"] is not None:
        db_opts["schema"] = options["schema"]
    df.to_sql("account", engine, **db_opts)

    # subscriptions & events
    df_s = pd.DataFrame({
        'account_id': pd.Series([], dtype='int'),
        'start_date': pd.Series([], dtype='datetime64[ns]'),
        'end_date': pd.Series([], dtype='datetime64[ns]'),
        'product': pd.Series([], dtype='str'),
        'bill_period_months': pd.Series([], dtype='int'),
        'mrr': pd.Series([], dtype='float')})
    df_e = pd.DataFrame({
        'account_id': pd.Series([], dtype='int'),
        'event_time': pd.Series([], dtype='datetime64[ns]'),
        'event_type_id': pd.Series([], dtype='int')})
    df_s = df_s.append(df_s0, ignore_index=True)
    df_e = df_e.append(df_e0, ignore_index=True)
    df_s.index = pd.RangeIndex(maxidxs + 1, maxidxs + 1 + len(df_s.index))
    df_e.index = pd.RangeIndex(maxidxe + 1, maxidxe + 1 + len(df_e.index))

    db_opts = {"index": False, "chunksize": 10000, "if_exists": "append"}
    if options["schema"] is not None:
        db_opts["schema"] = options["schema"]

    # storing into the DB (by the help of pandas)
    df_s.to_sql("subscription", engine, **db_opts)
    df_e.to_sql("event", engine, **db_opts)
    session.commit()
    return customers


def mainloop_threading(someroutine, items):
    results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for x in items:
            futures.append(executor.submit(someroutine))
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    return results


def _beh_generate_customer(start_of_month, log_means, behave_cov, channel_name):
    '''
    Given a mean and covariance matrix, the event rates for the customer are drawn from the multi-variate
    gaussian distribution.
    subtract 0.5 and set min at 0.5 per month, so there can be very low rates despite 0 (1) min in log normal sim
    :return: a Custoemr object
    '''
    exp_fun = lambda x: np.power(1.6, x)
    customer_rates = np.random.multivariate_normal(mean=log_means, cov=behave_cov)
    customer_rates = exp_fun(customer_rates)
    customer_rates = np.maximum(customer_rates - 0.667, 0.333)
    new_customer = Customer(customer_rates, channel_name=channel_name, start_of_month=start_of_month)
    # print(customer_rates)
    return new_customer


def get_engine():
    """
    this routine create the simulation data base
    it is controlled by envirnoment variables
    :return:
    :rtype:
    """
    dialect = os.getenv("CHURN_DB_DIALECT") or None

    # print(f"setting up DB for dialect {dialect}")
    engine_default_uri = f"sqlite:///:memory:"
    engine = create_engine(f"sqlite:///:memory:")
    if dialect is None or dialect == "sqlite":
        if "SQLITE_FILE" not in os.environ:
            # the sqlite memory db is the default
            pass
        else:
            # if a file is given in the env-var SQLITE_FILE ...
            sqlitefile = os.getenv("SQLITE_FILE")
            engine = create_engine(f"sqlite:///{sqlitefile}")
            # print(f"created sqlite file {sqlitefile}")

        # adding this listener will speed up the sqlite transacrions
        @event.listens_for(engine, "begin")
        def do_begin(conn):
            # emit our own BEGIN
            # conn.execute("BEGIN IMMEDIATE")
            conn.execute("BEGIN TRANSACTION")
    # Postgres
    elif dialect == "postgres":
        # for postgres we need a schema
        schema = required_envvar("CHURN_DB_SCHEMA", "Please set the environment variable CHURN_DB_SCHEMA for postgres")
        # ... and the user credentials
        user = required_envvar("CHURN_DB_USER", "Please set the environment variable CHURN_DB_USER for postgres")
        pw = required_envvar("CHURN_DB_PASS", "Please set the environment variable CHURN_DB_PASS for postgres")
        dbname = required_envvar("CHURN_DB", "Please set the database name in CHURN_DB for postgres")

        default_database_uri = f"postgresql://{user}:{pw}@localhost:5432/postgres"
        database_uri = f"postgresql://{user}:{pw}@localhost:5432/{dbname}"

        engine = create_engine(database_uri)

        # check if db exists
        try:
            engine.connect()
            sql = "select schema_name from information_schema.schemata;"
            engine.execute(sql)
        except OperationalError:
            # Switch database component of the uri
            engine = create_engine(default_database_uri)

    # returning the engine object
    return engine


def setup_all(modelname):
    """
    creating the data base
    :param modelname:
    :type modelname:
    :return:
    :rtype:
    """
    engine = get_engine()
    create_tables(engine)  # if tables exist, they will all be dropped
    create_lookups(engine, modelname)
    return engine


def simulate(options={}):
    schema = os.getenv("CHURN_DB_SCHEMA") if os.getenv("CHURN_DB_DIALECT") == "postgres" else None
    parameters = {
        "model": os.getenv("CHURN_MODEL") or "biznet1",
        "start": date(2020, 1, 1),
        "end": date(2020, 6, 1),
        "seed": 5432,
        "init_customers": 100,
        "schema": schema,
    }
    for key, val in options.items():
        # if key == "schema":
        #     continue
        if key in parameters:
            parameters[key] = options[key]
    engine = setup_all(parameters["model"])

    if os.getenv("CHURN_DB_DIALECT")=="sqlite":
        del parameters["schema"]
    churn_sim = ChurnSimulation(engine=engine, **parameters)
    churn_sim.run_simulation()


if __name__ == '__main__':
    churnmodel = os.getenv("CHURN_MODEL")
    schema = os.getenv("CHURN_DB_SCHEMA") if os.getenv("CHURN_DB_DIALECT") == "postgres" else None
    start = date(2020, 1, 1)
    end = date(2020, 6, 1)
    init_customers = 10000

    random_seed = 5432
    engine = setup_all(churnmodel)

    churn_sim = ChurnSimulation(churnmodel, start, end, init_customers, random_seed, engine, schema)
    churn_sim.run_simulation()
