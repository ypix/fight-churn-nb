from datetime import date, timedelta
import numpy as np
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

from churnmodels.schema import get_schema_rfl, get_db_uri
from churnmodels.simulation.utility2 import UtilityModel
from churnmodels.simulation.behavior2 import FatTailledBehaviorModel
from churnmodels.simulation import simulate, Customer
from churnmodels import conf


def simulate_customer(start_of_month, plans, model_list, population_percents, country_lookup: dict, util_mod, end_date):
    '''
    Simulate one customer collecting its events and subscriptions.

    This function has the core interaction between the simulation objects.  Customer is created from the behavior
    model, and picking a random start date within the month.  Then the customer objects simulates the events for
    the month, and the utility model determines if there is a churn based on the simulated event counts.

    :param start_of_month:
    :return: the new customer object it contains the events and subscriptions
    '''
    # customer_model = self.pick_customer_model()
    customer_model = np.random.choice(model_list, p=population_percents['pcnt'])
    new_customer = customer_model.generate_customer(start_of_month)

    # customer_country = np.random.choice(country_lookup['country'], p=country_lookup['pcnt'])
    customer_country = np.random.choice(list(country_lookup.keys()), p=list(country_lookup.values()))
    new_customer.country = customer_country

    new_customer.pick_plan(plans)

    # Pick a random start date for the subscription within the month
    end_range = start_of_month + relativedelta(months=+1)
    # this_month = start_of_month + timedelta(days=np.random.randrange((end_range - start_of_month).days))
    this_month = start_of_month + timedelta(days=np.random.randint((end_range - start_of_month).days))

    churned = False
    while not churned:
        next_month = this_month + relativedelta(months=1)
        new_customer.subscriptions.append((this_month, next_month, new_customer.mrr))
        month_count = new_customer.generate_events(this_month, next_month)
        churned = util_mod.simulate_churn(month_count, new_customer) or next_month > end_date
        if not churned:
            util_mod.simulate_upgrade_downgrade(month_count, new_customer, plans)
            this_month = next_month
    return new_customer


def test1():
    """
    creating an sqlite data base with simulation data
    Depending on the environment variables the data base will be created
    :return:
    :rtype:
    """
    model = "biznet1"
    options = {
        "model": model,
        "start": date(2020, 1, 1),
        "end": date(2020, 6, 1),
        "seed": 5432,
        "init_customers": 100,
        "schema": model
    }
    simulate(options)
    pass


def test2():
    """
    analysing the simulated behavior of a single customer
    :return:
    """
    model_name = "biznet1"
    seed = 5432
    options = {
        "model": model_name,
        "start": date(2020, 1, 1),
        "end": date(2020, 6, 1),
        "seed": seed,
        "init_customers": 100,
        "schema": model_name
    }
    np.random.seed(seed)

    model_files = conf.get_model(model_name)
    behavior_models = {}
    model_list = []
    behave_versions = [value for value in model_files.keys()
                       if value not in ('utility', 'population', 'country', 'plans')]
    for version in behave_versions:
        behave_mod = FatTailledBehaviorModel(model_name, seed, version)
        behavior_models[behave_mod.version] = behave_mod
        model_list.append(behave_mod)

    population_percents = None
    if len(behavior_models) > 1:
        population_percents = model_files["population"]["data"]
    util_mod = UtilityModel(model_name, model_files["utility"]["data"])
    util_mod.setChurnScale(behavior_models, population_percents)
    # population_picker = np.cumsum(population_percents)

    plans = model_files["plans"]["data"].reset_index()

    # country_lookup = model_files["country"]["data"].reset_index()
    countries = model_files["country"]["data"]
    country_lookup = countries[countries.columns[0]].to_dict()

    parameter = {
        "start_of_month": options["start"],
        "plans": plans,
        "model_list": model_list,
        "population_percents": population_percents,
        "country_lookup": country_lookup,
        "util_mod": util_mod,
        "end_date": options["end"]
    }
    cust1 = simulate_customer(**parameter)
    print(cust1)
    for key, val in cust1.__dict__.items():
        print(f"{key}={val}")
    # print(model_files["utility"]["data"])

    pass


def test3():
    """
    Example for accessing postgres DB with refelction
    :return:
    :rtype:
    """
    options = {"user": "postgres",
               "pass": "password",
               "dbname": "churn",
               "schema": "biznet1"
               }
    tables=get_schema_rfl(options)
    db_uri=get_db_uri(options, "postgres")
    engine=create_engine(db_uri)
    session = sessionmaker(bind=engine)()

    q=session.query(tables.Account)
    df=pd.read_sql_query(q.statement, engine)
    print(df)
    pass


if __name__ == '__main__':
    test1()
    # test2()
    # test3()
