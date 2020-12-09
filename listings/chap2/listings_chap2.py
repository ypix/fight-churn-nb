from sqlalchemy import create_engine, func, or_
from sqlalchemy.orm import sessionmaker

from churnmodels import Subscription, Event
from churnsim2 import get_engine
import pandas as pd

# engine = get_engine()
engine = create_engine(f"sqlite:///c:/tmp/churn2.db")


def run_2_1():
    session = sessionmaker(bind=engine)()
    d_start_date = "2020-01-01"
    d_end_date = "2020-03-01"

    """
    -- PostGres SQL for start_accounts: 
        select  account_id, sum (mrr) as total_mrr    
        from subscription s inner join date_range d on
            s.start_date <= d.start_date    
            and (s.end_date > d.start_date or s.end_date is null)
        group by account_id    
    """

    # start_accounts
    q = session.query(Subscription.account_id, func.sum(Subscription.mrr).label("total_mrr"))\
        .filter(
                # SQL: s.start_date <= d.start_date
            Subscription.start_date <= d_start_date,
                # SQL: s.end_date > d.start_date or s.end_date is null
            or_(Subscription.end_date > d_start_date, Subscription.end_date == None))\
        .group_by(Subscription.account_id) # SQL: group by account_id
    # getting the result from the DB stored into a pandas DataFrame
    start_accounts = pd.read_sql(q.statement, engine).set_index("account_id")

    # end_accounts
    q = session.query(Subscription.account_id, func.sum(Subscription.mrr).label("total_mrr")).filter(
        Subscription.start_date <= d_end_date,
        or_(Subscription.end_date > d_end_date, Subscription.end_date == None)).group_by(Subscription.account_id)
    # q = q.filter(Subscription.account_id==64)
    end_accounts = pd.read_sql(q.statement, engine).set_index("account_id")

    # pandas calls
    # retained_accounts: inner join
    retained_accounts = pd.merge(start_accounts, end_accounts, on="account_id")

    # example of left/right joins with pandas
    outer = start_accounts.join(end_accounts, lsuffix='_start', rsuffix='_end')
    outer2 = end_accounts.join(start_accounts, lsuffix='_start', rsuffix='_end')

    pass


if __name__ == '__main__':
    run_2_1()
