from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, DateTime, Float, Boolean, event
import pandas as pd
import os

from sqlalchemy.sql.ddl import CreateSchema, DDL


class Base(object):
    pass


glob_schema= os.getenv("CHURN_DB_SCHEMA") or "public"
if "CHURN_DB_SCHEMA" in os.environ and os.getenv("CHURN_DB_DIALECT") != "sqlite":
    # print(f"postgres schema is {glob_schema}")
    setattr(Base, "__table_args__", {"schema": glob_schema})

Base = declarative_base(cls=Base)

if os.getenv("CHURN_DB_DIALECT") == "postgres":
    Base.metadata.schema=glob_schema
    event.listen(Base.metadata, 'before_create', DDL(f"CREATE SCHEMA IF NOT EXISTS {glob_schema}"))


class Account(Base):
    __tablename__ = "account"

    id = Column(Integer, primary_key=True)
    channel = Column(String, default=None)
    date_of_birth = Column(Date, nullable=False)
    country = Column(String, default=None)

    def __repr__(self):
        return "<Account(channel='%s', date_of_birth='%s', country='%s')>" % (
            self.channel, self.date_of_birth, self.country)


class ActivePeriod(Base):
    __tablename__ = "active_period"

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, nullable=False)
    start_date = Column(Date, nullable=False)
    churn_date = Column(Date, default=None)

    def __repr__(self):
        return "<ActivePeriod(account_id='%s', start_date='%s', churn_date='%s')>" % (
            self.account_id, self.start_date, self.churn_date)


class ActiveWeek(Base):
    __tablename__ = "active_week"

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, default=None)

    def __repr__(self):
        return "<ActiveWeek(account_id='%s', start_date='%s', end_date='%s')>" % (
            self.account_id, self.start_date, self.end_date)


class Event(Base):
    __tablename__ = "event"

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, nullable=False)
    event_time = Column(DateTime, nullable=False)
    event_type_id = Column(Integer, nullable=False)

    def __repr__(self):
        return "<Event(account_id='%s', event_time='%s', event_type_id='%s')>" % (
            self.account_id, self.event_time, self.event_type_id)


class EventType(Base):
    __tablename__ = "event_type"

    event_type_id = Column(Integer, primary_key=True)
    event_type_name = Column(String, nullable=False)

    def __repr__(self):
        return "<EventType(event_type_id='%s', event_type_name='%s')>" % (
            self.event_type_id, self.event_type_name)


class Metric(Base):
    __tablename__ = "metric"

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, nullable=False)
    metric_time = Column(DateTime, nullable=False)
    metric_name_id = Column(Integer, nullable=False)
    metric_value = Column(Float, default=None)

    def __repr__(self):
        return "<Metric(account_id='%s', metric_time='%s', metric_name_id='%s', metric_value=%5.2d)>" % (
            self.account_id, self.metric_time, self.metric_name_id, self.metric_value)


class MetricName(Base):
    __tablename__ = "metric_name"

    id = Column(Integer, primary_key=True)
    metric_name_id = Column(Integer, nullable=False)
    metric_name = Column(String, default=None)

    def __repr__(self):
        return "<MetricName(metric_name_id='%s', metric_name='%s')>" % (
            self.metric_name_id, self.metric_name)


class Observation(Base):
    __tablename__ = "observation"

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, nullable=False)
    observation_date = Column(Date, nullable=False)
    is_churn = Column(Boolean, default=False)

    def __repr__(self):
        return "<Observation(account_id='%s', observation_date='%s', is_churn='%s')>" % (
            self.account_id, self.observation_date, self.is_churn)


class Subscription(Base):
    __tablename__ = "subscription"

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, nullable=False)
    product = Column(String, default=None)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, default=None)
    mrr = Column(Float, nullable=False)
    quantity = Column(Float, default=None)
    units = Column(String, default=None)
    bill_period_months = Column(Integer, nullable=False)

    def __repr__(self):
        text = "<Subscription("
        text += "account_id='%s', product='%s', start_date='%s'" % (
            self.account_id, self.product, self.start_date)
        text += ", end_date='%s', mrr=%5.2d, quantity=%5.2d" % (
            self.end_date, self.mrr, self.quantity)
        text += ", units='%s', bill_period_months=%s" % (
            self.units, self.bill_period_months)
        text += ")>"
        return text


#########################
def create_tables(engine):
    metadata = Base.metadata
    metadata.bind = engine
    try:
        metadata.drop_all()
    except:
        pass
    metadata.create_all()


def create_lookups(engine, modelname):
    """
    maing lookup tables
    :arg
    """
    from churnmodels.conf import folder as path

    prefix = modelname
    tbl2stats = {
        "event_type": {"statsfile": "utility", "col_name": "event_type_name", "class_name": "EventType"}
    }

    import churnmodels.conf

    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()

    for lutbl, attribs in tbl2stats.items():
        statsfile = attribs["statsfile"]
        col_name = attribs["col_name"]
        class_name = attribs["class_name"]

        filename0 = f"{prefix}_{statsfile}"
        filename = f"{path}/{filename0}.csv"
        df = pd.read_csv(filename, index_col=0)
        for name in df.index.values:
            newlu = EventType() if class_name == "EventType" else None
            setattr(newlu, col_name, name)
            session.add(newlu)
        session.commit()
