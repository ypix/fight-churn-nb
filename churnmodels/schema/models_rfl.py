from sqlalchemy import MetaData, create_engine, Table
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Subscription(Base):
    __table__ = Table('subscription', Base.metadata, autoload=True)


class Account(Base):
    __table__ = Table('account', Base.metadata, autoload=True)


class ActivePeriod(Base):
    __table__ = Table('active_period', Base.metadata, autoload=True)


class ActiveWeek(Base):
    __table__ = Table('active_week', Base.metadata, autoload=True)


class Event(Base):
    __table__ = Table('event', Base.metadata, autoload=True)


class EventType(Base):
    __table__ = Table('event_type', Base.metadata, autoload=True)


class Metric(Base):
    __table__ = Table('metric', Base.metadata, autoload=True)


class MetricName(Base):
    __table__ = Table('metric_name', Base.metadata, autoload=True)


class Observation(Base):
    __table__ = Table('observation', Base.metadata, autoload=True)

def _howto_do_it():
    from re import sub

    # function to convert string to camelCase
    def camelCase(string):
        string = sub(r"(_|-)+", " ", string).title().replace(" ", "")
        # return string[0].lower() + string[1:]
        return string[0].upper() + string[1:]

    user = "postgres"
    pw = "password"
    dbname = "churn"

    database_uri = f"postgresql://{user}:{pw}@localhost:5432/{dbname}"
    engine = create_engine(database_uri)

    meta = MetaData()
    meta.reflect(bind=engine, schema="biznet1")
    for table in meta.tables.values():
        print("""
    class %s(Base):
        __table__ = Table(%r, Base.metadata, autoload=True)
        """ % (camelCase(table.name), table.name))
    pass
