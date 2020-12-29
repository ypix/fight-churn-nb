import pandas as pd
import sqlparse
from sqlalchemy import create_engine, func, case, or_, literal, and_, select
from sqlalchemy.orm import sessionmaker

from churnmodels.schema import get_schema_rfl, get_db_uri


class DBHelper():
    engine = None
    to_days = lambda x: x
    schema = None
    T = None

    def __init__(self, options):

        if options["dialect"] in ["postgres", "postgresql"]:
            # tables is a (dynamical) module containg Wrapper classes for our data base
            T = get_schema_rfl(options)

            # connect to the database
            db_uri = get_db_uri(options, "postgres")  # "postgres" names the dialect we are using
            engine = create_engine(db_uri)
            engine.dialect.has_schema(engine, options["schema"])
            self.schema = options["schema"]
        else:
            T = get_schema_rfl(options)

            # connect to the database
            db_uri = get_db_uri(options, "sqlite")  # "postgres" names the dialect we are using
            engine = create_engine(db_uri)

            from sqlalchemy import event
            @event.listens_for(engine, "connect")
            def connect(dbapi_connection, connection_rec):
                dbapi_connection.enable_load_extension(True)
                dbapi_connection.execute('SELECT load_extension("libsqlitefunctions")')

        # ..how to bring all tables in T to the global namespace
        for tbl in T.__dict__.keys():
            if not tbl[0].isupper():
                continue
            exec(f"{tbl} = T.{tbl}")
        self.T = T
        self.engine = engine
        self.session = sessionmaker(bind=engine)()

        if options["dialect"] == "sqlite":
            # sqlite problematic when computing days
            self.to_days = lambda some_date: func.julianday(some_date)
        else:
            # dummy func because of sqlite
            self.to_days = lambda some_date: func.DATE(some_date)

    def pretty(self, q1):
        # debug: looking at the SQL pretty printed
        text1 = str(q1.statement.compile(self.engine, compile_kwargs={"literal_binds": True}))
        text2 = sqlparse.format(text1, reindent=True, keyword_case='upper')
        return text2
        # print(text2)

    def get_session(self):
        return self.session

    def get_dataset(self, d_obs_start=None, d_obs_end=None, metric_period=7):
        Metric = self.T.Metric
        MetricName = self.T.MetricName
        Observation = self.T.Observation
        session = self.get_session()
        fields = [
            Metric.account_id,
            Observation.observation_date,
            Observation.is_churn
        ]
        targets = {}
        df_metricnames = pd.read_sql(session.query(MetricName).statement, self.engine)
        for index, row in df_metricnames.iterrows():
            newfield = func.sum(case([
                (Metric.metric_name_id == row.metric_name_id, Metric.metric_value)
            ], else_=0)).label(row.metric_name)
            fields.append(newfield)

        qr = session.query(*fields) \
            .join(Observation, Metric.account_id == Observation.account_id) \
            .filter(
            Metric.metric_time > func.DATE(self.to_days(Observation.observation_date) - metric_period),
            Metric.metric_time <= Observation.observation_date) \
            .group_by(Metric.account_id, Metric.metric_time,
                      Observation.observation_date, Observation.is_churn) \
            .order_by(Observation.observation_date, Metric.account_id)

        if d_obs_start is not None:
            qr = qr.filter(Observation.observation_date >= d_obs_start)
        if d_obs_end is not None:
            qr = qr.filter(Observation.observation_date <= d_obs_end)

        # print(pretty_sql(qr))
        ddf = pd.read_sql(qr.statement, self.engine)
        # ddf=ddf.set_index("account_id")
        ddf = ddf.set_index(["account_id", "observation_date"])
        return ddf

    def get_active_customers(self):
        Metric = self.T.Metric
        MetricName = self.T.MetricName
        Subscription = self.T.Subscription

        engine = self.engine
        session = self.get_session()
        last_metric_time = session.query(func.DATE(func.max(Metric.metric_time))).one()[0] or 0
        last_metric_time = str(last_metric_time)  # making a string, let the pretty print work for postgres...

        fields = [
            Metric.account_id,
            func.DATE(last_metric_time).label("last_metric_time"),
        ]
        targets = {}
        df_metricnames = pd.read_sql(session.query(MetricName).statement, engine)
        for index, row in df_metricnames.iterrows():
            newfield = func.sum(case([
                (Metric.metric_name_id == row.metric_name_id, Metric.metric_value)
            ], else_=0)).label(row.metric_name)
            fields.append(newfield)

        qr = session.query(*fields) \
            .join(Subscription, Metric.account_id == Subscription.account_id) \
            .filter(
            func.DATE(Metric.metric_time) == last_metric_time,
            func.DATE(Subscription.start_date) <= last_metric_time,
            or_(func.DATE(Subscription.end_date) >= last_metric_time, Subscription.end_date == None)
        ) \
            .group_by(Metric.account_id, "last_metric_time") \
            .order_by(Metric.account_id)

        # print(pretty_sql(qr))
        ddf = pd.read_sql(qr.statement, engine)
        return ddf

    def make_relative_metrics(self, d_obs_start, d_obs_end, metric_name1, metric_name2):
        num_metric = self.make_relative_metrics_part(d_obs_start, d_obs_end, "num_metric", "num_value",
                                                     metric_name1)
        den_metric = self.make_relative_metrics_part(d_obs_start, d_obs_end, "den_metric", "den_value",
                                                     metric_name2)
        qr = self.make_relative_metrics_sub(num_metric, den_metric)
        return qr

    def make_relative_metrics_sub(self, num_metric, den_metric, metric_name_lu=-1):
        session = self.get_session()
        qr = session.query(
            num_metric.c.account_id,
            func.DATE(num_metric.c.metric_time).label("metric_time"),
            literal(metric_name_lu).label("metric_name_id"),
            case([
                (num_metric.c.num_value == None, 0),
                (den_metric.c.den_value == None, 0),
                (den_metric.c.den_value == 0, 0),
                (den_metric.c.den_value != 0, num_metric.c.num_value / den_metric.c.den_value)
            ], else_=0).label("metric_value"),
            case([
                (num_metric.c.num_value == None, 0),
                (den_metric.c.den_value == None, 0),
                (den_metric.c.den_value == 0, 0),
                (num_metric.c.num_value / den_metric.c.den_value > 0,
                 func.log(num_metric.c.num_value / den_metric.c.den_value))
            ], else_=0).label("metric_value_log")
        ) \
            .join(den_metric, and_(
            num_metric.c.account_id == den_metric.c.account_id,
            num_metric.c.metric_time == den_metric.c.metric_time
        ), isouter=True) \
            .order_by(num_metric.c.account_id, num_metric.c.metric_time)
        return qr

    def make_relative_metrics_part(self, d_obs_start, d_obs_end, cte_name, target_field, metric_name):
        Metric = self.T.Metric
        MetricName = self.T.MetricName
        session = self.get_session()
        num_metric = session.query(
            Metric.account_id,
            Metric.metric_time,
            Metric.metric_value.label(target_field)
        ) \
            .join(MetricName, Metric.metric_name_id == MetricName.metric_name_id) \
            .filter(MetricName.metric_name == metric_name,
                    Metric.metric_time.between(d_obs_start, d_obs_end)
                    ) \
            .order_by(Metric.account_id, Metric.metric_time) \
            .cte(cte_name)
        return num_metric

    def days_interval(self, d_start_date, d_end_date, step=7, label="date"):
        session = self.get_session()
        cnt = session.query(func.DATE(d_start_date).label(label)) \
            .cte(name="cnt", recursive=True)
        next_date = func.DATE(self.to_days(cnt.c[label]) + (step)).label(label)
        end_crit = next_date <= d_end_date
        if step < 0:
            end_crit = next_date >= d_end_date
        union_all = cnt.union_all(select([next_date], cnt).where(end_crit))
        return session.query(union_all)

    def add_metrics(self, newmetricname, fields):
        Metric = self.T.Metric
        MetricName = self.T.MetricName
        session = self.get_session()

        metric_name_lu = session.query(MetricName.metric_name_id) \
            .filter(MetricName.metric_name == newmetricname).first()  # or 0
        if metric_name_lu is not None:
            metric_name_lu = metric_name_lu[0]
            metric_name_id = metric_name_lu
            #print(f"new metric_name_id={metric_name_id} for {newmetricname}")
            # print(pretty_sql(qr))
            #ddf = pd.read_sql(qr.statement, dbhelper.engine)
            #print(ddf)

            if metric_name_id > 0:
                # delete all old values "new_metric_id"
                session.commit()
                old_metrics = session.query(Metric).filter(Metric.metric_name_id == metric_name_id).filter(
                    Metric.metric_name_id == -1)
                old_metrics.delete()
                session.commit()

                fields0=[]
                target_columns=[]
                for key, val in fields.items():
                    target_columns.append(key)
                    fields0.append(val)
                #new_metrics_insert = qr.cte("new_metrics_insert")
                select_stm = select(*fields0)
                #target_columns = ['account_id', 'metric_time', 'metric_name_id', 'metric_value']
                session.execute(Metric.__table__.insert().from_select(target_columns, select_stm))
                session.commit()
