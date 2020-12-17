from sqlalchemy import MetaData, create_engine, Table
from sqlalchemy.ext.declarative import declarative_base
from re import sub
import tempfile
import sys
import os
import shutil

Base = declarative_base()


# function to convert string to camelCase
def camelCase(string):
    string = sub(r"(_|-)+", " ", string).title().replace(" ", "")
    # return string[0].lower() + string[1:]
    return string[0].upper() + string[1:]


def get_or_default(dict, key, alternative=None):
    if key in dict:
        return dict[key]
    return alternative


def get_db_uri(options, dialect="sqlite"):
    database_uri = f"<Error: SQL dialect '{dialect}' not implemented>"
    if dialect == "sqlite":
        database_uri = f"{dialect}:///:memory:"
        sqlitefile = get_or_default(options, "filename")
        if sqlitefile is not None:
            database_uri = f"{dialect}:///{sqlitefile}"
    elif dialect == "postgres":
        user = get_or_default(options, "user", "postgres")
        pw = get_or_default(options, "pass", "password")
        dbname = get_or_default(options, "dbname", "churn")
        host = get_or_default(options, "host", "localhost")
        port = get_or_default(options, "port", "5432")
        database_uri = f"{dialect}://{user}:{pw}@{host}:{port}/{dbname}"
    return database_uri


def get_schema(options):
    """
    return the dynamical loaded schema module cotaining wrapper classes of the DB tables
    :param options:
        dict containing user, pass, dbname, schema, host, port
    :type options:
    :return:
    :rtype:
    """
    module = None
    tmp = tempfile.NamedTemporaryFile(mode="w+t", delete=False, suffix=".py")
    text = _howto_do_it(options)

    somedir = os.path.dirname(tmp.name)
    module_name = os.path.basename(tmp.name)[:-3]
    hold_path = sys.path
    sys.path.insert(1, somedir)
    try:
        tmp.write(text)
        tmp.flush()
        module = __import__(module_name)
        # for key, val in module.__dict__.items():
        #     print(key)
    finally:
        tmp.close()  # deletes the file
        try:
            os.remove(tmp.name)
            # we protect the user: some py files could contain user password informations
            shutil.rmtree(f"{somedir}/__pycache__")
        except:
            pass

    sys.path = hold_path
    return module



def _howto_do_it(options):
    user = get_or_default(options, "user", "postgres")
    pw = get_or_default(options, "pass", "password")
    dbname = get_or_default(options, "dbname", "churn")
    schema = get_or_default(options, "schema", "biznet1")
    host = get_or_default(options, "host", "localhost")
    port = get_or_default(options, "port", "5432")

    database_uri = f"postgresql://{user}:{pw}@{host}:{port}/{dbname}"
    engine = create_engine(database_uri)

    meta = MetaData()
    meta.reflect(bind=engine, schema=schema)
    start_text = ""
    start_text += f"\nfrom sqlalchemy import MetaData, create_engine, Table"
    start_text += f"\nfrom sqlalchemy.ext.declarative import declarative_base"
    start_text += f"\ndatabase_uri = \"postgresql://{user}:{pw}@{host}:{port}/{dbname}\""
    start_text += f"\nengine = create_engine(database_uri)"
    start_text += f"\nBase = declarative_base(bind=engine)"
    start_text += f"\nBase.metadata.schema = '{schema}'"
    # start_text += f"\nprint(Base.__dict__)"
    for table in meta.tables.values():
        tblname=table.name
        tblname=tblname.split(".")[-1]
        classname = camelCase(tblname)
        new_table_class = f"class {classname}(Base):"
        new_table_class += f"\n\t__table__ = Table('{table.name}', Base.metadata, autoload=True)"
        # new_table_class += f"\n\t__table_args__ = {{'schema': '{schema}', 'autoload': True}}"
        start_text += f"\n\n{new_table_class}"
    start_text += "\n"

    # print(start_text)

    # exec(start_text)
    return start_text
