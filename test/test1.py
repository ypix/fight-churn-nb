from datetime import date
import os

from churnmodels.simulation import simulate


def test1():
    model = "biznet1"
    options = {
        "model": model,
        "start": date(2020, 1, 1),
        "end": date(2020, 6, 1),
        "seed": 5432,
        "init_customers": 100,
        "schema":model
    }
    simulate(options)
    pass

def test2():
    model = "biznet1"
    options = {
        "model": model,
        "start": date(2020, 1, 1),
        "end": date(2020, 6, 1),
        "seed": 5432,
        "init_customers": 100,
        "schema":model
    }
    simulate(options)
    pass


if __name__ == '__main__':
    test1()
