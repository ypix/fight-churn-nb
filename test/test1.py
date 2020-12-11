from datetime import date
import os

from churnmodels.simulation import simulate


def test1():
    options = {
        "model": os.getenv("CHURN_MODEL") or "biznet1",
        # "start": date(2020, 1, 1),
        # "end": date(2020, 6, 1),
        # "seed": 5432,
        "init_customers": 10,
    }
    simulate(options)
    # options["model"]="socialnet7"
    # simulate(options)
    pass


if __name__ == '__main__':
    test1()
