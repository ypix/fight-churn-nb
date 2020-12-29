# fight-churn-nb


This repository was created against the background that the 
above mentioned book depends on a Postgres DB installation. 
Here, a SQLite DB was used as an alternative, which, 
being file-based, is easier to use. 
In addition, the Jupyter environment was chosen, 
which allows experimental single steps to be taken 
and intermediate results to be viewed graphically.

The creation of simulation data on churn behavior 
is implemented by means of the following pPython libraries:
* SQLAlchemy
* pandas
* jupyter

## Prerequisites
Prerequisties like creating a python environment with anaconda, 
creating and installing the churnmodels package
and running the simulation are described in a jupyter notebook:
* [DBs using schemas](listings/part0/jup_chap01_regarding-db-schemas.ipynb)
* [install package and simulate data](listings/part0/jup_chap01_simulation.ipynb)

When using SQLite3 within SQLAlchemy the extension functions need to be compiled to 
dynamic library. Details for creating the dynamic libraries can be found in
* [Generate dynamic library on extension functions for SQLite3](ext_lib/how-to.ipynb)


## Tests
The code  has been tested for the data bases 
* SQLite (v3.33) and
* Postgres (v4).

The usage of SQLAlchemy does not guarantee, that a switch to a data base 
may not cause any problems when running the python codes.


## References
This repository contains Python code on the book "fight churn with data" by Carl Gold.
The repository refers to the original code on github
* https://github.com/carl24k/fight-churn

The SQLite sources have been taken from 
* https://github.com/cloudmeter/sqlite

The SQLite extensions DLL file for windows has been compiled with win-builds 
* http://win-builds.org/doku.php





