# fight-churn-nb

This repository contains Python code on the book
"fight churn with data" by Carl Gold. 

The repository refers to the original code on github
* https://github.com/carl24k/fight-churn

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
* [schema handling for DBs](listings/chap01/jup_chap01_regarding-db-schemas.ipynb)
* [install package and simulate data](listings/chap01/jup_chap01_simulation.ipynb)

## Tests
The repository has been tested for the data bases SQLite (v3.33) and Postgres (v4).
Be aware that the usage of SQLAlchemy does not gaarantee, that a switch of a data base 
may cause problems in running the python codes.









