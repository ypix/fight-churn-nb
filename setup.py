from setuptools import setup, find_packages
import churnmodels

print(find_packages())

setup(
    name='churnmodels',
    version=churnmodels.__version__,
    packages=find_packages(),
    include_package_data=True,
    url='',
    license='',
    author='Kai',
    author_email='',
    description=''
)
