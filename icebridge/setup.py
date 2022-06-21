from setuptools import setup, find_packages

setup(
    name='icebridge',
    packages=find_packages(where="app/build/libs"),
    package_dir={"": "app-uber.jar"},
    include_package_data=True
)
