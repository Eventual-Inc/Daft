from setuptools import setup

setup(
    name="icebridge",
    packages=["icebridge"],
    package_dir={"": "python"},
    install_requires=[
        "py4j ==0.10.9.5",
    ],
)
