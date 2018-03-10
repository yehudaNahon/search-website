from setuptools import setup

setup(
    name='koogle',
    packages=['koogle'],
    include_package_data=True,
    install_requires=[
        'flask',
        'bs4',
        'requests'
    ],
)