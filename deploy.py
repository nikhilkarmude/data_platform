# python3 deploy.py bdist_wheel

from setuptools import setup, find_packages

setup(
    name="finance_data_platform",
    version="0.0.1",
    author="Nikhil Karmude",
    author_email="nikhilkarmude@domain.com",
    description="A brief description of your project",
    packages=find_packages(), 
    install_requires=[ 
        'boto3',
        'SQLAlchemy'
    ],
    scripts=['main.py'],  
    classifiers=[  
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
