from setuptools import setup, find_packages

setup(
  name='eagle',
  version='0.0.2',
  author_email='xhd0216@gmail.com',
  url='https://github.com/xhd0216/goldwater',
  packages=find_packages(),
  entry_points={
    'console_scripts': [
      'getCOT = cot_retriever.main:main',
    ]
  },
  install_requires=[
    'argparse',
    'requests',
    'sqlalchemy',
  ],
)
