from setuptools import setup

setup(name='mednickdb_pyapi',
      version='0.1',
      description='python API for accessing and posting data and files to the mednickdb',
      url='https://github.com/MednickLab/mednickdb_pyapi',
      author='Ben Yetton',
      author_email='bdyetton@gmail.com',
      license='MIT',
      packages=['mednickdb_pyapi'],
      install_requires=['requests','pytest','pytest-dependency', 'datetime', 'numpy'],
      zip_safe=False)