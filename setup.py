from distutils.core import setup

setup(
    name='check_sql_queries',
    version='1.0',
    packages=['sql_client'],
    url='',
    license='',
    author='daniel.ricart',
    author_email='daniel.ricart@scmspain.com',
    description='run SQL count(*) queries against MSSQL server databases',
    requires=['pypyodbc', 'requests']
)
