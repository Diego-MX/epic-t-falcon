# DX, Epic Bank
# CDMX, 17 octubre '23

EPIC_REF = 'gh-1.6' 
REQS_FILE = '../reqs_dbks.txt'

import subprocess
subprocess.check_call(['pip', 'install', '--upgrade', 'typing-extensions==4.7.1'])
subprocess.check_call(['pip', 'install', '-r', REQS_FILE])

import yaml
with open("../user_databricks.yml", 'r') as _f:
    u_dbks = yaml.safe_load(_f)

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
epicpy_load = {
    'url'   : 'github.com/Bineo2/data-python-tools.git', 
    'ref'   : EPIC_REF, 
    'token' : dbutils.secrets.get(u_dbks['dbks_scope'], u_dbks['dbks_token'])}
subprocess.check_call(['pip', 'install', 
    "git+https://{token}@{url}@{ref}".format(**epicpy_load)])


import epic_py
print(f"""EpicPy
Referencia\t: {EPIC_REF}
Version TOML\t: {epic_py.__version__}""")



