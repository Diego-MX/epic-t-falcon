# DX, Epic Bank
# CDMX, 17 octubre '23
# Wrapper para PIP INSTALL en un mismo job. Se debe ejecutar con cuidado. 

from json import dumps
from subprocess import check_call
from sys import argv

from pkg_resources import working_set
from pyspark.dbutils import DBUtils     # pylint: disable=import-error,no-name-in-module
from pyspark.sql import SparkSession

has_yaml = 'yaml' in working_set.by_key

USER_FILE = "../user_databricks.yml"
REQS_FILE = "../reqs_dbks.txt"
EPIC_REF = 'meetme-1'
V_TYPING = '4.7.1'


def from_reqsfile(a_file=None): 
    a_file = a_file or REQS_FILE
    pip_install('-r', a_file)
    return 

def gh_epicpy(ref=None, tokenfile=None, typing=None, verbose=False): 
    if typing: 
        v_typing = V_TYPING if typing is True else typing
        pip_install('--upgrade', f"typing-extensions=={typing}")
    the_keys = {
        'url'  : 'github.com/Bineo2/data-python-tools.git', 
        'token': token_from_userfile(tokenfile), 
        'ref'  : ref}
    pip_install("git+https://{token}@{url}@{ref}".format(**the_keys))
    if verbose: 
        import epic_py
        dumper = {'Epic Ref': ref, 'Epic Ver': epic_py.__version__}
        print(dumps(dumper))
    return
    
def token_from_userfile(userfile):
    if not has_yaml: 
        pip_install('pyyaml==6.0.1')
    from yaml import safe_load
    with open(userfile, 'r') as _f:        # pylint: disable=unspecified-encoding
        tokener = safe_load(_f)
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    return dbutils.secrets.get(tokener['dbks_scope'], tokener['dbks_token']) 
    
def pip_install(*args): 
    check_call(['pip', 'install', *args])
    return


if __name__ == '__main__': 
    # pylint: disable=invalid-name
    epic_ref  = argv[1] if len(argv) > 1 else EPIC_REF
    reqs_file = argv[2] if len(argv) > 2 else REQS_FILE
    user_file = argv[3] if len(argv) > 3 else USER_FILE
    v_typing  = argv[4] if len(argv) > 4 else V_TYPING
    
    from_reqsfile(reqs_file)
    gh_epicpy(epic_ref, user_file, v_typing, False)
    