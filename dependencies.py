"""
Unificar la instalación de dependencias (pip_reqs.txt) en los diferentes servidores: 
- Si es local el archivo '.env' tiene variables relevantes. 
- Si es Databricks, el archivo 'user_databricks.json' vincula con el llavero correspondiente.

En cualquier caso se confirma un token de github: GH_ACCESS_TOKEN. 
"""

# pylint: disable=unspecified-encoding
# pylint: disable=undefined-variable
import json 
import os
from pathlib import Path
import re
from subprocess import check_call

from config import USER_DBKS, REQS_FILE, EPIC_REF 

EPIC_URL = "Bineo2/data-python-tools"

spacer = ' '.split

def dotenv_manual(env_file='.env'):
    rm_comment = lambda ss: re.sub('#.*$', '', ss)
    secret_reg = r'([A-Z_]*) ?= ?\"?([^\s\=]*)\"?'
    with open(env_file, 'r') as e_file: 
        no_comments = map(rm_comment, e_file.readlines())
    the_secrets = {mm.group(1): mm.group(2)
        for ll in no_comments if (mm := re.match(secret_reg, ll))}         
    os.environ.update(the_secrets)


def token_from_server(): 
    if 'DATABRICKS_RUNTIME_ENVIRONMENT' in os.environ: 
        with open(USER_DBKS, 'r') as j_file: 
            tokener = json.load(j_file)
        the_token = dbutils.secrets.get(tokener['dbks_scope'], tokener['dbks_token'])
        os.environ['GH_ACCESS_TOKEN'] = the_token
    elif Path('.env').is_file(): 
        dotenv_manual('.env')
    else: 
        raise EnvironmentError("Cannot set Github token to install from Github.")


def install_reqs(): 
    check_call(spacer(f"pip install --requirement {REQS_FILE}"))


def install_epicpy(ref=None, mode=None, **kwargs):
    ref = ref or EPIC_REF
    mode = mode or 'https'
    if mode == 'https': 
        token = os.environ['GH_ACCESS_TOKEN']
        w_url = f"git+https://{token}@github.com/{EPIC_URL}@{ref}"
    elif mode == 'ssh': 
        if 'host' in kwargs:
            prehost = kwargs['host']
        else: 
            warn("May need to provide SSH host from ~/.ssh/config")
            prehost = 'github.com'
        w_url = f"git+ssh://{prehost}:{EPIC_URL}@{ref}"
    check_call(spacer(f"pip install {w_url}"))



if __name__ == '__main__': 
    if 'GH_ACCESS_TOKEN' not in os.environ: 
        token_from_server()
    install_reqs()