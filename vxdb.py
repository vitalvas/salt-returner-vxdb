import time
import json
import requests
import logging
import salt.returners
import salt.utils.jid
import salt.utils.json

log = logging.getLogger(__name__)


# Define the module's virtual name
__virtualname__ = 'vxdb'

def __virtual__():
  return __virtualname__

def _get_options(ret=None):
  defaults = {
    "address": "http://127.0.0.1:8080"
  }
  attrs = {
    "address": "address"
  }
  _options = salt.returners.get_returner_options(
    __virtualname__, ret, attrs, __salt__=__salt__, __opts__=__opts__, defaults=defaults
  )
  return _options

def _update_resources(ret):
  opts = _get_options(ret)
  addr = opts['address']

  minion = ret["id"]
  fun = ret["fun"]
  jid = ret["jid"]
  r = requests.put(
    f"{addr}/returner/{minion}/{jid}/{fun}",
    data=salt.utils.json.dumps(ret),
    headers={
      'x-key-ttl': str(90 * 86400)
    }
  )
  r.raise_for_status()
  r2 = requests.put(
    f"{addr}/state/{minion}",
    data=json.dumps({
      'timestamp': int(time.time()),
      'jid': jid,
      'fun': fun,
      'retcode': ret.get('retcode'),
      'success': ret.get('success')
    }),
    headers={
      'x-key-ttl': str(90 * 86400)
    }
  )
  r2.raise_for_status()


def returner(ret):
  """
  Return data to a vxdb data store
  """
  supported_funcs = ['state.sls', 'state.apply', 'state.highstate']
  if ret.get('fun') in supported_funcs:
    _update_resources(ret)


def prep_jid(nocache=False, passed_jid=None):
  """
  Do any work necessary to prepare a JID, including sending a custom id
  """
  return passed_jid if passed_jid is not None else salt.utils.jid.gen_jid(__opts__)


def save_load(jid, load, minions=None):
  """
  Save the load to the specified jid
  """
  opts = _get_options({})
  addr = opts['address']
  r = requests.put(
    f"{addr}/load/{jid}",
    data=salt.utils.json.dumps(load),
    headers={
      'x-key-ttl': str(30 * 86400),
    }
  )
  r.raise_for_status()

def get_load(jid):
  """
  Return the load data that marks a specified jid
  """
  opts = _get_options({})
  addr = opts['address']
  r = requests.get(f"{addr}/load/{jid}")
  if r.status_code == 200:
    return salt.utils.json.loads(r.text)
  elif r.status_code == 404:
    return {}
  else:
    r.raise_for_status()
 
