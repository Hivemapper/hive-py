import argparse
import json
import requests

API_URL_BALANCE = 'https://hivemapper.com/api/developer/balance'
API_URL_HISTORY = 'https://hivemapper.com/api/developer/history'

DEFAULT_LIMIT = 25
DEFAULT_SKIP = 0

def headers(auth):
  return {
    "content-type": "application/json",
    "authorization": f'Basic {auth}',
  }

def display_balance(auth):
  with requests.get(API_URL_BALANCE, headers=headers(auth)) as r:
    r.raise_for_status()
    resp = r.json()
    balance = resp['balance']
    print(f'Remaining API credits: {balance}')

def format_transaction(transaction, verbose = False):
  area = transaction['area']
  timestamp = transaction['timestamp']
  credits = transaction['credits']
  payload = transaction['payload']
  weeks = transaction['weeks']

  lines = [
    f'> {timestamp}',
    f'   credits: {credits}'
  ]

  if verbose:
    lines += [
      f'   area: {area:.2f} m^2,',
      f'   weeks: {", ".join(weeks)}',
      json.dumps(payload, indent=2),
    ]

  return  '\n'.join(lines)

def display_history(auth, limit, verbose = False):
  i = 0
  while i < limit:
    lim = min(limit, DEFAULT_LIMIT)
    url = f'{API_URL_HISTORY}?limit={lim}&skip={i}'
    with requests.get(url, headers=headers(auth)) as r:
      r.raise_for_status()
      resp = r.json()
      history = resp['history']
      if not history:
        return

      for transaction in history:
        print(format_transaction(transaction, verbose))

    i += lim

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('-a', '--authorization', type=str, required=True)
  parser.add_argument('-b', '--balance', action='store_true')
  parser.add_argument('-l', '--limit', type=int, default=DEFAULT_LIMIT)
  parser.add_argument('-t', '--history', action='store_true')
  parser.add_argument('-v', '--verbose', action='store_true')

  args = parser.parse_args()
  auth = args.authorization

  if args.balance:
    display_balance(auth)

  if args.history:
    display_history(auth, args.limit, args.verbose)
