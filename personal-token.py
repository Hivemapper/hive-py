import base64
import argparse

def get_personal_token(user_name, api_key):
    string_to_encode = f"{user_name}:{api_key}"
    encoded_bytes = base64.b64encode(string_to_encode.encode("utf-8"))
    encoded_string = encoded_bytes.decode("utf-8")

    return encoded_string

if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=False)

    parser.add_argument('-h', '--help', action='help', default=argparse.SUPPRESS,
                    help='Input the user name and api key to return a base64 encoded string for all Hive-Py API requests.')

    parser.add_argument('-u', '--user_name', type=str, required=True, help="User name of the account listed on profile: https://hivemapper.com/account/profile")
    parser.add_argument('-k', '--api_key', type=str, required=True, help="Generate an API key here: https://hivemapper.com/console/developers/api-key")

    args = parser.parse_args()

    print(get_personal_token(args.user_name, args.api_key))
