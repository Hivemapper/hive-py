import argparse
import json
import requests
from requests.adapters import HTTPAdapter, Retry
from typing import Any, List, TypedDict, Dict, Union, Optional

from imagery.query import load_features, transform_input

BATCH_SIZE = 10000
DEFAULT_BACKOFF = 1.0
DEFAULT_RETRIES = 10
STATUS_FORCELIST = [429, 502, 503, 504, 524]

BURST_API_URL = 'https://hivemapper.com/api/developer/burst/create/'

# Create a session with retry strategy
request_session = requests.Session()
retries = Retry(
    total=DEFAULT_RETRIES,
    backoff_factor=DEFAULT_BACKOFF,
    status_forcelist=STATUS_FORCELIST,
    raise_on_status=True,
    allowed_methods=['GET', 'POST'],
)
request_session.mount('http://', HTTPAdapter(max_retries=retries))
request_session.mount('https://', HTTPAdapter(max_retries=retries))
# Define the type for a single burst entry
class GeoJSON(TypedDict):
    type: str
    coordinates: List[List[List[float]]]

class Burst(TypedDict):
    geojson: GeoJSON
    createdBy: str
    validUntil: str
    validFrom: str
    amount: int
    createdFrom: str
    organization: str
    credits: int
    hash: str
    status: str

# Define the overall return type of the function
class CreateBurstResult(TypedDict):
    success: bool
    bursts: List[Burst]
    creditsRemaining: int

def post_request(
    url: str, 
    headers: Dict[str, str], 
    data: Dict[str, Any], 
    verbose: bool = False
) -> Union[Dict[str, Any], list]:
    """
    Performs a POST request with retries and error handling.

    Args:
        url (str): The URL for the POST request.
        headers (Dict[str, str]): Headers to include in the request.
        data (Dict[str, Any]): The data to send in the POST request.
        verbose (bool, optional): If True, print verbose error messages. Defaults to False.

    Returns:
        Union[Dict[str, Any], list]: The JSON response if the request is successful,
                                     or an empty list if a server error occurs and verbose is True.
    """
    with request_session.post(url, data=json.dumps(data), headers=headers) as r:
        try:
            try:
                response_json = r.json()
                if "error" in response_json:
                    http_json_error_msg = response_json["error"]
                    print(http_json_error_msg)
            except json.JSONDecodeError:
                pass
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 500:
                if verbose:
                    print('Encountered a server error, skipping:')
                    print(e)
                return []
            else:
                raise e
        except requests.exceptions.RetryError as e:
            if verbose:
                print('Encountered a retry error, skipping:')
                print(e)
            return []
        resp = r.json()
        return resp

def get_request(
    url: str, 
    headers: Optional[Dict[str, str]] = None, 
    params: Optional[Dict[str, Any]] = None, 
    verbose: bool = False
) -> Union[Dict[str, Any], list]:
    """
    Performs a GET request with retries and error handling.

    Args:
        url (str): The URL for the GET request.
        headers (Optional[Dict[str, str]]): Headers to include in the request. Defaults to None.
        params (Optional[Dict[str, Any]]): Query parameters to include in the request. Defaults to None.
        verbose (bool, optional): If True, print verbose error messages. Defaults to False.

    Returns:
        Union[Dict[str, Any], list]: The JSON response if the request is successful,
                                     or an empty list if a server error occurs and verbose is True.
    """
    with request_session.get(url, headers=headers, params=params) as r:
        try:
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 500:
                if verbose:
                    print('Encountered a server error, skipping:')
                    print(e)
                return []
            else:
                raise e
        except requests.exceptions.RetryError as e:
            if verbose:
                print('Encountered a retry error, skipping:')
                print(e)
            return []
        except json.JSONDecodeError:
            if verbose:
                print('Failed to decode JSON response:')
                print(r.text)
            return []

def create_bursts(geojson_file_path: str, authorization: str, verbose=False) -> CreateBurstResult:
    """
    Create bursts from a GeoJSON file. Each burst location costs 125 credits.

    Args:
        geojson_file_path (str): Path to the GeoJSON file.
        authorization (str): Basic Authorization token.
        verbose (bool, optional): If True, print verbose error messages. Defaults to False.
    
    Returns:
        CreateBurstResult: A dictionary containing the success status, list of bursts created, and remaining
                            credits after the operation.
    """
    headers = {
        'Authorization': authorization,
        'Content-Type': 'application/json'
    }

    geojson_file = transform_input(
        file_path=geojson_file_path,
        verbose=verbose,
         use_cache=False,
    )

    features, _, _ = load_features(geojson_file, verbose)

    # format the features into array of geometries, json format [geojson: {geometry}]
    polygons = [{"geojson": feature["geometry"]} for feature in features]

    return post_request(BURST_API_URL, headers=headers, data=polygons, verbose=verbose)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', type=str, required=True)
    parser.add_argument('-a', '--authorization', type=str, required=True)
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    bursts = create_bursts(args.input_file, args.authorization, verbose=args.verbose)