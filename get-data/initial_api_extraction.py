############################
# FUNCTIONS FOR EXTRACTING DATA FROM ONS API
############################

import pandas as pd
import requests


def get_ashe_datasets(
    endpoint: str = "https://api.beta.ons.gov.uk/v1",
    search_terms: list = ["ashe", "earnings"]
    ) -> pd.DataFrame:
    
    """
    Retrieves and filters ONS datasets related to ASHE (Annual Survey of Hours and Earnings) or earnings.

    This function queries the UK Office for National Statistics (ONS) API to retrieve metadata
    about all available datasets, then filters the datasets based on the presence of specified
    search terms (by default, "ashe" and "earnings") in the dataset keywords.

    Parameters:
        endpoint (str): The base URL of the ONS API. Defaults to the beta endpoint.
        search_terms (list): A list of search keywords to filter relevant datasets. Defaults to ["ashe", "earnings"].

    Returns:
        pd.DataFrame: A DataFrame containing metadata of datasets matching the search terms,
        including fields like 'id', 'title', and associated keywords.

    Raises:
        Exception: If the API request fails or returns a non-200 HTTP status code.
    """
    
    #make query and get response
    try:
        dataset_resp = requests.get(endpoint + "/datasets")
    except:
        raise Exception("Failed to connect to ONS API endpoint. Please check the URL or your internet connection.")
    
    #check response
    if dataset_resp.status_code !=200:
        raise Exception(f"Error: Status code: {dataset_resp.status_code}")
    
    #parse response
    dataset_json = dataset_resp.json()
    
    #total count
    items = dataset_json["total_count"]
    
    #make request with all responses
    params = {"limit": items}
    
    try:
        dataset_resp = requests.get(endpoint + "/datasets", params = params)
    except:
        raise Exception("Failed to connect to ONS API endpoint. Please check the URL or your internet connection.")
    
    #check response
    if dataset_resp.status_code !=200:
        raise Exception(f"Error: Status code: {dataset_resp.status_code}")
    
    #extract items
    df = pd.DataFrame(dataset_json["items"])
    
    #collapse search terms
    search_terms = "|".join(search_terms)
    
    #explode and filter to ashe data, then dedepuplicate
    unnested = df.explode("keywords")
    ashe_check = unnested[unnested["keywords"].str.contains(search_terms, case = False, na = False)]
    ashe_check.drop_duplicates(subset = "id", inplace = True)
    
    return ashe_check
    
    
    
    