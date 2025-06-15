############################
# FUNCTIONS FOR EXTRACTING DATA FROM ONS API
############################

import pandas as pd
import requests

### Generic query of ons api
def query_ons_api(url: str) -> dict:
    
    """
    Queries the UK Office for National Statistics (ONS) API and retrieves the full dataset.

    This function performs an initial request to determine the total number of items available
    at the specified API endpoint. It then makes a second request to fetch all items using
    the appropriate limit parameter.

    Parameters:
        url (str): The base URL of the ONS API endpoint.

    Returns:
        dict: A dictionary containing the full JSON response from the ONS API.

    Raises:
        Exception: If the API request fails due to connection issues or returns a non-200 status code.
    """    
    
    #make query and get response
    try:
        resp = requests.get(url)
    except:
        raise Exception("Failed to connect to ONS API endpoint. Please check the URL or your internet connection.")
    
    #check response
    if resp.status_code !=200:
        raise Exception(f"Error: Status code: {resp.status_code}")
    
     #parse response
    json = resp.json()
    
    #total count
    items = json["total_count"]
    
    #make request with all responses
    params = {"limit": items}
    
    try:
        resp = requests.get(url, params = params)
    except:
        raise Exception("Failed to connect to ONS API endpoint. Please check the URL or your internet connection.")
    
    #check response
    if resp.status_code !=200:
        raise Exception(f"Error: Status code: {resp.status_code}")
    
    #parse response
    json = resp.json()
    
    return json
    

### Access the ids for interesting datasets and associated metadata
def get_ashe_datasets(
    endpoint: str = "https://api.beta.ons.gov.uk/v1",
    search_terms: list = ["ashe", "earnings"]
    ) -> pd.DataFrame:
    
    """
    Retrieves and filters datasets from the UK Office for National Statistics (ONS) API
    to return only those related to the Annual Survey of Hours and Earnings (ASHE).

    This function queries the ONS datasets endpoint, converts the response to a DataFrame,
    and filters datasets based on specified search terms in the 'keywords' field.

    Parameters:
        endpoint (str): Base URL of the ONS API. Defaults to the official beta API endpoint.
        search_terms (list): List of keywords to match in the dataset metadata. Defaults to ["ashe", "earnings"].

    Returns:
        pd.DataFrame: A DataFrame containing metadata for datasets matching the search terms,
                      deduplicated by dataset ID.

    Raises:
        Exception: Propagates exceptions from `query_ons_api` if API requests fail.
    """
    
    #make query and get response
    dataset_json = query_ons_api(f"{endpoint}/datasets")
    
    #extract items
    df = pd.DataFrame(dataset_json["items"])
    
    #collapse search terms
    search_terms = "|".join(search_terms)
    
    #explode and filter to ashe data, then dedepuplicate
    unnested = df.explode("keywords")
    ashe_check = unnested[unnested["keywords"].str.contains(search_terms, case = False, na = False)]
    ashe_check.drop_duplicates(subset = "id", inplace = True)
    
    return ashe_check
    
### Find version data from dataset id; from there can download observations and dimensional data
def get_versions_from_datasets(
    dataset_id: str,
    source_df: pd.DataFrame
    ) -> pd.DataFrame:
        
    #filter source df to pertinent dataset
    source_df = source_df[source_df["id"] == dataset_id]
    
    #extract links
    links = source_df["links"].apply(pd.Series)
    
    #extract editions
    editions = links["editions"].apply(pd.Series)
    
    #list of hrefs from editions
    edition_hrefs = editions["href"].tolist()
    
    #list of responses for each href
    editions_responses = [query_ons_api(h) for h in edition_hrefs]
    
    #extract items from each response
    edition_items = [pd.DataFrame(e["items"]) for e in editions_responses]
    
    #concat into single df
    edition_items_df = pd.concat(edition_items, ignore_index = True)
    
    #extract links
    edition_links = edition_items_df["links"].apply(pd.Series)
    
    #then versions
    versions = edition_links["versions"].apply(pd.Series)
    
    #version hrefs
    version_hrefs = versions["href"].tolist()
    
    #version responses
    version_responses = [query_ons_api(v) for v in version_hrefs]
    
    #extract items from each response
    version_items = [pd.DataFrame(v["items"]) for v in version_responses]
    
    #return
    return pd.concat(version_items, ignore_index = True)
    
#download observations from versions
def download_observations_from_versions(version_id: str, source_df: pd.DataFrame) -> pd.DataFrame:
    
    #filter to pertinent version
    source_df = source_df[source_df["id"] == version_id]
    
    #get downloads
    downloads = source_df["downloads"].apply(pd.Series)
    downloads = pd.concat([source_df.drop(columns = "downloads"), downloads], axis = 1)
    downloads = downloads[~downloads["csv"].isna()]
    
    #extract hrefs
    hrefs = [d["href"] for d in downloads["csv"]]
    
    #dataset ids
    dataset_ids = downloads["dataset_id"].tolist()
    
    #versions
    versions = downloads["version"].tolist()
    
    #download each csv
    for i in range(len(hrefs)):
        try:
            resp = requests.get(hrefs[i])
            if resp.status_code == 200:
                #construct save path
                save_path = f"bronze-files/{dataset_ids[i]}_{versions[i]}.csv"
                #write file
                with open(save_path, "wb") as f:
                    f.write(resp.content)
            else:
                raise Exception(f"Failed to download CSV from {hrefs[i]}. Status code: {resp.status_code}")
        except:
            raise Exception("Failed to connect to ONS API endpoint. Please check the URL or your internet connection.")
    
    
    
    
    
        
        
        
    
    
    