############################
# FUNCTIONS FOR EXTRACTING DATA FROM ONS API
############################

import pandas as pd
import requests

from utils.directory_navigation import find_project_root

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
def download_observations_from_versions(version_id: str, source_df: pd.DataFrame):
    
    """
    Downloads CSV observation files for a specific version from a provided DataFrame of dataset metadata.

    This function filters the input DataFrame to rows matching the given `version_id`, extracts download
    URLs from the "downloads" column (assumed to contain nested dictionaries), and attempts to download
    the associated CSV files. Downloaded files are saved to the local `bronze_files/` directory, named 
    according to the format `{dataset_id}_{version}.csv`.

    Parameters:
        version_id (str): The ID of the version to filter and download observations for.
        source_df (pd.DataFrame): A DataFrame containing metadata including "id", "downloads", 
                                  "dataset_id", and "version" columns. The "downloads" column is 
                                  expected to contain dictionaries with a "csv" key that includes a "href".

    Returns:
        pd.DataFrame: The filtered and flattened DataFrame used for downloading, including dataset ID,
                      version, and CSV download information.

    Raises:
        Exception: If a file fails to download due to a bad response or a connection error.
    """
    
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
    root = find_project_root() 
    for i in range(len(hrefs)):
        try:
            resp = requests.get(hrefs[i])
            if resp.status_code == 200:
                #construct save path
                save_path = f"{root}/bronze_files/{dataset_ids[i]}_{versions[i]}.csv"
                #write file
                with open(save_path, "wb") as f:
                    f.write(resp.content)
            else:
                raise Exception(f"Failed to download CSV from {hrefs[i]}. Status code: {resp.status_code}")
        except:
            raise Exception("Failed to connect to ONS API endpoint. Please check the URL or your internet connection.")
    
#download observations from versions
def download_dimensions_from_versions(source_df: pd.DataFrame):  
    
    """
    Downloads and saves unique dimension data across all dataset versions from the ONS API.

    This function:
    1. Iterates through all version IDs in source_df.
    2. Collects all dimension 'code' hrefs, ensuring no duplicates.
    3. Queries each unique 'code' href to fetch dimension codes.
    4. Saves each dimension's code list as a CSV file.

    Parameters:
        source_df (pd.DataFrame): A DataFrame with 'id' and 'dimensions' columns.

    Returns:
        pd.DataFrame: A concatenated DataFrame of all retrieved dimension codes.
    """
    #dict to all hold code refs
    all_code_hrefs = {}
    
    #loop through each version id
    for version_id in source_df["id"].unique():
        version_df = source_df[source_df["id"] == version_id]
        if version_df.empty:
            print(f"[WARNING] No data for version_id: {version_id}")
            continue

        #explode the resulting dimensions df and extract hrefs from it
        dimensions = version_df.explode("dimensions")["dimensions"].apply(pd.Series)
        if dimensions.empty:
            print(f"[WARNING] No dimensions extracted for version_id: {version_id}")
            continue

        hrefs = dimensions["href"].dropna().unique().tolist()

        #query hrefs and get edition dfs in return
        resp_json = [requests.get(h).json() for h in hrefs]
        resp_dfs = [pd.DataFrame(r) for r in resp_json]
        edition_dfs = [r.loc[["editions"]] for r in resp_dfs if "editions" in r.index]
        
        #extract links from each edition df
        edition_df = pd.concat(edition_dfs, ignore_index=True)
        links = edition_df["links"].apply(pd.Series)
        link_hrefs = links["href"].dropna().tolist()

        link_responses = [query_ons_api(l) for l in link_hrefs]
        link_items = [pd.DataFrame(l["items"]) for l in link_responses if "items" in l]

        link_df = pd.concat(link_items, ignore_index=True)
        dim_link_dicts = link_df["links"]

        for idx, d in enumerate(dim_link_dicts):
            try:
                code_href = d["codes"]["href"]
                dim_name = code_href.split("/")[5]
                all_code_hrefs[code_href] = dim_name
            except Exception as e:
                print(f"[WARNING] Skipping invalid link dict at index {idx}: {e}")

    #save outputs
    root = find_project_root()

    for href, dim_name in all_code_hrefs.items():
        code_data = query_ons_api(href)

        df = pd.DataFrame(code_data["items"]).drop(columns = "links", errors = "ignore")
        df["dimension"] = dim_name
        df.to_csv(f"{root}/bronze_files/{dim_name}.csv", index = False)
    
#download inflation
def download_inflation(dataset_id = "cpih01"):     
    
    """
    Downloads the latest version of an inflation dataset from the UK Office for National Statistics (ONS) API.

    This function searches for inflation-related datasets using the ASHE (Annual Survey of Hours and Earnings)
    API, identifies the dataset matching the specified `dataset_id`, retrieves its latest version URL, and downloads
    the corresponding CSV file. The CSV is saved to the `bronze_files` directory at the project root as `cpih.csv`.

    Args:
        dataset_id (str): The unique identifier of the inflation dataset to download.
                          Defaults to "cpih01", which typically corresponds to the Consumer Prices Index
                          including owner occupiers’ housing costs (CPIH).

    Raises:
        Exception: If the dataset metadata cannot be retrieved.
        Exception: If the API request for the latest dataset version fails.
        Exception: If the CSV file cannot be downloaded or saved.

    Returns:
        None
    """
    
    #dataset df
    dataset_json = get_ashe_datasets(search_terms = "inflation") 
    dataset_df = pd.DataFrame(dataset_json)
    
    #cpih df
    cpih_df = dataset_df[dataset_df["id"] == dataset_id]
    
    #cpih links
    links = cpih_df["links"].apply(pd.Series) #turn links dict into a df
    links_df = pd.concat([cpih_df.drop(columns = "links"), links], axis = 1) #replace links dict with above df
    
    #latest
    latest_url = links["latest_version"].tolist()[0]["href"]
    
    #query
    try:
        cpih_resp = requests.get(latest_url)
    except:
        raise Exception("Failed to connect to ONS API endpoint for latest version. Please check the URL or your internet connection.")
    
    if cpih_resp.status_code != 200:
        raise Exception(f"Error: Status code: {cpih_resp.status_code} when requesting latest version.")
        
    #download url
    download_url = cpih_resp.json().get("downloads").get("csv").get("href")
    
    #save outputs
    root = find_project_root()
    
    try:
        resp = requests.get(download_url)
        if resp.status_code == 200:
            #construct save path
            save_path = f"{root}/bronze_files/cpih.csv"
            #write file
            with open(save_path, "wb") as f:
                f.write(resp.content)
        else:
            raise Exception(f"Failed to download CPIH csv. Status code: {resp.status_code}")
    except:
        raise Exception("Failed to connect to ONS API endpoint for CPIH csv download. Please check the URL or your internet connection.")   
    
    
    
    
    
    
        
        
        
    
    
    