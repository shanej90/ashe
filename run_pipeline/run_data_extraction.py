#####################################
# SCRIPT TO GET DATA FROM ONS API
#####################################

### IMPORTS
import pandas as pd

from get_data.initial_api_extraction import *

### INFLATION
download_inflation()

### ASHE DATASETS
ashe_datasets = get_ashe_datasets()
dataset_ids = ashe_datasets["id"].tolist()

### DATASET VERSIONS
versions = [get_versions_from_datasets(i, source_df = ashe_datasets) for i in dataset_ids]
versions_df = pd.concat(versions)
versions_df = versions_df.drop_duplicates(subset = "id")
version_ids = versions_df["id"].tolist()

### DOWNLOAD OBSERVATIONS
for i in version_ids:
    download_observations_from_versions(i, source_df = versions_df)
    
### DOWNLOAD DIMENSIONS
download_dimensions_from_versions(versions_df)
