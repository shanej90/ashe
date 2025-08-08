#####################################################################
# TAKE RAW ONS DATA AND LOAD TO SQLITE DATABASE AS 'SILVER' DATA
#####################################################################

### IMPORTS
import os
import pandas as pd
import sqlite3

from utils.directory_navigation import find_project_root

### IDENTIFY ROOT
root = find_project_root()

### LIST FILES
dim_files = os.listdir(f"{root}/bronze_files/dimensions")
fact_files = os.listdir(f"{root}/bronze_files/facts")

### LOAD RAW DATA
dimensions_raw = [pd.read_csv(f) for f in dim_files]
facts_raw = [pd.read_csv(f) for f in fact_files]

### FILE KEYS
dim_keys = [i.replace(".csv", "") for i in dim_files]
fact_keys = [i.replace(".csv", "") for i in fact_files]

### DF DICTS
dim_dict = dict(zip(dim_keys, dimensions_raw))
fact_dict = dict(zip(fact_keys, facts_raw))

### ADD BUSINESS LOGIC: DIMENSIONS

#years
dim_dict["calendar-years"] = dim_dict["calendar-years"].drop(columns = "dimension").sort_values(by = "code")

#inflation
dim_dict["cpih"]["month_start"] = pd.to_datetime(dim_dict["cpih"]["mmm-yy"], format = "%b-%y") 
dim_dict["cpih"] = (
    dim_dict["cpih"][dim_dict["cpih"]["cpih1dim1aggid" == "CP00"]].
    drop(columns = ["cpih1dim1aggid", "Aggregate", "Geography", "mmm-yy", "Time"]).
    sort_values(by = "month_start")
    )

#constituencies
dim_dict["parliamentary-constituencies"] = dim_dict["parliamentary-constituencies"].drop(columns = "dimension")

#sex
dim_dict["sex"] = dim_dict["sex"].drop(columns = "dimension")

#working pattern
dim_dict["working-pattern"] = dim_dict["working-pattern"].drop(columns = "dimension")

#workplace vs. residence
dim_dict["workplace-or-residence"] = dim_dict["workplace-or-residence"].drop(columns = "dimension")

### ADD BUSINESS LOGIC: FACTS






