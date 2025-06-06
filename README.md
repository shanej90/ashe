# ASHE

__Currently a work in progress!__

This is a personal data project to get [Office for National Statistics](https://www.ons.gov.uk/) (ONS) data from the [Annual Survey of Hours and Earnings](https://www.ons.gov.uk/employmentandlabourmarket/peopleinwork/earningsandworkinghours/bulletins/annualsurveyofhoursandearnings/2024) (ASHE) into Power BI.

The process I have taken is:

- Download data as Excel files from the ONS website, using the ONS API (via Python) to get the data
- Rather than use the trendy new 'ELT' process, instead I stick with old-fashioned 'ETL' as sqlite is not a data lake and doesn't have the tools the automate data manipulation, nor do so efficiently as could be done via Python.
- The manipulated data is then loaded as tables (kind of like 'silver' tables in a data lake) to the sqlite database.
- 'Gold'-style dimension and fact views are then created in sqlite and deployed to Power BI.

From their the model and accompanying report are developed using `.pbip` files. All SQL queries, Python code and model docs are included in this repository. The Power BI report is hosted via [NovyPro](https://www.novypro.com/).