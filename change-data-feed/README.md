## Project Overview
This template was developed to provide a good experience for absolute beginners in Databricks environment.

## What does this template do?
- Create the data lake dummy layers and databases silver and gold
- Create the silver and gold tables
- There's a step-by-step guide on how the change data feed works, as well as exploring table commit versions
- Indicates some methods to always Fetching the latest changed data version

## Notebooks
1. [change_data_feed](https://github.com/ac-gomes/data-in-code/blob/master/change-data-feed/change_data_feed.py)
1. [Initialize_table](https://github.com/ac-gomes/data-in-code/blob/master/change-data-feed/Initialize_table.py)
1. [Includes](https://github.com/ac-gomes/data-in-code/blob/master/change-data-feed/Includes.py)
1. [Insert_new_rows](https://github.com/ac-gomes/data-in-code/blob/master/change-data-feed/Insert_new_rows.py)
1. [Update_rows](https://github.com/ac-gomes/data-in-code/blob/master/change-data-feed/Update_rows.py)
1. [Create_gold_table](https://github.com/ac-gomes/data-in-code/blob/master/change-data-feed/Create_gold_table.py)
1. [DBC file](https://github.com/ac-gomes/data-in-code/blob/master/change-data-feed/cdf.dbc)


## How to use it?
- Just import the cdf.dbc file in your [Databricks Community account ](https://community.cloud.databricks.com/) and run the commands in each cell from ```change_data_feed``` notebook. So, evaluate each result for better understanding...


## Feel free to contribute 😃


## Enjoy it!