from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
import asyncio
from aiohttp import ClientSession, ClientTimeout
import os
import csv
import openpyxl
import pandas as pd
import numpy as np
import sqlite3 as sql
import matplotlib.pyplot as plt
import time


def get_pw_workers():
    PW_URL=os.environ["PW_URL"] #set with export=PW_URL
    PW_JWT=os.environ['PW_JWT'] #rpa jwt

    transport = AIOHTTPTransport(url=PW_URL, headers={'X-Authorization': PW_JWT})
    client = Client(transport=transport, fetch_schema_from_transport=False, execute_timeout=180)
    print("Getting workers")
    workers_patchwork = []
    page_number=1
    while page_number >= 1:
            params = {"page": page_number}
            workers_query = gql(
                """
                query getWorkers($page: Int!){
                    workers(items: 100, page: $page) {
                        niNumber
                        firstName
                        lastName
                        esrNumber
                        dateOfBirth
                    }
                }
                """
            )
            result = client.execute(workers_query, variable_values=params)

            if result['workers'] == []:
                print("=========================================")
                print("No more pages at", page_number)
                print("=========================================")
                page_number = 0
                break

            print("Page number", page_number, "processed.")
            page_number += 1

            for worker in result['workers']:
                employee_number = worker['esrNumber']
                first_name = worker['firstName']
                last_name = worker['lastName']
                ni_number = worker['niNumber']
                date_of_birth = worker['dateOfBirth']
                workers_patchwork.append({'First Name': first_name,
                                        'Last Name': last_name,
                                        'employee_number': employee_number,
                                        'ni_number': ni_number,
                                        'date_of_birth': date_of_birth})
                
    # New data frame set to list generated above 
    workers_patchwork = pd.DataFrame(workers_patchwork)
    workers_patchwork.to_csv('data_frame_logic.csv', index=False)

#get_pw_workers()
# New data frame set to list generated above 
workers_patchwork = pd.read_csv("/Users/benjaminharry/Documents/Scripts/API_Scripts/data_frame_logic.csv")
# Sets up correct rows
workers_patchwork["pw_full_name"] = workers_patchwork['Last Name'] + " " + workers_patchwork['First Name']
workers_patchwork["amended_employee_number"] = workers_patchwork["employee_number"].str.split('-').str[0]
workers_patchwork.to_csv('data_frame_logic.csv', index=False)

# New data frame set to converted csv from health roster
workers_health_roster = pd.read_csv("/Users/benjaminharry/Documents/Scripts/API_Scripts/people.csv")
# Sets up correct rows
workers_health_roster["amended_staff_number"] = workers_health_roster["Staff Number"].str.split('-').str[0]
workers_health_roster["hr_full_name"] = workers_health_roster['Surname'] + " " + workers_health_roster['Forenames']
workers_health_roster = workers_health_roster.rename(columns={'NINumber': '_ni_number'}) 
workers_health_roster.to_csv("/Users/benjaminharry/Documents/Scripts/API_Scripts/people.csv", index=None, header=True)

# Merges both dataframes via NI_Number
df_merged_data = workers_health_roster.merge(workers_patchwork, left_on='_ni_number', right_on='ni_number')
df_merged_data_no_nan = df_merged_data.dropna(subset=['ni_number'])

df_merged_data_no_nan["SQL_command"] = "'" + df_merged_data_no_nan['ni_number'] + "',"
df_merged_data_no_nan["Date of Birth"] = pd.to_datetime(df_merged_data_no_nan["Date of Birth"]).dt.strftime('%Y-%m-%d')

#Mismatching data that has matching NI_Numbers. Names + DOB wrong
mismatched_df = df_merged_data_no_nan[(df_merged_data_no_nan['pw_full_name'] != df_merged_data_no_nan['hr_full_name']) | (df_merged_data_no_nan['date_of_birth'] != df_merged_data_no_nan['Date of Birth'])]

#We will be using HR as the soruce of truth here 
mismatched_df["Mismatched_data"] = np.where((mismatched_df['hr_full_name'] != mismatched_df['pw_full_name']) & (mismatched_df['Date of Birth'] != mismatched_df['date_of_birth']), 'Names and dob mismatch', 
                                            np.where(mismatched_df['hr_full_name'] != mismatched_df['pw_full_name'], 'Names mismatch',
                                            np.where(mismatched_df['Date of Birth'] != mismatched_df['date_of_birth'], 'DOB mismatch', '')))

mismatched_df.to_csv("/Users/benjaminharry/Documents/Scripts/API_Scripts/mismatched_data.csv", index=None, header=True)

#Drops all data that doesn't match on both sides
df_merged_data_no_nan = pd.merge(df_merged_data_no_nan, mismatched_df, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
df_merged_data_no_nan.to_csv("/Users/benjaminharry/Documents/Scripts/API_Scripts/merged_data_no_nan.csv", index=None, header=True)



print(mismatched_df)
print(df_merged_data_no_nan)
