import streamlit as st
from snowflake.snowpark.context import get_active_session

import pandas as pd
from datetime import date
from datetime import datetime
from datetime import timedelta
from pyxlsb import open_workbook as open_xlsb
import os
import shutil
import numpy as np
import re
st.set_page_config(page_title="Unreported AE Monitoring Portal", layout="wide")


# Get the current credentials
session = get_active_session()
session.sql("ALTER SESSION SET statement_timeout = '4h'")
    
if 'init_load' not in st.session_state:
    st.session_state['init_load'] = 0
    
if  st.session_state['init_load'] == 0:
    alert = st.warning('Loading Inital Programs, Keyword and Supervisor Data...  This may take a minute or two...', icon="ℹ️") # Display the alert

    with st.spinner('Wait for it...'):
        
        
        if 'todays_dt' not in st.session_state:
            st.session_state['todays_dt'] = date.today()
                       
        if 'start_date' not in st.session_state:
            st.session_state['start_date'] = str(date.today() - timedelta(days = 1)).replace('-', '')
        
        if 'keywords_df' not in st.session_state:
            keywords_sql = "SELECT * FROM PRD_MLS_PL_SP_DB.SMC_PAE.PAE_KEYWORD"
            keywords_data = session.sql(keywords_sql).collect()
            st.session_state['keywords_df'] = pd.DataFrame(keywords_data)
            
        if 'keyword_list' not in st.session_state:
            st.session_state['keyword_list'] = st.session_state['keywords_df']['KEYWORD'].tolist()
            
        
        if 'supervisors_df' not in st.session_state:
            supervisor_sql = "SELECT * FROM PRD_MLS_PL_SP_DB.SMC_PAE.SUPERVISOR"
            supervisor_data = session.sql(supervisor_sql).collect()
            st.session_state['supervisors_df'] = pd.DataFrame(supervisor_data)
        
        if 'program_list' not in st.session_state:
            program_sql = "SELECT ORDER_FACILITY_CODE FROM PRD_MLS_PL_SP_DB.SMC_PAE.VW_PATIENT_FACILITY"
            program_data = session.sql(program_sql).collect()
            program_df = pd.DataFrame(program_data)
            plan_name_list = program_df['ORDER_FACILITY_CODE'].unique().tolist()
            
            # Filter out None values from plan_name_list and pharma_prg_name_list
            filtered_plan_names = [name for name in plan_name_list if name is not None]
              
            # Combine the filtered lists and sort the resultant list
            st.session_state['program_list'] = sorted(list(set(map(int, filtered_plan_names))))
            

def remove_commas(col):
    col = str(col)
    col = col.replace(',', '')
    col = col[ : 6]
    return col


def check_text(df, column_name, words_list):
    """
    Function to check if any word or phrase in a list exists in a DataFrame column composed of free text.
    
    Parameters:
        df (DataFrame): The DataFrame containing the column to be checked.
        column_name (str): The name of the column in the DataFrame to be checked.
        words_list (list): A list of words or phrases to search for in the column.
        
    Returns:
        DataFrame: A DataFrame containing only the rows where any word or phrase in the words_list
                   exists in the specified column. Additionally, a new column 'matched_keyword' is added
                   to indicate which keywords were matched in each row.
    """
    if not words_list:
        raise ValueError("The words_list cannot be empty.")
    
    if column_name not in df.columns:
        raise ValueError(f"The specified column '{column_name}' does not exist in the DataFrame.")
    
    # Initialize an empty list to store matched keywords
    matched_keywords = []

    # Create a regex pattern to match whole words or phrases (case-insensitive)
    pattern = r"\b(?:{})\b".format("|".join(map(re.escape, words_list)))
    
    # Iterate through each row in the DataFrame and find matched keywords
    for index, row in df.iterrows():
        cell_value = row[column_name]
        if pd.isna(cell_value) or not isinstance(cell_value, str):
            matched_keywords.append(None)
        else:
            row_matched_keywords = re.findall(pattern, cell_value, re.IGNORECASE)
            matched_keywords.append(row_matched_keywords if row_matched_keywords else None)
    
    # Add the matched keywords as a new column in the DataFrame
    df['matched_keyword'] = matched_keywords
    
    # Filter the DataFrame to include only rows with matched keywords
    filtered_df = df[df['matched_keyword'].notna()].copy()
    
    return filtered_df
    
def find_match_and_merge(df1, df1_column, df2, df2_column, merge_columns):
    """
    Function to find a match in one DataFrame based on a column value, 
    and then merge the matched row from another DataFrame into the first DataFrame.
    
    Parameters:
        df1 (DataFrame): The first DataFrame where the match will be found and merged.
        df1_column (str): The name of the column in the first DataFrame to be used for matching.
        df2 (DataFrame): The second DataFrame from which the matching row will be taken.
        df2_column (str): The name of the column in the second DataFrame to be used for matching.
        merge_columns (list): A list containing the names of the columns from the second DataFrame
                              to be merged into the first DataFrame.
        
    Returns:
        DataFrame: The first DataFrame with new columns added containing the matched values from the second DataFrame.
    """
    # Create copies of the DataFrames if they are not already copies
    df1 = df1.copy()
    df2 = df2.copy()
    
    # Create a dictionary mapping unique values in df2_column to rows in df2
    df2_dict = dict(zip(df2[df2_column], df2[merge_columns[0]]))
    df2_dict_second = dict(zip(df2[df2_column], df2[merge_columns[1]]))
    
    # Map values from df1_column to values from df2_dict, where possible
    df1['SUPERVISOR'] = df1[df1_column].map(df2_dict)
    df1['BACKUP_SUPERVISOR'] = df1[df1_column].map(df2_dict_second)
    
    return df1
        

if  st.session_state['init_load'] == 0:    
    alert.empty() # Clear the alert
    
st.session_state['init_load'] = 1

# Write directly to the app
st.title("Unreported AE Monitoring Portal")

# Create tabs
tab1, tab2 = st.tabs(["Unreported AE Search", "AE Audit Review"])

with tab1:

    st.subheader("Today's Date:  " + st.session_state['todays_dt'].strftime("%m-%d-%Y"))
    
    st.write(
    """This site is utilized to scan all patient notes where an
            **Adverse Event**, is possible to have occurred and 
            possibly not reported, based on a keyword list provided by the 
            Quality Team.
    """
    )
    
    st.write("\n")
    st.write("\n")
    st.write("\n")
    
    # Review the keywords matches
    st.subheader("Review Possible AEs from Keyword Match")
    
    # Check if 'eid' is already in session state
    if 'eid' not in st.session_state:
        # Display the input box to collect the EID
        eid = st.text_input("Please supply your eid for downloading a results file.")
        if eid:  # Check if the user has entered an EID
            st.session_state['eid'] = eid  # Store the EID in session state
    
    
    #Review keyword matches
    matches_on = st.toggle("Would you like to review today's keyword matches?")
    
    if matches_on:
        if 'kw_match_count' not in st.session_state:
            st.session_state['kw_match_count'] = 0
            
        st.subheader("Choose Dates for Notes Data Pull")
        st.write("Beginning Date is inclusive and Ending Date is NOT inclusive.")
        beg_date = st.date_input("What is the beginning date?", value=None, format="YYYY/MM/DD")
        st.write("You selected:", beg_date)
        end_date = st.date_input("What is the ending date?", value=None, format="YYYY/MM/DD")
        st.write("You selected:", end_date)
        
    
        if st.session_state['eid'] != None:
            summary_date = f"{st.session_state['start_date'][4:6]}-{st.session_state['start_date'][6:]}-{st.session_state['start_date'][:4]}"
            date_range = f"{beg_date} through {end_date}"
            
    
            program_option = st.selectbox(
                "Which Program would you like to check?",
                (st.session_state['program_list']), 
                index=None,
                key='program_option', 
                placeholder="Choose an option")
            
            st.subheader(f"Notes Summary for:  {date_range}")
            
            try:
                # SQL Parameters
                notes_sql = f"""
                SELECT * FROM PRD_MLS_PL_SP_DB.SMC_PAE.VW_PATIENT_NOTES
                WHERE ORDER_FACILITY_CODE = '{program_option}'
                AND NOTE_ADD_DATE BETWEEN '{beg_date}' AND '{end_date}'
                AND NOTE_ADD_USER != 'smcsystemuser@inovalon.com'
                AND NOTE_ADD_USER != 'smc-scheduler@inovalon.com'
                AND NOTE_ADD_USER != 'SYSADM'
                AND NOTE != '--- Order Created from Referral Tracking---'
                ;"""
                #(PLAN_NAME = '{program_option}' OR PHARMA_PROGRAM_NAME = '{program_option}')
                
                notes_data = session.sql(notes_sql).collect()
                notes_data_df = pd.DataFrame(notes_data).drop_duplicates(keep='first', inplace=False)
                notes_data_df.drop(['PLAN_NAME'], axis=1, inplace=True)
                notes_data_df.drop(['PHARMA_PROGRAM_NAME'], axis=1, inplace=True)
                
                st.session_state['df_count'] = notes_data_df.shape[0] 
                st.write(f"Excluding Workflow Notes not equal to Clinical Workstage, there were {str(st.session_state['df_count'])} total notes generated for your date range." ) 
                
                view_notes_data_df_on = st.toggle("Would you like to review all Program Notes records for the selected date range?")
    
                if view_notes_data_df_on:
                    st.write(notes_data_df)
    
    
    
                if st.session_state['df_count'] == 0:
                    
                    st.write("There were no Notes found for this program on this date.")
                    
                else:
                    # Call the function to check if any word or phrase in the list exists in the 'Text_Column'
                    filtered_df = check_text(notes_data_df, 'NOTE', st.session_state['keyword_list'])
                     
                    
                    # Call the function to find matches and merge
                    result_df = find_match_and_merge(filtered_df, 'NOTE_ADD_USER', st.session_state['supervisors_df'], 'EMPLOYEE_NAME', ['SUPERVISOR', 'BACKUP_SUPERVISOR'])
                    
                    
                    if result_df.shape[0] == 0:
                        st.write("There were no keyword matches for this program.")
    
                    else:
                        result_df['PATIENT_ID'] = result_df['PATIENT_ID'].apply(remove_commas)
                        
                        result_df = result_df.loc[result_df['ORDER_FACILITY_CODE'] == str(program_option)]
     
                        st.session_state['kw_match_count'] = result_df.shape[0]
                        
                                           
                        result_df.rename(columns={"PATIENT_ID": "Patient MRN",
                                                  "NOTE": "AE_PQC",
                                                  "NOTE_ADD_DATE": "AE_PQC_Date",
                                                  "NOTE_ADD_USER": "Agent Responsible",
                                                  "SUPERVISOR" : "Supervisor of RA",
                                                  "BACKUP_SUPERVISOR": "BackUp Supervisor for Agent"}, inplace=True)
                        
                        result_df.drop(['PATIENT_UNIQUE_IDENTIFIER'], axis=1, inplace=True)
                        
                        result_df['Unreported AE']=False
                        
                        # Update the NOTED_AE column in edit_df with case-insensitive check
                        result_df['NOTED_AE']=False
                        result_df['AUDIT_DATE'] = pd.NaT
                        
                        result_df['AUDITOR'] = st.session_state['eid']
                        
                       
                        col_order = ['AUDIT_DATE', 'AUDITOR', 'NOTE_TYPE_NAME', 'Patient MRN',  'PATIENT_ALT_PATIENT_ID',
                                      'AE_PQC', 'matched_keyword', 
                                     'Unreported AE', 'AE_PQC_Date', 'Agent Responsible', 'NOTED_AE', 'Supervisor of RA', 
                                     'BackUp Supervisor for Agent', 'ORDER_FACILITY_CODE']
    
                        result_df = result_df.drop_duplicates(subset = ['Patient MRN',  'AE_PQC', 'AE_PQC_Date'],
                                                              keep='first',
                                                              inplace=False)
                        
                        
                        if 'result_df' not in st.session_state:     
                            
                
                            st.session_state.result_df = result_df[col_order]
                            st.session_state.result_df['AUDIT_DATE'] = datetime.now()
                            #st.session_state.result_df['AUDITOR'] = st.session_state['eid']
                            st.session_state.result_df['AUDIT_DATE'] = pd.to_datetime(st.session_state.result_df['AUDIT_DATE'])
                            
                            # Function to update 'NOTED_AE' based on 'matched_keyword'
                            def update_noted_ae():
                            
                                # Iterate over each row in the DataFrame
                                for index, row in st.session_state.result_df.iterrows():
                                    # Check if 'ADVERSE EVENT' or 'Adverse Event' is in the 'matched_keyword' list
                                    if 'ADVERSE EVENT' in row['matched_keyword'] or 'Adverse Event' in row['matched_keyword']:
                                        # Set 'NOTED_AE' to True
                                        st.session_state.result_df.at[index, 'NOTED_AE'] = True
                                    
                               
                            update_noted_ae()

                        else:
                            st.session_state.result_df = result_df[col_order]
                            st.session_state.result_df['AUDIT_DATE'] = datetime.now()
                            #st.session_state.result_df['AUDITOR'] = st.session_state['eid']
                            st.session_state.result_df['AUDIT_DATE'] = pd.to_datetime(st.session_state.result_df['AUDIT_DATE'])
                            
                            # Function to update 'NOTED_AE' based on 'matched_keyword'
                            def update_noted_ae():
                            
                                # Iterate over each row in the DataFrame
                                for index, row in st.session_state.result_df.iterrows():
                                    # Check if 'ADVERSE EVENT' or 'Adverse Event' is in the 'matched_keyword' list
                                    if 'ADVERSE EVENT' in row['matched_keyword'] or 'Adverse Event' in row['matched_keyword']:
                                        # Set 'NOTED_AE' to True
                                        st.session_state.result_df.at[index, 'NOTED_AE'] = True
                                    
                               
                            update_noted_ae()
                                            
                            
                        st.write(f"There are {st.session_state.result_df.shape[0]} records that have a keyword match.")
     
                        
                        # Visualizations
                        
                        st.bar_chart(st.session_state.result_df['NOTE_TYPE_NAME'].value_counts())
                                            
                        
                        # Display the data editor and save the state back to session_state
                        edit_df = st.data_editor(st.session_state.result_df)
                        
                        if 'edit_df' not in st.session_state:
    
                            st.session_state.edit_df = edit_df
                        else:
                            st.session_state.edit_df = edit_df
    
    
    
            except:
             # Prevent the error from propagating into your Streamlit app.
              pass

                
            def insert_data_to_snowflake_via_snowflake_df(data: pd.DataFrame, table_name: str):
                try:
                    # Get the active Snowflake session
                    #session = get_active_session()  # Make sure this is correctly retrieving the session
            
                    # Convert boolean columns to string representations suitable for Snowflake
                    data['Unreported AE'] = data['Unreported AE'].apply(lambda x: 'TRUE' if x else 'FALSE')
                    data['NOTED_AE'] = data['NOTED_AE'].apply(lambda x: 'TRUE' if x else 'FALSE')
                    data.drop(columns=['Supervisor of RA', 'BackUp Supervisor for Agent'], axis=1, inplace=True)   
                    
                    # Convert pandas DataFrame to Snowpark DataFrame
                    snowpark_df = session.create_dataframe(data)

                    # Write to a Snowflake table
                    snowpark_df.write.mode("append").save_as_table(table_name)
            
                    st.write("Data saved to Snowflake successfully!")
            
                except Exception as e:
                    raise Exception(f"Error saving data to Snowflake: {e}")
            

            
            # Button to trigger the save operation
            save_to_db_button = st.button("Save Completed Audit to Database", type="primary")
            
            if save_to_db_button:
                insert_data_to_snowflake_via_snowflake_df(st.session_state.edit_df, "AE_AUDITS")
                            
                 
            process_button = st.button("Click to create a download file of Unreported AEs.", type="primary", key = 'process_button')
                    
            if process_button:
                st.success("click the download button to create the Unreported AE file")
            
            
                def convert_df(df):
                        df = df.loc[df['Unreported AE']== True] 
                        df.drop(['Unreported AE'], axis=1, inplace=True)
                        return df.to_csv(index=False)#.encode('utf-8')
                        
                csv = convert_df(st.session_state.edit_df)
                    
                dwnload_path = f"C:/Users/{st.session_state['eid']}/Downloads/"
                move_path = f"C:/Users/{st.session_state['eid']}/Documents/"
                file_name='Unreported_AEs' + summary_date + '.csv'      
                dwnload_button = st.download_button(
                   "Press to Download",
                   csv,
                    file_name = file_name,
                   #"file.csv",
                   #"text/csv",
                   key='download-csv'
                )           
            
                # Write each dataframe to a different worksheet.
            
                if dwnload_button:
                    if os.path.exists(dwnload_path + file_name):
                        st.success("File downloaded successfully!")
                        if not os.path.exists(move_path + file_name):
                            shutil.move(dwnload_path + file_name, move_path + file_name)
        

    st.write("\n")
    st.write("\n")
    st.write("\n")
    
    # View or modify the supervisors list
    st.subheader("Supervisors")
    supervisor_on = st.toggle("Would you like to review the Supervisors list?")
    
    if supervisor_on:
        # Display the current supervisors list
        st.write("Supervisors list activated!")
    
        
        # Display the current supervisors list
        st.dataframe(st.session_state['supervisors_df'], hide_index=True)
        
        supervisor_modify_on = st.toggle("Would you like to enable modification options?")
        
        if supervisor_modify_on:
            # Operation selection
            
            operation = st.radio("Select operation:", ("Add Row", "Modify Row", "Delete Row"))
    
            if operation == "Add Row":
                # Get input for new row
                with st.form(key='supv_insert_form'):
                    column1 = st.text_input("EMPLOYEE_NAME")
                    column2 = st.text_input("SUPERVISOR")
                    column3 = st.text_input("BACKUP_SUPERVISOR")
                    submit_button = st.form_submit_button(label='Add Row')
                    
                if submit_button:
                    # Execute SQL query to insert new row
                    query = f"""INSERT INTO PRD_MLS_PL_SP_DB.SMC_PAE.SUPERVISOR 
                    (EMPLOYEE_NAME, SUPERVISOR, BACKUP_SUPERVISOR)
                    VALUES ('{column1}', '{column2}', '{column3}');"""
                    session.sql(query).collect()
                    st.success("Row added successfully!")
            
            elif operation == "Modify Row":
                # Get input for row to modify
                with st.form(key='supv_modify_form'):
                    row_id = st.text_input("Enter row ID to modify:")
                    new_column1 = st.text_input("New value for EMPLOYEE_NAME")
                    new_column2 = st.text_input("New value for SUPERVISOR")
                    new_column3 = st.text_input("New value for BACKUP_SUPERVISOR")
                    submit_button = st.form_submit_button(label='Modify Row')
                
    
                if submit_button:
                    # Execute SQL query to update row
                    query = f"""
                            UPDATE PRD_MLS_PL_SP_DB.SMC_PAE.SUPERVISOR 
                            SET EMPLOYEE_NAME = '{new_column1}', 
                                SUPERVISOR = '{new_column2}',
                                BACKUP_SUPERVISOR = '{new_column3}'
                            WHERE id = '{row_id}';
                            """
                    session.sql(query).collect()
                    st.success("Row modified successfully!")
            
            elif operation == "Delete Row":
                with st.form(key='supv_delete_form'):
                    
                    # Get input for row to delete
                    row_id = st.text_input("Enter row ID to delete:")
                    submit_button = st.form_submit_button(label = "Delete Row")
                    
                if submit_button:
                    # Execute SQL query to delete row
                    query = f"""
                            DELETE FROM PRD_MLS_PL_SP_DB.SMC_PAE.SUPERVISOR 
                            WHERE id = '{row_id}';
                            """
                    session.sql(query).collect()
                    st.success("Row deleted successfully!")
    
    st.write("\n")
    st.write("\n")
    st.write("\n")
    
    # View or modify the keywords list
    st.subheader("Keywords")
    keywords_on = st.toggle("Would you like to review the Keywords list?")
    if keywords_on:
        st.write("Keywords list activated!")
       
        # Display the current supervisors list
        st.dataframe(st.session_state['keywords_df'], hide_index=True)
    
        keywords_modify_on = st.toggle("Would you like to enable modification options?", key='kw_t_m_o')
    
        if keywords_modify_on:
            # Operation selection
            operation = st.radio("Select operation:", ("Add Row", "Modify Row", "Delete Row"), key='kw_r_m_o')
    
            if operation == "Add Row":
                # Get input for new row
                with st.form(key='kw_insert_form'):
                    column1 = st.text_input("KEYWORD")
                    submit_button = st.form_submit_button(label='Add Row')
                    
                if submit_button:
                    # Execute SQL query to insert new row
                    query = f"""INSERT INTO PRD_MLS_PL_SP_DB.SMC_PAE.PAE_KEYWORD 
                    (KEYWORD)
                    VALUES ('{column1}');"""
                    session.sql(query).collect()
                    st.success("Row added successfully!")
            
            elif operation == "Modify Row":
                # Get input for row to modify
                with st.form(key='kw_modify_form'):
                    row_id = st.text_input("Enter row ID to modify:")
                    new_column1 = st.text_input("New value for KEYWORD")
    
                    submit_button = st.form_submit_button(label='Modify Row')
                
    
                if submit_button:
                    # Execute SQL query to update row
                    query = f"""
                            UPDATE PRD_MLS_PL_SP_DB.SMC_PAE.PAE_KEYWORD 
                            SET KEYWORD = '{new_column1}' 
                            WHERE id = '{row_id}'
                            """
                    session.sql(query).collect()
                    st.success("Row modified successfully!")
            
            elif operation == "Delete Row":
                with st.form(key='kw_delete_form'):
                    
                    # Get input for row to delete
                    row_id = st.text_input("Enter row ID to delete:")
                    submit_button = st.form_submit_button(label = "Delete Row")
                    
                if submit_button:
                    # Execute SQL query to delete row
                    query = f"""
                            DELETE FROM PRD_MLS_PL_SP_DB.SMC_PAE.PAE_KEYWORD 
                            WHERE id = '{row_id}';
                            """
                    session.sql(query).collect()
                    st.success("Row deleted successfully!")

with tab2:
    st.subheader(f"Audit Reviews")
    
    st.subheader("Choose Dates for Audit Data Pull")
    st.write("Beginning Date is inclusive and Ending Date is NOT inclusive.")
    audit_beg_date = st.date_input("What is the beginning date?", value=None, format="YYYY/MM/DD", key='audit_beg_date')
    st.write("You selected:", audit_beg_date)
    audit_end_date = st.date_input("What is the ending date?", value=None, format="YYYY/MM/DD", key='audit_end_date')
    st.write("You selected:", audit_end_date)

    audit_program_option = st.selectbox(
        "Which Program would you like to check?",
        (st.session_state['program_list']), 
        index=None,
        key='audit_program_option', 
        placeholder="Choose an option")

    if audit_beg_date and audit_end_date != None:
        # SQL Parameters
        audit_sql = f"""
        SELECT *
        FROM PRD_MLS_PL_SP_DB.SMC_PAE.AE_AUDITS
        WHERE ORDER_FACILITY_CODE = '{audit_program_option}'
        AND AUDIT_DATE BETWEEN '{audit_beg_date}' AND '{audit_end_date}';
        """
    
        
        audit_sql_data = session.sql(audit_sql).collect()
        audit_data_df = pd.DataFrame(audit_sql_data)
    
        st.dataframe(audit_data_df)


