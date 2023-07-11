import cx_Oracle
from dotenv import load_dotenv
import pandas as pd
from datetime import date, datetime, timedelta
import matplotlib.pyplot as plt
import os


class Membrane_finder:
    def __init__(self, SQL_membrane_maintenance, SQL_membrane_change):
        self.current_time()
        self.user_id, self.password_id, self.dsn_id = self.env_loader()
        self.df_membrane_maintenance = self.connect_oracle(SQL_membrane_maintenance)
        self.df_membrane_change = self.connect_oracle(SQL_membrane_change)
        self.latest_reasons, self.df_sorted = self.data_manipulator()
        self.print_reasons()
        self.df_sorted = self.create_membrane_inserted_column()
        self.df_sorted = self.create_backtracked_wafers_column()
        self.df_sorted = self.create_backtracked_time_column()
        self.df_sorted = self.create_backtracked_reason_column()
        self.df_sorted = self.create_membrane_storage_time_column()
        self.df_sorted = self.create_storage_time_correction()
        self.df_sorted = self.improve_data_quality()
        self.save_to_csv()



    def current_time(self):
        #Print current time
        today = date.today()
        string_time = today.strftime("%d/%m/%Y")
        return print("\n\nCurrent date:", string_time, "\n")
    
    def env_loader(self):
        #load dotenv
        load_dotenv("dotenv.env")
        user_id = os.getenv("USER_pdam")
        password_id = os.getenv("PW_pdam")
        dsn_id = os.getenv("DSN_pdam")
        return user_id, password_id, dsn_id
    

    def connect_oracle(self, SQL_query):
        try:
            #Creating connection to Oracle database
            connection = cx_Oracle.connect(user=self.user_id, password=self.password_id, dsn=self.dsn_id)
                
            #Creating Cursor Object
            cur = connection.cursor()
            print("**Connection to Oracle Successful**\n")
            
            #Execute SQL
            cur.execute(SQL_query)
            
            #Fetch all rows
            rows = cur.fetchall()
            
            #Filter main DataFrame
            df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
            
            df["COLLECTED_DATE"] = pd.to_datetime(df["COLLECTED_DATE"], format="%d.%m.%Y %H:%M:%S")

            #close object
            cur.close()
            
            #Print if Oracle connection fails
        except cx_Oracle.Error as error:
            print("\nFailed to connect,", error, "\n")
        finally:
            if connection:
                connection.close()

            return df

    def data_manipulator(self):
        
        pivot_table1 = self.df_membrane_maintenance.pivot_table(
        index=["ACTIVITY_ID", "EQP_ID", "COLLECTED_DATE", "COLLECTED_BY"],
        columns= "PROMPT_ID",
        values= "COLLECTED_VALUE",
        aggfunc = lambda x: ' '.join(x)
        )
        
        pivot_table2 = self.df_membrane_change.pivot_table(
            index=["ACTIVITY_ID", "EQP_ID", "COLLECTED_DATE", "COLLECTED_BY"],
            columns= "PROMPT_ID",
            values= "COLLECTED_VALUE",
            aggfunc = lambda x: ' '.join(x)
        )
        
        # For printing latest head changes
        change_syy = self.df_membrane_change[self.df_membrane_change["PROMPT_ID"] == "Syy"]
        latest_reasons = change_syy.iloc[::-1].drop_duplicates(subset=["EQP_ID"])

        #CONCATENATE DATA
        concat = pd.concat([pivot_table1, pivot_table2], axis = 1)

        #Make multi indexed columns flat
        df_flat = concat.reset_index()

        #save Kiillotuspää data to HEAD Nro
        df_flat["Head Nro"] = df_flat["Head Nro"].combine_first(df_flat["Kiillotuspää"])

        #Now can safely drop Kiillotuspää data because column data is now in Head Nro column
        df_flat = df_flat.drop(columns=["Kiillotuspää"])

        #join multi index columns before saving csv file
        df_flat.columns = [''.join(str(col)) for col in df_flat.columns]

        #sort by date (important!)
        df_sorted = df_flat.sort_values(by='COLLECTED_DATE', ascending=False)

        #reset index
        df_sorted = df_sorted.reset_index(drop=True)

        #Convert datatypes to float
        df_sorted["Käytetty aika"] = df_sorted["Käytetty aika"].astype(float)
        df_sorted["Ajetut kiekot"] = df_sorted["Ajetut kiekot"].astype(float)

        return latest_reasons, df_sorted


    def print_reasons(self):
        #Print Current HEAD's
        return print("\nLatest CMP_02A and CMP_02B HEAD CHANGES:\n"), print(self.latest_reasons)
    
    def __find_membrane(self, index, row, column_search, column_original):
        search_value = row[column_search]
        start_index = index + 1


        # If the next_index is not found, return NaN
        remainin_rows = self.df_sorted.loc[start_index:, column_search]
        if not any(remainin_rows.eq(search_value)):
            return float('nan')
            
        # Find the index of the next row with the same value in column_search
        next_index = remainin_rows.eq(search_value).idxmax()

        # Return the value from column_original at the next_index
        return self.df_sorted.loc[next_index, column_original]

    def create_membrane_inserted_column(self):
        # Create the new column 'Membrane.EXP.DATE_INSERTED' which copies the Membrane.EXP.DATE data to the row when the membrane is changed to the machine
        self.df_sorted['Membrane.EXP.DATE_INSERTED'] = self.df_sorted.apply(lambda row: self.__find_membrane(row.name, row, 'Head Nro', 'Membrane.EXP.DATE'), axis=1)
        return self.df_sorted

    ### CALCULATE TIME BETWEEN MAINTENANCE AND CHANGE ###

    def __calculate_delta_time(self, index, row, column_search, activity_column, date_column):
        if row[activity_column] not in  ('CMP_02B.POLISHING_HEAD_CHANGE', "CMP_02A.POLISHING_HEAD_CHANGE"):
            return float('nan')
            
        search_value = row[column_search]
        start_index = index + 1

        # Check if there are more occurrences of the search_value with 'CHANGE' as activity_column value after the current row
        remaining_rows = self.df_sorted.loc[start_index:]
        remaining_rows = remaining_rows[remaining_rows[activity_column] == 'AM.CMP_02.HEAD_MAINTENANCE']
        
        if not any(remaining_rows[column_search].eq(search_value)):
            return float('nan')

        # Find the index of the next row with the same value in column_search and 'CHANGE' as activity_column value
        next_index = remaining_rows[remaining_rows[column_search] == search_value].index[0]

        # Calculate the time difference between the current row and the next row
        delta_time = self.df_sorted.loc[next_index, date_column] - row[date_column]

        #converting delta_time to hours by using .total_seconds() method
        delta_time_hours = round(delta_time.total_seconds() / 3600, 3)

        #matematical correction
        delta_time_hours = -delta_time_hours

        # Return the time difference
        return delta_time_hours


    def __find_syy(self, index, row, column_search, activity_column, syy_column):
        if row[activity_column] != "AM.CMP_02.HEAD_MAINTENANCE":
            return float('nan')
            
        search_value = row[column_search]
        start_index = index - 1

        # Check if there are more occurrences of the search_value with 'CHANGE' as activity_column value after the current row
        remaining_rows = self.df_sorted.loc[:start_index, :]
        remaining_rows = remaining_rows[remaining_rows[activity_column].isin(('CMP_02A.POLISHING_HEAD_CHANGE', 'CMP_02B.POLISHING_HEAD_CHANGE'))]
        
        if not any(remaining_rows[column_search].eq(search_value)):
            return float('nan')

        #REVERSE THE DATA SEARCH from bottom to top
        remaining_rows = remaining_rows.iloc[::-1]

        # Find the index of the previous row with the same value in column_search
        prev_index = remaining_rows[remaining_rows[column_search] == search_value].index[0]


        # Calculate the time difference between the current row and the next row
        syy_value = self.df_sorted.loc[prev_index, syy_column]

        # Return the time difference
        return syy_value

    def __find_usage_time(self, index, row, column_search, activity_column, usage_column):
        if row[activity_column] != "AM.CMP_02.HEAD_MAINTENANCE":
            return float('nan')
            
        search_value = row[column_search]
        start_index = index - 1

        # Check if there are more occurrences of the search_value with 'CHANGE' as activity_column value after the current row
        remaining_rows = self.df_sorted.loc[:start_index, :]
        remaining_rows = remaining_rows[remaining_rows[activity_column].isin(('CMP_02A.POLISHING_HEAD_CHANGE', 'CMP_02B.POLISHING_HEAD_CHANGE'))]
        
        if not any(remaining_rows[column_search].eq(search_value)):
            return float('nan')

        #REVERSE THE DATA SEARCH from bottom to top
        remaining_rows = remaining_rows.iloc[::-1]

        # Find the index of the previous row with the same value in column_search
        prev_index = remaining_rows[remaining_rows[column_search] == search_value].index[0]


        # Calculate the time difference between the current row and the next row
        usage_time = self.df_sorted.loc[prev_index, usage_column]

        # Return the time difference
        return usage_time

    def __find_amount_wafers(self, index, row, column_search, activity_column, amount_column):
        if row[activity_column] != "AM.CMP_02.HEAD_MAINTENANCE":
            return float('nan')
            
        search_value = row[column_search]
        start_index = index - 1

        # Check if there are more occurrences of the search_value with 'CHANGE' as activity_column value after the current row
        remaining_rows = self.df_sorted.loc[:start_index, :]
        remaining_rows = remaining_rows[remaining_rows[activity_column].isin(('CMP_02A.POLISHING_HEAD_CHANGE', 'CMP_02B.POLISHING_HEAD_CHANGE'))]
        
        if not any(remaining_rows[column_search].eq(search_value)):
            return float('nan')

        #REVERSE THE DATA SEARCH from bottom to top
        remaining_rows = remaining_rows.iloc[::-1]

        # Find the index of the previous row with the same value in column_search
        prev_index = remaining_rows[remaining_rows[column_search] == search_value].index[0]


        # Calculate the time difference between the current row and the next row
        amount_wafers = self.df_sorted.loc[prev_index, amount_column]

        # Return the time difference
        return amount_wafers
    
    def create_backtracked_wafers_column(self):
        # Create the new column 'Backtracked_Ajetut_kiekot'
        self.df_sorted['Backtracked_Ajetut_kiekot'] = self.df_sorted.apply(lambda row: self.__find_amount_wafers(row.name, row, 'Head Nro', 'ACTIVITY_ID', 'Ajetut kiekot'), axis=1)
        return self.df_sorted

    def create_backtracked_time_column(self):
        # Create the new column 'Backtracked_Käytetty_aika'
        self.df_sorted['Backtracked_Käytetty_aika'] = self.df_sorted.apply(lambda row: self.__find_usage_time(row.name, row, 'Head Nro', 'ACTIVITY_ID', 'Käytetty aika'), axis=1)
        return self.df_sorted

    def create_backtracked_reason_column(self):
        # Create the new column 'Backtracked_Syy'
        self.df_sorted['Backtracked_Syy'] = self.df_sorted.apply(lambda row: self.__find_syy(row.name, row, 'Head Nro', 'ACTIVITY_ID', 'Syy'), axis=1)
        return self.df_sorted

    def create_membrane_storage_time_column(self):
        # Create the new column 'Membrane_storage_time(h)'
        self.df_sorted['Time_between_maintenance_and_change(h)'] = self.df_sorted.apply(lambda row: self.__calculate_delta_time(row.name, row, 'Head Nro', 'ACTIVITY_ID', 'COLLECTED_DATE'), axis=1)
        return self.df_sorted

    def create_storage_time_correction(self):
        #Now need to take into account that delta_time is time between specific head maintenance and removal of that specific head from the machine.
        #To calculate storage time we need to take into account machine's head usage time which can be found from column "Käytetty aika"
        #"Käytetty aika" is in hours so we can minus this from total time.
        self.df_sorted["Membrane_storage_time(h)"] = self.df_sorted['Time_between_maintenance_and_change(h)'] - self.df_sorted["Käytetty aika"]
        return self.df_sorted

    def improve_data_quality(self):
        #Improve uniformity of datetime
        self.df_sorted["COLLECTED_DATE"] = self.df_sorted["COLLECTED_DATE"].dt.strftime("%d.%m.%Y %H:%M")
        return self.df_sorted

    def save_to_csv(self):
        #save to csv file
        self.df_sorted.to_csv("CMP_02 Membrane tracking.csv", index = False)

        print("Opening 1000 days of CMP2 head maintenance data...")

        #Start file in excel
        os.startfile("CMP_02 Membrane tracking.csv")

