from SQL_query_file import SQL_queries
from membrane_finder_file import Membrane_finder


#Set SQL Query
sql_object = SQL_queries()
SQL_membrane_maintenance, SQL_membrane_change = sql_object.get_variables()

#Create csv file to track wafers
Membrane_finder(SQL_membrane_maintenance, SQL_membrane_change)