
#Censored SQL queries

class SQL_queries:
    def __init__(self):
        self.SQL_membrane_maintenance = self.membrane_maintenance_query()
        self.SQL_membrane_change = self.membrane_change_query()

    def membrane_maintenance_query(self):
        SQL_membrane_maintenance = """
        SELECT
         AS ACTIVITY_ID,
        ,
         AS COLLECTED_DATE,
         AS PROMPT_ID,
         AS COLLECTED_VALUE,
         AS COLLECTED_BY

        FROM
        ,
        ,
        ,
        ,
        

        WHERE
        AND  = 
        AND  = 
        AND  = '.HEAD_MAINTENANCE'
        AND  <> 'N/A'
        AND  = 
        AND  = 
        AND  = 
        AND >SYSDATE-1000
        AND SUBSTR(, 1,  INSTR(,'~',1,2) -1) = 
        ORDER BY

        """
        return SQL_membrane_maintenance

    def membrane_change_query(self):
        SQL_membrane_change = """

        SELECT
        ,
        ,
         AS PROMPT_ID,
         AS COLLECTED_VALUE,
         AS COLLECTED_BY,
         AS COLLECTED_DATE

        FROM 
        , 
        , 
        , 
        ,  
        

        WHERE
        AND  = 
        AND  = 
        AND  = '.POLISHING_HEAD_CHANGE'
        AND  <> 'N/A'
        AND  = 
        AND  = 
        AND  = 
        AND >SYSDATE-1000
        AND SUBSTR(, 1,  INSTR(,'~',1,2) -1) = 
        ORDER BY 

        """
        return SQL_membrane_change
    
    def get_variables(self):
        return self.SQL_membrane_maintenance, self.SQL_membrane_change