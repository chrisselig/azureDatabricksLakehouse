# Scripts is used to define functions used during the EDA phase

# Connect to Azure databricks
def connectToAzureDatabricks(dsnName):
    conn = pyodbc.connect(dsnName, autocommit=True)
    cursor = conn.cursor()
    
    
import pyodbc
connectToAzureDatabricks(myAzureDatabricks_DSN)
    