import pymysql.cursors
import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine


# Carga de variables de ambiente que permita una conexión dinámica con la base de datos
# De igual manera se usa como estándar para no exponer información vulnerable
load_dotenv()



# Se crea una clase que puede ser ingestada por un script, haciendo la conexión a la base de datos modular y dinámica
class MySQLConnector():

    # Constructor vacío que configura los datos de la variable de ambiente
    # Se dejan atributos connection, cursor, y engine para poder ser usados por cualquier script que importe la clase
    # Engine es importante tenerlo como atributo porque es lo que permite la carga mediante DataFrames
    def __init__(self) -> None:
        
        self.connection =   pymysql.connect(host=os.getenv("HOST"),
                            user=os.getenv("USER"),
                            database=os.getenv("DATABASE"),
                            cursorclass=pymysql.cursors.DictCursor)
        self.cursor = self.connection.cursor()

        self.engine = create_engine("mysql+pymysql://{user}:@{host}/{db}"
				.format(host=os.getenv("HOST"), db=os.getenv("DATABASE"), user=os.getenv("USER")))
        
    # Método que, dado un SQL query, lo ejecuta, y retorna el resultado
    def execute(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()
    

