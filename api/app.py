from fastapi import FastAPI, File, UploadFile
import uvicorn
import pandas as pd
from typing import List
from os import listdir
from os.path import isfile, join
from database.db_connect import MySQLConnector


app = FastAPI()
connector = MySQLConnector()


# Etapa de preprocesamiento que se encarga de refinar los datos de manera que se puedan subir sin problemas al motor de base de datos
# En este caso, se le asignan nombres a cada columnas, y remueven filas con valores nulos
# Lee los archivos, los tranforma a pandas.DataFrame, agrega columnas y quita nulos mediante los métodos de df, y salva el DataFrame refinado en una carpeta nueva
# Se hace así para no caerle encima a los archivos raw, y tener a disposición los nuevos
# TODO: Automatizar el proceso en vez de dejar todo hard-codded
def preprocessing():
    path = '../data/raw'

    f1 = f'{path}/departments.csv'
    f2 = f'{path}/hired_employees.csv'
    f3 = f'{path}/jobs.csv'
    
    df1 = pd.read_csv(f1)
    df1.columns = ['ID', 'name']
    df1 = df1.dropna(how='any',axis=0) 
    df1.to_csv(f'../data/refined/departmens.csv', index=0)

    df2 = pd.read_csv(f2)
    df2.columns = ['ID', 'emp_name', 'hire_ts', 'dep_id', 'job_id']
    df2 = df2.dropna(how='any',axis=0) 
    df2.to_csv(f'../data/refined/hired_employees.csv', index=0)

    df3 = pd.read_csv(f3)
    df3.columns = ['ID', 'name']
    df3 = df3.dropna(how='any',axis=0) 
    df3.to_csv(f'../data/refined/jobs.csv', index=0)



# Landing page que no hace nada, se tiene únicamente para redirigir cuando no exista una ruta
@app.get('/')
def read_root():
    return {"root": 'root'}


# Mediante FastAPI, se tiene un GUI que permite cargar los datos a memoria del programa
# Itera sobre la lista de archivos, los convierte en DataFrames para ser ingestados a la base de datos
# df.to_sql permite cargar los datos dado un "engine" de SQLAlchemy, el cual se obtiene como atributo de la clase MySQLConnector
# Hace overwrite dado a que se le pasa el keyword 'replace' dentro de este método
# Para no gastar memoria, "cierra" cada archivo cuando no se lee más dentro de la iteración
# Se tiene un try-except en casos donde lo que se suba no sea un .csv
# Es eficiente para batch transactions ya que puede subir n cantidad de filas mediante DF, entonces no importa el tamaño del csv original
# En caso de querer aplicar alguna limitante a la carga de datos por request implicaría manipular el csv que se ingesta en vez de DF
@app.post('/upload_overwrite')
def upload(files: List[UploadFile] = File(...)):
    r = []
    for file in files:
        try:
            df = pd.read_csv(file.file)
            filename = file.filename.split('.')[0]
            count = df.to_sql(con=connector.engine, name=filename, if_exists='replace', index=False)
            print(count)
        except Exception as e:
            return {'message': e}
        finally:
            file.file.close()
    
    return {'message': r}



# Mediante FastAPI, se tiene un GUI que permite cargar los datos a memoria del programa
# Itera sobre la lista de archivos, los convierte en DataFrames para ser ingestados a la base de datos
# df.to_sql permite cargar los datos dado un "engine" de SQLAlchemy, el cual se obtiene como atributo de la clase MySQLConnector
# Hace overwrite dado a que se le pasa el keyword 'append' dentro de este método
# Para no gastar memoria, "cierra" cada archivo cuando no se lee más dentro de la iteración
# Se tiene un try-except en casos donde lo que se suba no sea un .csv
# Es eficiente para batch transactions ya que puede subir n cantidad de filas mediante DF, entonces no importa el tamaño del csv original
# En caso de querer aplicar alguna limitante a la carga de datos por request implicaría manipular el csv que se ingesta en vez de DF
@app.post('/upload_append')
def upload(files: List[UploadFile] = File(...)):
    r = []
    for file in files:
        try:
            df = pd.read_csv(file.file)
            filename = file.filename.split('.')[0]
            count = df.to_sql(con=connector.engine, name=filename, if_exists='append', index=False)
            print(count)
        except Exception as e:
            return {'message': e}
        finally:
            file.file.close()
    
    return {'message': r}
    

# TODO: métodos que permitan hacer llamadas a la DB y retornen los dos queries requeridos.
# Estos se suministran en un archivo aparte, como manera de pseudocódigo.


# Main de la aplicación, que empieza el servidor local
if __name__ == '__main__':
    preprocessing()
    uvicorn.run(app, host="127.0.0.1", port=8000)

