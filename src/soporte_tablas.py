from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from time import sleep
from selenium import webdriver  
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager 
from selenium.webdriver.common.keys import Keys  
from selenium.webdriver.support.ui import Select  
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException 
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors 


def conexion(nombre):
    try:  
        conexion = psycopg2.connect(
        database = nombre,
        user = "postgres",
        password = "admin",
        host = "localhost",
        port = "5432")
        
    except OperationalError as e:
        if e.pgcode == errorcodes.INVALID_PASSWORD:
            print("La contraseña es erronea")
        elif e.pgcode == errorcodes.CONNECTION_EXCEPTION:
            print("Error de conexion")
        else:
            print(f"Ocurrió el error {e}")

    return conexion



def crear_tablas(conexion, cursor):
    
    cursor.execute("""CREATE TABLE Actriz_Actor (
    Id INT PRIMARY KEY AUTO_INCREMENT,
    Nombre VARCHAR(100),
    Año_Nacimiento INT,
    Conocido_Por TEXT,
    Que_Hace TEXT
);""")
    
    cursor.execute('''
    CREATE TABLE Peliculas (
    Id INT PRIMARY KEY AUTO_INCREMENT,
    Titulo VARCHAR(200),
    Calificacion_IMDb FLOAT,
    Director VARCHAR(100),
    Guion TEXT,
    Argumento TEXT,
    Duracion VARCHAR(50),
    IMDb_Id VARCHAR(50),
    Actor_Id INT,
    FOREIGN KEY (Actor_Id) REFERENCES Actriz_Actor(Id)
);
    ''')
    cursor.execute('''
    CREATE TABLE Premios (
    Id INT PRIMARY KEY AUTO_INCREMENT,
    Descripcion VARCHAR(255),
    Año INT,
    Actor_Id INT,
    Pelicula_Id INT,
    FOREIGN KEY (Actor_Id) REFERENCES Actriz_Actor(Id),
    FOREIGN KEY (Pelicula_Id) REFERENCES Peliculas(Id)
);
    ''');
    cursor.execute('''
    CREATE TABLE Genero_Pelicula (
    Id INT PRIMARY KEY AUTO_INCREMENT,
    Genero VARCHAR(50),
    Pelicula_Id INT,
    FOREIGN KEY (Pelicula_Id) REFERENCES Peliculas(Id)
);
    ''')
    

def insertar_datos(df_mercado, conexion):
    cursor = conexion.cursor()

    lista_tuplas_mercados = [tuple(fila) for fila in df_artistas[['id_supermercado', 'Supermercado']].values]
    query_insercion_super = '''
    INSERT INTO Supermercados (id_supermercado, nombre)
    VALUES (%s, %s)
    ON CONFLICT (id_supermercado) DO NOTHING;
    '''
    cursor.executemany(query_insercion_super, lista_tuplas_mercados)
    conexion.commit()

    lista_tuplas_productos = [tuple(fila) for fila in df_mercado[['id_supermercado', 'Categoria', 'producto', 'Marca', 'Volumen']].values]
    query_insercion_productos = '''
    INSERT INTO Productos (id_supermercado, categoria, producto, Marca, Volumen)
    VALUES (%s, %s, %s, %s, %s);
    '''
    cursor.executemany(query_insercion_productos, lista_tuplas_productos)
    conexion.commit()

    lista_tuplas_precios = [tuple(fila) for fila in df_mercado[['id_supermercado', 'fecha', 'Precio', 'Porcentaje']].values]
    query_insercion_precios = '''
    INSERT INTO Precios (id_supermercado, fecha, Precio, Porcentaje)
    VALUES (%s, %s, %s, %s);
    '''
    cursor.executemany(query_insercion_precios, lista_tuplas_precios)
    conexion.commit()