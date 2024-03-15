"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd

def obtener_nombres_columnas(datos):
    columnas = list(range(4))

    line_index = 0

    # Obtener los nombres de la primera fila
    for i in range(len(columnas)):
        line_index = datos[0].find('  ')
        columnas[i] = datos[0][:line_index].strip()
        datos[0] = datos[0][line_index+1:].lstrip()

    # Completar los nombres de las columnas
    datos[1] = datos[1].lstrip()
    line_index = datos[1].find('  ')
    columnas[1] += ' ' + datos[1][:line_index].strip()
    datos[1] = datos[1][line_index+1:].lstrip()

    line_index = datos[1].find('  ')
    columnas[2] += ' ' + datos[1][:line_index].strip()

    # Formato de nombres de columnas
    columnas = [x.replace(' ', '_').lower() for x in columnas]

    return columnas

def obtener_datos_columnas(clusters_file_lines):
    clusters_report_data = [[] for _ in range(4)]
    lines = clusters_file_lines[4:]
    last_row = 0

    for line in lines:

        if line.strip() == '':
            pass

        elif "%" in line:
            # Separar la línea en dos partes y obtener el índice de la última fila visitada
            line_1 = line[:41].strip().rstrip('%').split()
            line_2 = line[41:]
            last_row = int(line_1[0]) - 1

            # Agregar datos a las listas
            clusters_report_data[0].append(line_1[0])
            clusters_report_data[1].append(line_1[1])
            clusters_report_data[2].append(line_1[2])
            clusters_report_data[3].append(line_2.strip('\n'))

        else:
            # Agregar el resto de la línea al último elemento de la lista
            line_2 = line[41:].strip('\n')
            clusters_report_data[3][last_row] += " " + line_2

    return clusters_report_data


def ingest_data():
    filename = 'clusters_report.txt'

    with open(filename, 'r') as clusters_file:
        data = clusters_file.readlines()

        # Formato de nombres de columnas
        clusters_report_columns = obtener_nombres_columnas(data)
        datos_columnas = obtener_datos_columnas(data)

        # Organizar en un diccionario las columnas con sus correspondientes datos
        clusters_report_data = dict(zip(clusters_report_columns, datos_columnas))
        
        # Convertir a dataframe
        df = pd.DataFrame(clusters_report_data)
        
        # Limpiar datos
        df.cluster = df.cluster.astype(int)
        df.cantidad_de_palabras_clave = df.cantidad_de_palabras_clave.astype(int)
        df.porcentaje_de_palabras_clave = df.porcentaje_de_palabras_clave.str.replace(',', '.').astype(float)
        df.principales_palabras_clave = (df.principales_palabras_clave
                                        .str.replace(' +', ' ', regex=True)
                                        .str.rstrip('.'))

    return df