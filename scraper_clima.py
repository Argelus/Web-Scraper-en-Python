import requests
import pandas as pd
import time
import math

# --- Configuración ---
# Cambia estos valores según la estación y el rango de fechas que necesites.
# El ID "00206D77" corresponde a "La Noria" en Pátzcuaro.
STATION_SK = "00206D77" 
FECHA_INICIO = "2015-01-01"
FECHA_FIN = "2024-10-04"
NOMBRE_ARCHIVO_SALIDA = "datos_climaticos_final.csv"
# --------------------

BASE_URL = "https://estaciones.apeamac.com/history"
# Esta es la URL correcta que descubrimos para obtener los datos
DATA_URL = "https://estaciones.apeamac.com/assets/scripts/server_processing.php" 

# Usamos una sesión para que maneje las cookies automáticamente
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Referer": BASE_URL
})

try:
    # Paso 1: Visitar la página para obtener la cookie de sesión.
    print("Paso 1: Estableciendo sesión con el servidor...")
    session.get(BASE_URL, timeout=15).raise_for_status()
    print("  -> Sesión establecida con éxito.")

    # Paso 2: Hacer la primera solicitud para saber el total de registros
    payload_inicial = {
        "station_sk": STATION_SK,
        "date_from": FECHA_INICIO,
        "date_to": FECHA_FIN,
        "start": 0,
        "length": 1,
        "draw": 1
    }
    
    print("\nPaso 2: Obteniendo el número total de registros...")
    response_data = session.post(DATA_URL, data=payload_inicial)
    response_data.raise_for_status()
    
    json_response = response_data.json()
    total_records = json_response.get("recordsTotal", 0)
    
    if total_records == 0:
        print("No se encontraron registros para la estación y fechas seleccionadas.")
        exit()
        
    page_size = 750
    total_pages = math.ceil(total_records / page_size)
    
    print(f"Total de registros a descargar: {total_records}")
    print(f"Se realizarán {total_pages} solicitudes en total.")

    # Paso 3: Iterar para obtener todos los datos por lotes
    all_data = []
    print("\nPaso 3: Iniciando la descarga de datos por lotes...")
    
    for page in range(total_pages):
        start_record = page * page_size
        print(f"  -> Obteniendo lote {page + 1} de {total_pages}...")
        
        payload = {
            "station_sk": STATION_SK,
            "date_from": FECHA_INICIO,
            "date_to": FECHA_FIN,
            "start": start_record,
            "length": page_size,
            "draw": page + 2
        }
        
        response = session.post(DATA_URL, data=payload)
        response.raise_for_status()
        
        data = response.json().get("data", [])
        all_data.extend(data)
        time.sleep(1) # Pausa para no sobrecargar el servidor

    # Paso 4: Guardar los datos en un archivo CSV
    print("\nPaso 4: Guardando los datos en el archivo CSV...")
    
    # Nombres de las columnas según la tabla de la página
    column_names = [
        "Estacion", "Fecha", 
        "Temp_Prom", "Temp_Max", "Temp_Min",
        "PuntoRocio_Prom", "PuntoRocio_Min",
        "RadSolar_Prom", 
        "DPV_Prom", "DPV_Min",
        "HumRel_Prom", "HumRel_Max", "HumRel_Min",
        "Precipitacion_Sum",
        "HumHoja_Tiempo",
        "VientoVel_Prom", "VientoVel_Max",
        "VientoRafaga_Max",
        "DeltaT_Prom", "DeltaT_Max", "DeltaT_Min",
        "DuracionSol_Tiempo",
        "ET0"
    ]
    
    # Verificación para evitar errores si el número de columnas no coincide
    num_columns = len(all_data[0]) if all_data else len(column_names)
    if num_columns != len(column_names):
        print(f"Advertencia: El número de columnas de datos ({num_columns}) no coincide con los nombres definidos ({len(column_names)}). Se usarán columnas genéricas.")
        column_names = [f"Columna_{i+1}" for i in range(num_columns)]

    df = pd.DataFrame(all_data, columns=column_names)
    df.to_csv(NOMBRE_ARCHIVO_SALIDA, index=False, encoding='utf-8-sig')
    
    print(f"\n🎉 ¡MISIÓN CUMPLIDA! Se guardaron {len(df)} registros en el archivo '{NOMBRE_ARCHIVO_SALIDA}'")

except requests.exceptions.RequestException as e:
    print(f"\nOcurrió un error de red: {e}")
except Exception as e:
    print(f"\nOcurrió un error inesperado: {e}")