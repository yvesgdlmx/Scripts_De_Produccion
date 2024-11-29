import csv
import mysql.connector
from datetime import datetime

def parse_date(date_str):
    date_formats = [
        '%d/%m/%Y',    # 13/11/2024
        '%d/%m/%y',    # 13/11/24
        '%Y-%m-%d',    # 2024-11-13
        '%m/%d/%Y',    # 11/13/2024
        '%m/%d/%y'     # 11/13/24
    ]
    for fmt in date_formats:
        try:
            date = datetime.strptime(date_str, fmt)
            if date.year < 100:
                date = date.replace(year=date.year + 2000)
            return date.strftime('%Y-%m-%d')
        except ValueError:
            continue
    raise ValueError(f"No se pudo parsear la fecha: {date_str}")

def process_stations(input_file):
    station_counts = {}
    station_details = {}
    client_counts = {}
    fs_counts = {}
    now = datetime.now()
    current_date = now.strftime('%Y-%m-%d')
    current_time = now.strftime('%H:%M:%S')
    
    with open(input_file, 'r') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader)  # Saltar la primera línea (encabezados)
        for row in reader:
            if len(row) >= 11:
                try:
                    enter_date = parse_date(row[0])
                    tray_number = row[2]
                    station = row[4]
                    current_station_date = parse_date(row[5])
                    client = row[6]  # DIVISION (cliente)
                    sf_value = row[10]
                    fs_value = row[10]  # Valor F/S
                    
                    station_key = f"{station}_{sf_value}"
                    
                    if station_key not in station_counts:
                        station_counts[station_key] = 0
                        station_details[station_key] = []
                        client_counts[station_key] = {'NV': 0, 'HOYA': 0, 'INK': 0}
                        fs_counts[station_key] = {'F': 0, 'S': 0}
                    
                    station_counts[station_key] += 1
                    station_details[station_key].append((enter_date, tray_number, current_station_date, 
                                                     current_date, current_time, sf_value))
                    
                    # Contar por cliente
                    if client == 'NV':
                        client_counts[station_key]['NV'] += 1
                    elif client == 'HO':
                        client_counts[station_key]['HOYA'] += 1
                    elif client == 'INK':
                        client_counts[station_key]['INK'] += 1
                    
                    # Contar F/S
                    if fs_value == 'F':
                        fs_counts[station_key]['F'] += 1
                    elif fs_value == 'S':
                        fs_counts[station_key]['S'] += 1
                        
                except ValueError as e:
                    print(f"Error al procesar la fila: {e}")
                    print(f"Fila problemática: {row}")
                    continue
                    
    return station_counts, station_details, client_counts, fs_counts

try:
    connection = mysql.connector.connect(
        host='autorack.proxy.rlwy.net',
        port=22723,
        user='root',
        password='zsulNCCrYFSfBqIxwwIXIKqLQKFJWwbw',
        database='railway'
    )
    
    if connection.is_connected():
        print("Conexión establecida exitosamente.")
        
    cursor = connection.cursor()
    input_file = 'I:/VISION/a_IP.txt'
    
    station_counts, station_details, client_counts, fs_counts = process_stations(input_file)
    
    data_to_insert = []
    for station_key, count in station_counts.items():
        first_record = station_details[station_key][0]
        station_name = station_key.rsplit('_', 1)[0]
        client_count = client_counts[station_key]
        fs_count = fs_counts[station_key]
        
        data_to_insert.append((
            first_record[0],  # enter_date
            first_record[1],  # tray_number
            station_name,     # estacion
            first_record[2],  # current_station_date
            count,            # total (conteo de registros)
            first_record[3],  # fecha_insercion
            first_record[4],  # hora_insercion
            first_record[5],  # sf_value
            client_count['NV'],
            client_count['HOYA'],
            client_count['INK'],
            fs_count['F'],    # conteo de F
            fs_count['S']     # conteo de S
        ))

    sql_insert = """
    INSERT INTO conteo_estaciones (
        enter_date, tray_number, estacion, current_station_date, 
        total, fecha_insercion, hora_insercion, sf, NVI, HOYA, INK, f_count, s_count
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    cursor.executemany(sql_insert, data_to_insert)
    connection.commit()
    
    print(f"Número de estaciones procesadas: {len(data_to_insert)}")
    print("Datos insertados exitosamente.")
    
    print("\nConteo de registros por estación y cliente:")
    for station_key, count in station_counts.items():
        station_name, sf_value = station_key.rsplit('_', 1)
        client_count = client_counts[station_key]
        fs_count = fs_counts[station_key]
        print(f"{station_name} ({sf_value}): Total: {count}, "
              f"NV: {client_count['NV']}, HOYA: {client_count['HOYA']}, "
              f"INK: {client_count['INK']}, F: {fs_count['F']}, S: {fs_count['S']}")

except mysql.connector.Error as err:
    print("Error al ejecutar el comando SQL:", err)
finally:
    if 'cursor' in locals() and cursor:
        cursor.close()
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        print("Conexión cerrada.")

print("Proceso completado.")