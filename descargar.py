from flask import Flask, Response
from zk import ZK
import csv
import io
from datetime import datetime

app = Flask(__name__)

@app.route('/descargar')
def descargar_csv():
    # IP fija .222. force_udp=False es lo estándar para TCP. 
    # password=0 es el estándar de ZKTeco cuando no hay clave.
    zk = ZK('192.168.15.222', port=4370, timeout=10, password=0, force_udp=False)
    conn = None
    
    try:
        print("Intentando conectar al F22...")
        conn = zk.connect()
        
        # Test de conexión para verificar que el socket esté realmente estable
        conn.test_connect()
        
        # Deshabilitar dispositivo para lectura segura
        conn.disable_device()
        
        print("Conexión exitosa. Extrayendo marcaciones...")
        attendances = conn.get_attendance()
        
        # Crear el CSV en memoria
        output = io.StringIO()
        writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        
        # Cabecera para Excel
        writer.writerow(['ID Empleado', 'Fecha y Hora', 'Tipo Marcacion'])
        
        for att in attendances:
            # Tipos comunes: 0=Entrada, 1=Salida, 2=Check-in, 3=Check-out
            # Si el F22 devuelve otro valor, lo ponemos tal cual
            tipo_map = {0: 'Entrada', 1: 'Salida', 2: 'Check-in', 3: 'Check-out'}
            tipo_texto = tipo_map.get(att.punch, f"Tipo {att.punch}")
            
            writer.writerow([att.user_id, att.timestamp.strftime('%Y-%m-%d %H:%M:%S'), tipo_texto])
            
        output.seek(0)
        nombre_archivo = f"Asistencia_Caritas_{datetime.now().strftime('%d-%m-%Y_%H-%M')}.csv"
        
        print("CSV generado. Enviando al usuario...")
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-disposition": f"attachment; filename={nombre_archivo}"}
        )
        
    except Exception as e:
        print(f"Error detectado: {str(e)}")
        return f"Error al conectar con el reloj F22: {str(e)}", 500
        
    finally:
        if conn:
            try:
                conn.enable_device()
                conn.disconnect()
            except:
                pass

if __name__ == '__main__':
    print("Iniciando servidor local de asistencia...")
    # host='0.0.0.0' para que otras PCs de la red puedan verlo
    app.run(host='0.0.0.0', port=8000)