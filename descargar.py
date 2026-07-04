from flask import Flask, Response
from zk import ZK
import csv
import io
from datetime import datetime

app = Flask(__name__)

@app.route('/descargar')
def descargar_csv():
    # Usamos la IP .222 que confirmaste que responde
    # force_udp=False porque Test-NetConnection ya nos confirmó que el puerto TCP está abierto
    zk = ZK('192.168.15.222', port=4370, timeout=15, password=0, force_udp=True)
    conn = None
    
    try:
        print("Intentando conectar al F22...")
        conn = zk.connect()
        conn.disable_device()
        
        print("Conexión exitosa. Extrayendo marcaciones...")
        attendances = conn.get_attendance()
        
        # Crear el CSV en memoria
        output = io.StringIO()
        writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        
        # Cabecera para Excel en español
        writer.writerow(['ID Empleado', 'Fecha y Hora', 'Tipo Marcacion'])
        
        for att in attendances:
            tipo = "Entrada" if att.punch == 0 else "Salida" if att.punch == 1 else att.punch
            writer.writerow([att.user_id, att.timestamp.strftime('%Y-%m-%d %H:%M:%S'), tipo])
            
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
            conn.enable_device()
            conn.disconnect()

if __name__ == '__main__':
    print("Iniciando servidor local de asistencia...")
    # Corre en el puerto 8000 para no chocar con nada más en el servidor
    app.run(host='0.0.0.0', port=8000)