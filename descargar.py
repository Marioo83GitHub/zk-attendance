from zk import ZK, const
import psycopg2
from datetime import datetime

def conectar_bd():
    # Conexión a tu PostgreSQL local
    return psycopg2.connect(
        dbname="asistencia_caritas",
        user="postgres",
        password="tu_password",
        host="127.0.0.1",
        port="5432"
    )

def descargar_marcaciones():
    # IP del F22 en la red local de Cáritas
    zk = ZK('192.168.15.50', port=4370, timeout=10, password=0, force_udp=False)
    conn = None
    db_conn = None
    
    try:
        print("Conectando al F22...")
        conn = zk.connect()
        
        # Deshabilitar el reloj temporalmente mientras lee para evitar errores
        conn.disable_device()
        
        print("Obteniendo registros de asistencia...")
        attendances = conn.get_attendance()
        
        # Conectar a PostgreSQL para guardar
        db_conn = conectar_bd()
        cursor = db_conn.cursor()
        
        registros_nuevos = 0
        
        for att in attendances:
            # att.user_id: ID del empleado
            # att.timestamp: Fecha y hora exacta (datetime)
            # att.punch: Tipo (Entrada=0, Salida=1, etc.)
            
            # Usamos un INSERT con ON CONFLICT para evitar duplicados 
            # si el script lee las mismas marcaciones en la siguiente ejecución
            query = """
                INSERT INTO marcaciones (empleado_id, fecha_hora, tipo_marcacion) 
                VALUES (%s, %s, %s)
                ON CONFLICT (empleado_id, fecha_hora) DO NOTHING;
            """
            cursor.execute(query, (att.user_id, att.timestamp, att.punch))
            
            # Si el rowcount es 1, significa que fue un registro nuevo insertado
            if cursor.rowcount == 1:
                registros_nuevos += 1
                
        db_conn.commit()
        print(f"Sincronización completa. {registros_nuevos} registros nuevos guardados.")
        
        # Habilitar el reloj nuevamente
        conn.enable_device()
        
    except Exception as e:
        print(f"Error en la conexión o guardado: {e}")
        
    finally:
        if conn:
            conn.disconnect()
        if db_conn:
            cursor.close()
            db_conn.close()

if __name__ == "__main__":
    descargar_marcaciones()