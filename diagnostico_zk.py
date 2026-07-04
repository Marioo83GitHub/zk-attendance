# diagnostico_zk.py
import sys
from zk import ZK

def test_connection(ip, port, comm_key, force_udp, timeout=5):
    """Prueba una configuración específica de conexión"""
    print(f"\n{'='*60}")
    print(f"Probando: IP={ip}, Port={port}, CommKey={comm_key}, force_udp={force_udp}")
    print('='*60)
    
    try:
        zk = ZK(
            ip=ip,
            port=port,
            timeout=timeout,
            password=comm_key,
            force_udp=force_udp,
            ommit_ping=False,
            verbose=True,  # Ver todos los paquetes enviados/recibidos
        )
        
        print("Intentando connect()...")
        conn = zk.connect()
        
        if conn:
            print("✓ CONEXIÓN EXITOSA")
            print("Intentando get_attendance()...")
            att = conn.get_attendance()
            print(f"✓ Se obtuvieron {len(att)} registros")
            conn.disconnect()
            return True
        else:
            print("✗ connect() retornó None")
            return False
            
    except ConnectionResetError as e:
        print(f"✗ WinError 10054 (TCP RST): {e}")
        return False
    except Exception as e:
        print(f"✗ Error: {type(e).__name__}: {e}")
        return False

if __name__ == "__main__":
    # IPs de tus dispositivos
    devices = ["192.168.15.222", "192.168.15.223"]  # ← AJUSTA ESTAS IPs
    
    # Configuraciones a probar
    configs = [
        {"comm_key": 0, "force_udp": True},   # Configuración actual
        {"comm_key": 0, "force_udp": False},  # Sin forzar UDP
        {"comm_key": 1, "force_udp": True},   # Comm Key = 1
        {"comm_key": 123456, "force_udp": True},  # Comm Key común
    ]
    
    for ip in devices:
        print(f"\n{'#'*60}")
        print(f"DISPOSITIVO: {ip}")
        print('#'*60)
        
        for config in configs:
            success = test_connection(
                ip=ip,
                port=4370,
                comm_key=config["comm_key"],
                force_udp=config["force_udp"]
            )
            if success:
                print(f"\n✓✓✓ CONFIGURACIÓN EXITOSA PARA {ip}: {config}")
                print("Usa esta configuración en tu script principal")
                break