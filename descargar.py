#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
descargar.py
============
Servidor Flask para extracción de marcaciones de asistencia desde ZKTeco F22
vía protocolo TCP/IP (puerto 4370) usando pyzk con mitigación de WinError 10054.

Autor: Senior Systems Engineer
Protocolo: ZKTeco Standalone (ZEM600/ZEM460) - Pull Mode
"""

import os
import sys
import csv
import json
import time
import socket
import logging
import threading
from datetime import datetime
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from typing import List, Optional, Generator, Dict, Any

from flask import Flask, jsonify, request, Response, send_file
from zk import ZK, const
from zk.exception import ZKErrorResponse

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

APP_CONFIG = {
    "DEVICE_IP": os.getenv("ZK_IP", "192.168.15.222"),
    "DEVICE_PORT": int(os.getenv("ZK_PORT", 4370)),
    "COMM_KEY": int(os.getenv("ZK_COMM_KEY", 0)),  # Comm Key del F22
    "TIMEOUT": 10,
    "FORCE_UDP_OVER_TCP": True,  # Mitigación clave para F22
    "MAX_RETRIES": 3,
    "RETRY_DELAY": 2,
    "SESSION_LOCK_TIMEOUT": 30,
    "EXPORT_DIR": os.getenv("EXPORT_DIR", "./exports"),
}

# Logging ingenieril para diagnóstico de protocolo
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("zk_protocol.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("ZKTeco-F22-Pull")


# ============================================================================
# MODELOS
# ============================================================================

@dataclass
class AttendanceRecord:
    uid: str
    user_id: str
    name: str
    timestamp: datetime
    status: int
    punch_type: str

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat() if self.timestamp else None
        return d


# ============================================================================
# GESTIÓN DE SESIONES (PREVENCIÓN DE GHOST SESSIONS)
# ============================================================================

class SessionManager:
    """
    Garantiza exclusividad en el socket TCP del F22.
    El firmware solo permite 1 conexión concurrente (Pull).
    """
    
    _lock = threading.Lock()
    _active_session = False
    
    @classmethod
    @contextmanager
    def exclusive_session(cls, timeout: int = APP_CONFIG["SESSION_LOCK_TIMEOUT"]):
        acquired = cls._lock.acquire(timeout=timeout)
        if not acquired:
            raise RuntimeError(
                "SessionManager: Timeout de exclusividad. "
                "Otra instancia está consumiendo el socket TCP del F22."
            )
        cls._active_session = True
        logger.debug("SessionManager: Lock adquirido. Socket exclusivo.")
        try:
            yield
        finally:
            cls._active_session = False
            cls._lock.release()
            logger.debug("SessionManager: Lock liberado.")


# ============================================================================
# NÚCLEO DEL PROTOCOLO ZK
# ============================================================================

class ZKPullClient:
    """
    Cliente optimizado para F22 con mitigaciones específicas de firmware.
    """

    def __init__(
        self,
        ip: str = APP_CONFIG["DEVICE_IP"],
        port: int = APP_CONFIG["DEVICE_PORT"],
        comm_key: int = APP_CONFIG["COMM_KEY"],
        timeout: int = APP_CONFIG["TIMEOUT"],
        force_udp: bool = APP_CONFIG["FORCE_UDP_OVER_TCP"],
    ):
        self.ip = ip
        self.port = port
        self.comm_key = comm_key
        self.timeout = timeout
        self.force_udp = force_udp
        self._connection = None

    def _pre_flight_check(self) -> bool:
        """
        Verifica reachability a nivel de capa 4 antes de invocar pyzk.
        Evita timeouts largos en pyzk si el dispositivo está offline.
        """
        try:
            with socket.create_connection((self.ip, self.port), timeout=2):
                return True
        except (socket.timeout, ConnectionRefusedError, OSError) as e:
            logger.warning(f"Pre-flight TCP check failed: {e}")
            return False

    def _build_zk_instance(self) -> ZK:
        """
        Construye la instancia ZK con los parámetros optimizados para F22.
        force_udp=True es CRÍTICO: encapsula tramas UDP (0x50 0x80) sobre TCP,
        comportamiento esperado por firmwares ZEM600/ZEM460 en modo Pull.
        """
        return ZK(
            ip=self.ip,
            port=self.port,
            timeout=self.timeout,
            password=self.comm_key,
            force_udp=self.force_udp,
            ommit_ping=False,
            verbose=False,
            encoding="UTF-8",
        )

    def _attempt_connect(self, zk: ZK) -> Any:
        """
        Intenta conexión con manejo específico de WinError 10054.
        El error 10054 es un TCP RST enviado por el firmware al rechazar
        el CMD_CONNECT inicial.
        """
        try:
            conn = zk.connect()
            if conn:
                # Validación post-conexión: algunos firmwares aceptan el socket
                # pero rechazan comandos. Verificamos con un comando ligero.
                try:
                    conn.disable_device()
                    conn.enable_device()
                except Exception as inner_e:
                    logger.warning(f"Conexión inestable post-handshake: {inner_e}")
                    # No rompemos aún, algunos F22 aceptan get_attendance igual
                return conn
            return None
        except ConnectionResetError as e:
            # Este es el WinError 10054 en Linux/Unix systems.
            # En Windows se manifiesta como OSError con errno 10054.
            logger.error(f"WinError 10054 (TCP RST): {e}")
            raise
        except OSError as e:
            if e.errno == 10054 or "10054" in str(e):
                logger.error(f"WinError 10054 capturado como OSError: {e}")
                raise ConnectionResetError(str(e))
            raise
        except Exception as e:
            logger.error(f"Error inesperado en connect(): {e}")
            raise

    @contextmanager
    def connection(self) -> Generator[Any, None, None]:
        """
        Context manager que garantiza la liberación del socket TCP.
        CRÍTICO: Previene Ghost Sessions que causan 10054 en ejecuciones futuras.
        """
        if not self._pre_flight_check():
            raise ConnectionError(
                f"Dispositivo {self.ip}:{self.port} no responde a SYN TCP. "
                "Verifica que ADMS no esté en modo failover bloqueando el puerto."
            )

        with SessionManager.exclusive_session():
            zk = self._build_zk_instance()
            conn = None
            try:
                conn = self._attempt_connect(zk)
                if not conn:
                    raise ConnectionError("pyzk.connect() retornó None.")
                self._connection = conn
                logger.info(f"Sesión establecida con {self.ip} (force_udp={self.force_udp})")
                yield conn
            except ConnectionResetError:
                logger.critical(
                    "Firmware F22 rechazó la sesión (TCP RST). "
                    "Causas probables: Comm Key incorrecto, Ghost Session previa, "
                    "o firmware incompatible con la negociación estándar de pyzk."
                )
                raise
            finally:
                # Desconexión obligatoria: envía CMD_EXIT al firmware
                if conn:
                    try:
                        logger.info("Enviando CMD_EXIT al firmware...")
                        conn.disconnect()
                    except Exception as disc_err:
                        logger.warning(f"Error durante disconnect (no fatal): {disc_err}")
                    finally:
                        self._connection = None

    def fetch_attendance(self) -> List[AttendanceRecord]:
        """
        Extrae el log de asistencia completo del F22.
        """
        records: List[AttendanceRecord] = []
        
        with self.connection() as conn:
            logger.info("Descargando buffer de asistencia...")
            raw_attendance = conn.get_attendance()
            
            if not raw_attendance:
                logger.warning("El buffer de asistencia está vacío.")
                return records

            # Obtener usuarios para enriquecer registros
            users = {}
            try:
                for user in conn.get_users():
                    users[user.user_id] = user.name
            except Exception as e:
                logger.warning(f"No se pudieron obtener usuarios: {e}")

            for att in raw_attendance:
                try:
                    punch_type = self._resolve_punch_type(att.punch)
                    record = AttendanceRecord(
                        uid=str(att.uid),
                        user_id=str(att.user_id),
                        name=users.get(str(att.user_id), f"UID_{att.user_id}"),
                        timestamp=att.timestamp,
                        status=att.status,
                        punch_type=punch_type,
                    )
                    records.append(record)
                except Exception as e:
                    logger.warning(f"Error parseando registro {att}: {e}")

            logger.info(f"Se extrajeron {len(records)} registros de asistencia.")
            return records

    @staticmethod
    def _resolve_punch_type(punch_code: int) -> str:
        """Mapea códigos de punch a descripciones legibles."""
        mapping = {
            0: "Check-In",
            1: "Check-Out",
            2: "Break-Out",
            3: "Break-In",
            4: "Overtime-In",
            5: "Overtime-Out",
        }
        return mapping.get(punch_code, f"Unknown({punch_code})")

    def clear_attendance_log(self) -> bool:
        """
        Limpia el buffer de asistencia del dispositivo.
        ADVERTENCIA: Esta operación es IRREVERSIBLE.
        """
        with self.connection() as conn:
            try:
                conn.clear_attendance()
                logger.info("Buffer de asistencia purgado del F22.")
                return True
            except Exception as e:
                logger.error(f"Fallo al limpiar buffer: {e}")
                return False


# ============================================================================
# SERVICIO DE EXPORTACIÓN
# ============================================================================

class ExportService:
    
    @staticmethod
    def to_csv(records: List[AttendanceRecord], filename: str) -> str:
        os.makedirs(APP_CONFIG["EXPORT_DIR"], exist_ok=True)
        filepath = os.path.join(APP_CONFIG["EXPORT_DIR"], filename)
        
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["UID", "User_ID", "Nombre", "Timestamp", "Status", "Punch_Type"])
            for r in records:
                writer.writerow([
                    r.uid,
                    r.user_id,
                    r.name,
                    r.timestamp.isoformat() if r.timestamp else "",
                    r.status,
                    r.punch_type,
                ])
        logger.info(f"Exportado CSV: {filepath}")
        return filepath

    @staticmethod
    def to_json(records: List[AttendanceRecord]) -> str:
        return json.dumps([r.to_dict() for r in records], indent=2, ensure_ascii=False)


# ============================================================================
# FLASK APP
# ============================================================================

app = Flask(__name__)
zk_client = ZKPullClient()


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "device": APP_CONFIG["DEVICE_IP"], "timestamp": datetime.now().isoformat()})


@app.route("/api/attendance", methods=["GET"])
def get_attendance():
    """
    Endpoint principal para extracción de marcaciones.
    Parámetros query opcionales:
      - format: 'json' (default) | 'csv'
      - clear: 'true' para purgar el buffer del dispositivo después de leer.
    """
    fmt = request.args.get("format", "json").lower()
    should_clear = request.args.get("clear", "false").lower() == "true"

    logger.info(f"Solicitud de descarga recibida (format={fmt}, clear={should_clear})")

    try:
        records = zk_client.fetch_attendance()

        if should_clear and records:
            logger.warning("Solicitud de purga de buffer detectada.")
            zk_client.clear_attendance_log()

        if fmt == "csv":
            filename = f"asistencia_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            filepath = ExportService.to_csv(records, filename)
            return send_file(
                filepath,
                as_attachment=True,
                download_name=filename,
                mimetype="text/csv",
            )
        else:
            return jsonify({
                "count": len(records),
                "extracted_at": datetime.now().isoformat(),
                "records": [r.to_dict() for r in records],
            })

    except ConnectionResetError as e:
        logger.critical(f"Error 10054 crítico: {e}")
        return jsonify({
            "error": "PROTOCOL_RESET",
            "message": (
                "El firmware del F22 forzó el cierre de la conexión (WinError 10054). "
                "Posibles causas: Comm Key incorrecto, sesión fantasma previa, "
                "o incompatibilidad de firmware. Reinicie físicamente el dispositivo "
                "y verifique el Comm Key."
            ),
            "details": str(e),
        }), 502

    except ConnectionError as e:
        logger.error(f"Error de conectividad: {e}")
        return jsonify({
            "error": "CONNECTIVITY",
            "message": str(e),
        }), 503

    except RuntimeError as e:
        logger.error(f"Error de concurrencia: {e}")
        return jsonify({
            "error": "CONCURRENCY",
            "message": str(e),
        }), 429

    except Exception as e:
        logger.exception(f"Error no controlado: {e}")
        return jsonify({
            "error": "INTERNAL",
            "message": str(e),
        }), 500


@app.route("/api/attendance/clear", methods=["POST"])
def clear_attendance():
    """Endpoint de emergencia para purgar el buffer del F22."""
    try:
        success = zk_client.clear_attendance_log()
        return jsonify({"purged": success}), 200 if success else 500
    except Exception as e:
        logger.exception(f"Error en purga: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# PUNTO DE ENTRADA
# ============================================================================

if __name__ == "__main__":
    logger.info("=" * 70)
    logger.info("ZKTeco F22 Pull Service - Iniciando")
    logger.info(f"Target: {APP_CONFIG['DEVICE_IP']}:{APP_CONFIG['DEVICE_PORT']}")
    logger.info(f"Comm Key: {APP_CONFIG['COMM_KEY']} | force_udp: {APP_CONFIG['FORCE_UDP_OVER_TCP']}")
    logger.info("=" * 70)
    
    # Advertencia de desarrollo: usar Gunicorn/uWSGI en producción
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
        threaded=True,  # Necesario para el SessionManager
    )