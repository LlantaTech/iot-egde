import socket

def has_internet(host="8.8.8.8", port=53, timeout=3):
    """
    Verifica si hay conexión a Internet intentando conectarse a un DNS público (Google).
    """
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except socket.error:
        return False