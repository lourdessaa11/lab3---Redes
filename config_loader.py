import json

def cargar_configuracion_topo(archivo_topo: str) -> dict:
    with open(archivo_topo, 'r') as f:
        datos = json.loads(f.read().replace("'", "\""))
    return datos['config']

def cargar_configuracion_nombres(archivo_nombres: str) -> dict:
    with open(archivo_nombres, 'r') as f:
        datos = json.loads(f.read().replace("'", "\""))
    return datos['config']