import json

def cargar_configuracion_topo(archivo_topo: str) -> dict:
    try:
        with open(archivo_topo, 'r') as f:
            contenido = f.read().replace("'", "\"")
            datos = json.loads(contenido)
        return datos.get('config', {})
    except Exception as e:
        print(f"Error cargando topología {archivo_topo}: {e}")
        return {}

def cargar_configuracion_nombres(archivo_nombres: str) -> dict:
    try:
        with open(archivo_nombres, 'r') as f:
            contenido = f.read().replace("'", "\"")
            datos = json.loads(contenido)
        return datos.get('config', {})
    except Exception as e:
        print(f"Error cargando nombres {archivo_nombres}: {e}")
        return {}
