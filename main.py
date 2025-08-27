import asyncio
import sys
import os
from Flooding_redis import NodoFlooding
from config_loader import cargar_configuracion_topo, cargar_configuracion_nombres


async def main(nombre_nodo: str):
    topo = cargar_configuracion_topo('topo2025-randomX-2025.txt')

    nombres_file = None
    for file in os.listdir('.'):
        if file.startswith('names') and file.endswith('.txt'):
            nombres_file = file
            break

    if not nombres_file:
        print("Error: No se encontró archivo")
        sys.exit(1)

    nombres = cargar_configuracion_nombres(nombres_file)

    # Verificar que el nodo existe en la topología
    if nombre_nodo not in topo:
        print(f"Error: El nodo '{nombre_nodo}' no existe en la topología")
        print(f"Nodos disponibles: {list(topo.keys())}")
        sys.exit(1)

    # Obtener vecinos del nodo actual
    vecinos = topo[nombre_nodo]

    # Crear instancia del nodo
    nodo = NodoFlooding(
        nombre=nombre_nodo,
        host="lab3.redesuvg.cloud",
        port=6379,
        password="UVGRedis2025",
        vecinos=vecinos,
        grupo="grupo5",
        seccion="sec10"
    )

    print(f"Iniciando nodo {nombre_nodo} con vecinos: {vecinos}")

    # Ejecutar el nodo
    try:
        await nodo.ejecutar()
    except KeyboardInterrupt:
        print(f"\n[{nombre_nodo}] Cerrando nodo...")
        await nodo.redis.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python main.py <nombre_nodo>")
        print("Ejemplo: python main.py A")
        sys.exit(1)
    asyncio.run(main(sys.argv[1]))