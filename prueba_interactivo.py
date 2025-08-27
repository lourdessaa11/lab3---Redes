import asyncio
import sys
from Flooding_redis import NodoFlooding
from config_loader import cargar_configuracion_topo, cargar_configuracion_nombres
import os


async def main_interactivo(nombre_nodo: str):
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

    if nombre_nodo not in topo:
        print(f"Error: El nodo '{nombre_nodo}' no existe en la topología")
        sys.exit(1)

    vecinos = topo[nombre_nodo]

    nodo = NodoFlooding(
        nombre=nombre_nodo,
        host="lab3.redesuvg.cloud",
        port=6379,
        password="UVGRedis2025",
        vecinos=vecinos,
        grupo="grupo5",  # Cambiar por tu número de grupo
        seccion="sec10"  # Cambiar por tu número de sección
    )

    print(f"Nodo {nombre_nodo} iniciado con vecinos: {vecinos}")
    print("Comandos disponibles:")
    print("  enviar <destino> <mensaje>")
    print("  salir")

    await nodo.suscribirse_a_canales()

    escucha_task = asyncio.create_task(nodo.escuchar_mensajes())

    try:
        while True:
            comando = await asyncio.get_event_loop().run_in_executor(
                None, input, f"[{nombre_nodo}]> "
            )

            if comando.startswith("enviar "):
                partes = comando.split(" ", 2)
                if len(partes) >= 3:
                    destino = partes[1]
                    mensaje = partes[2]
                    await nodo.enviar_mensaje_usuario(destino, mensaje)
                    print(f"Mensaje enviado a {destino}: {mensaje}")
                else:
                    print("Uso: enviar <destino> <mensaje>")

            elif comando == "salir":
                break
            else:
                print("Comando no reconocido")

    except KeyboardInterrupt:
        pass
    finally:
        escucha_task.cancel()
        await nodo.redis.close()
        print(f"\n[{nombre_nodo}] Cerrando nodo...")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python test_interactivo.py <nombre_nodo>")
        print("Ejemplo: python test_interactivo.py A")
        sys.exit(1)
    asyncio.run(main_interactivo(sys.argv[1]))