import asyncio
import sys
from nodo_lsr import LSRNode
from config_loader import cargar_configuracion_topo


async def main_interactivo(nombre_nodo: str):
    topo = cargar_configuracion_topo('topo2025-randomX-2025.txt')

    if nombre_nodo not in topo:
        print(f"Error: Nodo {nombre_nodo} no encontrado")
        sys.exit(1)

    vecinos = topo[nombre_nodo]

    nodo = LSRNode(
        nombre=nombre_nodo,
        host="lab3.redesuvg.cloud",
        port=6379,
        password="UVGRedis2025",
        vecinos=vecinos,
        grupo="grupo5",
        seccion="sec10"
    )

    await nodo.suscribirse_a_canales()
    await nodo.iniciar_periodic_tasks()

    escucha_task = asyncio.create_task(nodo.escuchar_mensajes())

    print(f"[LSR {nombre_nodo}] Nodo listo. Comandos:")
    print("  enviar <destino> <mensaje> - Enviar mensaje")
    print("  tabla - Mostrar tabla de rutas")
    print("  lspdb - Mostrar base de datos LSP")
    print("  salir - Salir")

    try:
        while True:
            comando = await asyncio.get_event_loop().run_in_executor(
                None, input, f"[LSR {nombre_nodo}]> "
            )

            if comando.startswith("enviar "):
                partes = comando.split(" ", 2)
                if len(partes) >= 3:
                    destino, mensaje = partes[1], partes[2]
                    await nodo.enviar_mensaje_usuario(destino, mensaje)
                else:
                    print("Uso: enviar <destino> <mensaje>")

            elif comando == "tabla":
                print("Tabla de rutas:", nodo.routing_table)

            elif comando == "lspdb":
                print("Base de datos LSP:", nodo.link_state_db)

            elif comando == "salir":
                break

            else:
                print("Comando no reconocido")

    except KeyboardInterrupt:
        pass
    finally:
        escucha_task.cancel()
        await nodo.redis.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python test_lsr_interactivo.py <nombre_nodo>")
        sys.exit(1)
    asyncio.run(main_interactivo(sys.argv[1]))