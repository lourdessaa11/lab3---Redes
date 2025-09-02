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
        print("Error: No se encontró archivo de nombres")
        sys.exit(1)

    nombres = cargar_configuracion_nombres(nombres_file)

    if nombre_nodo not in topo:
        print(f"Error: El nodo '{nombre_nodo}' no existe en la topología")
        sys.exit(1)

    if nombre_nodo not in nombres:
        print(f"Error: El nodo '{nombre_nodo}' no está en la configuración de nombres")
        sys.exit(1)

    vecinos = topo[nombre_nodo]
    mi_canal_completo = nombres[nombre_nodo]

    # Extraer información del canal
    partes_canal = mi_canal_completo.split('.')
    if len(partes_canal) != 3:
        print(f"Error: Formato de canal inválido: {mi_canal_completo}")
        sys.exit(1)

    seccion = partes_canal[0]  # ej: sec10
    grupo = partes_canal[1]  # ej: grupo5
    nodo = partes_canal[2]  # ej: A

    print(f"\n=== CONFIGURACIÓN AUTOMÁTICA ===")
    print(f"Nodo: {nombre_nodo}")
    print(f"Sección: {seccion}")
    print(f"Grupo: {grupo}")
    print(f"Mi canal: {mi_canal_completo}")
    print(f"Vecinos: {vecinos}")
    print(f"\n=== CANALES DE NOMBRES ===")
    for nodo_name, canal in nombres.items():
        print(f"  {nodo_name}: {canal}")
    print()

    # Crear nodo con información extraída del archivo de nombres
    nodo = NodoFlooding(
        nombre=nombre_nodo,
        host="lab3.redesuvg.cloud",
        port=6379,
        password="UVGRedis2025",
        vecinos=vecinos,
        nombres_config=nombres,
        grupo=grupo,
        seccion=seccion
    )

    print("Comandos disponibles:")
    print("  hello <destino> [mensaje]              - Enviar mensaje hello")
    print("  message <destino> <mensaje>            - Enviar mensaje normal")
    print("  info [destino]                         - Enviar información de vecinos")
    print("  echo <destino> <mensaje>               - Enviar echo")
    print("  send_to <seccion>.<grupo>.<nodo> <tipo> <mensaje>  - Enviar a canal específico")
    print("  my_channel                             - Mostrar mi canal")
    print("  show_names                             - Mostrar configuración de nombres")
    print("  salir                                  - Cerrar el nodo")
    print()

    await nodo.suscribirse_a_canales()

    escucha_task = asyncio.create_task(nodo.escuchar_mensajes())

    try:
        while True:
            comando = await asyncio.get_event_loop().run_in_executor(
                None, input, f"[{mi_canal_completo}]> "
            )

            partes = comando.strip().split()
            if not partes:
                continue

            cmd = partes[0].lower()

            if cmd == "hello":
                if len(partes) >= 2:
                    destino = partes[1]
                    mensaje = " ".join(partes[2:]) if len(partes) > 2 else ""
                    await nodo.enviar_hello(destino, mensaje)
                    canal_destino = nodo.get_canal_nombre(destino)
                    print(f"Hello enviado a {destino} (canal: {canal_destino})")
                else:
                    print("Uso: hello <destino> [mensaje]")

            elif cmd == "message":
                if len(partes) >= 3:
                    destino = partes[1]
                    mensaje = " ".join(partes[2:])
                    await nodo.enviar_mensaje_usuario(destino, mensaje)
                    canal_destino = nodo.get_canal_nombre(destino)
                    print(f"Mensaje enviado a {destino} (canal: {canal_destino}): {mensaje}")
                else:
                    print("Uso: message <destino> <mensaje>")

            elif cmd == "info":
                if len(partes) >= 2:
                    destino = partes[1]
                    await nodo.enviar_info(destino)
                    print(f"Info enviado a {destino}")
                else:
                    await nodo.enviar_info("broadcast")
                    print("Info enviado por broadcast")

            elif cmd == "echo":
                if len(partes) >= 3:
                    destino = partes[1]
                    mensaje = " ".join(partes[2:])
                    await nodo.enviar_echo(destino, mensaje)
                    canal_destino = nodo.get_canal_nombre(destino)
                    print(f"Echo enviado a {destino} (canal: {canal_destino}): {mensaje}")
                else:
                    print("Uso: echo <destino> <mensaje>")

            elif cmd == "send_to":
                if len(partes) >= 4:
                    canal_destino = partes[1]  # sec10.grupo5.B
                    tipo = partes[2]  # hello, message, echo, info
                    mensaje = " ".join(partes[3:])
                    await nodo.enviar_a_canal_especifico(canal_destino, tipo, mensaje)
                    print(f"Mensaje {tipo} enviado a canal {canal_destino}: {mensaje}")
                else:
                    print("Uso: send_to <seccion>.<grupo>.<nodo> <tipo> <mensaje>")
                    print("Ejemplo: send_to sec20.grupo3.C hello Hola desde otra sección!")

            elif cmd == "my_channel":
                print(f"Tu canal es: {mi_canal_completo}")

            elif cmd == "show_names":
                print("=== CONFIGURACIÓN DE NOMBRES ===")
                for nodo_name, canal in nombres.items():
                    print(f"  {nodo_name}: {canal}")

            elif cmd == "salir":
                break

            else:
                print("Comando no reconocido")
                print("Comandos: hello, message, info, echo, send_to, my_channel, show_names, salir")

    except KeyboardInterrupt:
        pass
    finally:
        escucha_task.cancel()
        try:
            await escucha_task
        except asyncio.CancelledError:
            pass
        await nodo.redis.close()
        print(f"\n[{nombre_nodo}] Cerrando nodo...")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python prueba_interactivo.py <nombre_nodo>")
        print("Ejemplo: python prueba_interactivo.py A")
        sys.exit(1)
    asyncio.run(main_interactivo(sys.argv[1]))
