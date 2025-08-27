import asyncio
import sys
import os
from nodo_lsr import LSRNode
from config_loader import cargar_configuracion_topo


async def main_lsr_manual(nombre_nodo: str):
    # Cargar topología
    topo = cargar_configuracion_topo('topo2025-randomX-2025.txt')

    if nombre_nodo not in topo:
        print(f"Error: Nodo {nombre_nodo} no encontrado en topología")
        print(f"Nodos disponibles: {list(topo.keys())}")
        sys.exit(1)

    vecinos = topo[nombre_nodo]

    # Crear nodo LSR
    nodo = LSRNode(
        nombre=nombre_nodo,
        host="lab3.redesuvg.cloud",
        port=6379,
        password="UVGRedis2025",
        vecinos=vecinos,
        grupo="grupo5",  # Cambiar por tu número de grupo
        seccion="sec10"  # Cambiar por tu número de sección
    )

    print(f"\n=== LSR Nodo {nombre_nodo} ===")
    print(f"Vecinos: {vecinos}")
    print("\nComandos disponibles:")
    print("  lsp                      - Enviar LSP para anunciar topología")
    print("  enviar <destino> <msg>   - Enviar mensaje a destino")
    print("  tabla                    - Mostrar tabla de rutas")
    print("  lspdb                    - Mostrar base de datos LSP")
    print("  salir                    - Salir")
    print("\nPRIMERO ejecuta 'lsp' en cada nodo para intercambiar topología\n")

    # Inicializar nodo
    await nodo.suscribirse_a_canales()
    await nodo.inicializar()

    # Crear tarea para escuchar mensajes
    escucha_task = asyncio.create_task(nodo.escuchar_mensajes())

    try:
        while True:
            comando = await asyncio.get_event_loop().run_in_executor(
                None, input, f"{nombre_nodo}> "
            )

            if comando == "lsp":
                lsp_data = await nodo.enviar_lsp_manual()
                print(f"LSP enviado con secuencia {lsp_data['sequence']}")

            elif comando.startswith("enviar "):
                partes = comando.split(" ", 2)
                if len(partes) >= 3:
                    destino, mensaje = partes[1], partes[2]
                    success = await nodo.enviar_mensaje_usuario(destino, mensaje)
                    if success:
                        print(f"Mensaje enviado correctamente")
                else:
                    print("Uso: enviar <destino> <mensaje>")
                    print("Ejemplo: enviar D Hola mundo")

            elif comando == "tabla":
                print("\n=== TABLA DE RUTAS ===")
                if nodo.routing_table:
                    print(f"{'Destino':<10} {'Siguiente':<12} {'Costo':<8}")
                    print("-" * 32)
                    for dest, (next_hop, cost) in sorted(nodo.routing_table.items()):
                        print(f"{dest:<10} {next_hop:<12} {cost:<8.1f}")
                else:
                    print("Tabla vacía - ejecuta 'lsp' primero")
                print("========================\n")

            elif comando == "lspdb":
                print("\n=== BASE DE DATOS LSP ===")
                if nodo.link_state_db:
                    for node, lsp_data in sorted(nodo.link_state_db.items()):
                        print(f"Nodo {node}:")
                        print(f"  Seq: {lsp_data['sequence']}")
                        print(f"  Vecinos: {lsp_data['neighbors']}")
                        print()
                else:
                    print("Base de datos vacía")
                print("==========================\n")

            elif comando == "vecinos":
                print(f"\nVecinos: {nodo.vecinos}")
                print(f"Costos: {nodo.neighbor_costs}\n")

            elif comando == "info":
                print(f"\n=== INFO NODO {nombre_nodo} ===")
                print(f"LSPs enviados: {nodo.lsp_sequence}")
                print(f"LSPs recibidos: {len(nodo.received_lsps)}")
                print(f"Nodos conocidos: {len(nodo.link_state_db)}")
                print(f"Rutas calculadas: {len(nodo.routing_table)}")
                print("=========================\n")

            elif comando == "help" or comando == "ayuda":
                print("\nComandos disponibles:")
                print("  lsp                      - Anunciar mi topología")
                print("  enviar <destino> <msg>   - Enviar mensaje")
                print("  tabla                    - Ver rutas calculadas")
                print("  lspdb                    - Ver base datos enlaces")
                print("  vecinos                  - Ver vecinos configurados")
                print("  info                     - Ver estadísticas del nodo")
                print("  salir                    - Terminar programa")
                print()

            elif comando == "salir":
                break

            elif comando == "":
                continue

            else:
                print(f"Comando '{comando}' no reconocido. Usa 'help' para ver comandos.")

    except KeyboardInterrupt:
        print(f"\n[{nombre_nodo}] Interrumpido por usuario")
    except Exception as e:
        print(f"[{nombre_nodo}] Error: {e}")
    finally:
        print(f"[{nombre_nodo}] Cerrando...")
        escucha_task.cancel()
        try:
            await escucha_task
        except asyncio.CancelledError:
            pass
        await nodo.redis.close()
        print(f"[{nombre_nodo}] Cerrado.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python test_lsr_manual.py <nombre_nodo>")
        print("Ejemplo: python test_lsr_manual.py A")
        sys.exit(1)

    asyncio.run(main_lsr_manual(sys.argv[1]))