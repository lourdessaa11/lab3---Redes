import asyncio
import sys
import json
from nodo_lsr import LSRNode


def cargar_vecinos_desde_topo(archivo_topo: str, mi_nodo: str) -> list:
    """Carga solo los vecinos del nodo actual desde el archivo de topología"""
    try:
        with open(archivo_topo, 'r') as f:
            contenido = f.read().replace("'", '"')
            data = json.loads(contenido)

        if mi_nodo in data['config']:
            return data['config'][mi_nodo]
        else:
            print(f"Advertencia: Nodo {mi_nodo} no encontrado en topología")
            return []
    except Exception as e:
        print(f"Error cargando topología: {e}")
        return []


def cargar_mapeo_nombres(archivo_nombres: str) -> dict:
    """Carga el mapeo de nombres de nodo a usuarios reales"""
    try:
        with open(archivo_nombres, 'r') as f:
            contenido = f.read().replace("'", '"')
            data = json.loads(contenido)
        return data['config']
    except Exception as e:
        print(f"Error cargando mapeo de nombres: {e}")
        return {}


async def main_lsr_manual(nombre_nodo: str, archivo_topo: str, archivo_nombres: str):
    """
    Función principal para ejecutar un nodo LSR

    Args:
        nombre_nodo: Tu nombre de nodo (ej: 'A', 'B', etc.)
        archivo_topo: Archivo con la topología
        archivo_nombres: Archivo con el mapeo de nombres
    """
    # Cargar mapeo de nombres
    mapeo_nombres = cargar_mapeo_nombres(archivo_nombres)

    # Verificar que nuestro nodo existe en el mapeo
    if nombre_nodo not in mapeo_nombres:
        print(f"Error: Nodo {nombre_nodo} no encontrado en el archivo de nombres")
        return

    # Obtener nuestro usuario REAL
    usuario_real = mapeo_nombres[nombre_nodo]

    # Cargar vecinos desde topología
    vecinos_nodos = cargar_vecinos_desde_topo(archivo_topo, nombre_nodo)

    # Convertir nombres de vecinos a usuarios REALES usando el mapeo
    vecinos_reales = []
    for vecino_nodo in vecinos_nodos:
        if vecino_nodo in mapeo_nombres:
            vecinos_reales.append(mapeo_nombres[vecino_nodo])
        else:
            print(f"Advertencia: Vecino {vecino_nodo} no encontrado en mapeo de nombres")

    # Extraer grupo del usuario real
    grupo = "grupo5"  # Default

    # Crear nodo LSR con usuarios REALES
    nodo = LSRNode(
        usuario_real=usuario_real,
        host="lab3.redesuvg.cloud",
        port=6379,
        password="UVGRedis2025",
        vecinos_reales=vecinos_reales,
        grupo=grupo,
        seccion="sec10"
    )

    print(f"\n=== LSR Nodo {nodo.nombre} ===")
    print(f"Nodo lógico: {nombre_nodo}")
    print(f"Usuario real: {usuario_real}")
    print(f"Vecinos reales: {vecinos_reales}")
    print("\nComandos disponibles:")
    print("  info                         - Enviar INFO para anunciar topología")
    print("  hello <destino_nodo> <mensaje> - Enviar mensaje hello")
    print("  message <destino_nodo> <mensaje> - Enviar mensaje (routing)")
    print("  echo <destino_nodo> <mensaje>  - Enviar echo")
    print("  tabla                       - Mostrar tabla de rutas")
    print("  lspdb                       - Mostrar base de datos LSP")
    print("  estadisticas                - Ver estadísticas del nodo")
    print("  vecinos                     - Ver vecinos configurados")
    print("  whoami                      - Mostrar mi identidad completa")
    print("  salir                       - Salir")
    print("\nPRIMERO ejecuta 'info' en cada nodo para intercambiar topología")
    print("EJEMPLO: message B Hola")
    print("EJEMPLO: hello C Hola vecino")
    print()

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

            if comando == "info":
                seq = await nodo.enviar_info_manual()
                print(f"INFO enviado con secuencia {seq}")

            elif comando.startswith("hello "):
                partes = comando.split(" ", 2)
                if len(partes) >= 3:
                    destino_nodo, mensaje = partes[1], partes[2]
                    # Traducir nombre de nodo a usuario real
                    if destino_nodo in mapeo_nombres:
                        destino_real = mapeo_nombres[destino_nodo]
                        await nodo.enviar_mensaje_raw('hello', destino_real, payload=mensaje)
                        print(f"HELLO enviado a {destino_nodo} ({destino_real}): {mensaje}")
                    else:
                        print(f"Error: Nodo {destino_nodo} no encontrado")
                else:
                    print("Uso: hello <destino_nodo> <mensaje>")
                    print("Ejemplo: hello B Hola vecino")

            elif comando.startswith("message "):
                partes = comando.split(" ", 2)
                if len(partes) >= 3:
                    destino_nodo, mensaje = partes[1], partes[2]
                    # Traducir nombre de nodo a usuario real
                    if destino_nodo in mapeo_nombres:
                        destino_real = mapeo_nombres[destino_nodo]
                        success = await nodo.enviar_mensaje_usuario(destino_real, mensaje)
                        if success:
                            print(f"Mensaje enviado correctamente a {destino_nodo}")
                    else:
                        print(f"Error: Nodo {destino_nodo} no encontrado")
                else:
                    print("Uso: message <destino_nodo> <mensaje>")
                    print("Ejemplo: message B Hola mundo")

            elif comando.startswith("echo "):
                partes = comando.split(" ", 2)
                if len(partes) >= 3:
                    destino_nodo, mensaje = partes[1], partes[2]
                    # Traducir nombre de nodo a usuario real
                    if destino_nodo in mapeo_nombres:
                        destino_real = mapeo_nombres[destino_nodo]
                        await nodo.enviar_mensaje_raw('echo', destino_real, payload=mensaje)
                        print(f"ECHO enviado a {destino_nodo} ({destino_real}): {mensaje}")
                    else:
                        print(f"Error: Nodo {destino_nodo} no encontrado")
                else:
                    print("Uso: echo <destino_nodo> <mensaje>")
                    print("Ejemplo: echo C Prueba echo")

            elif comando == "tabla":
                print("\n=== TABLA DE RUTAS ===")
                if nodo.routing_table:
                    print(f"{'Destino':<30} {'Siguiente':<30} {'Costo':<8}")
                    print("-" * 70)
                    for dest, (next_hop, cost) in sorted(nodo.routing_table.items()):
                        # Mostrar nombres simplificados
                        dest_simple = dest.split('@')[0] if '@' in dest else dest
                        next_simple = next_hop.split('@')[0] if '@' in next_hop else next_hop
                        print(f"{dest_simple:<30} {next_simple:<30} {cost:<8.1f}")
                else:
                    print("Tabla vacía - ejecuta 'info' primero")
                print("========================\n")

            elif comando == "lspdb":
                print("\n=== BASE DE DATOS LSP ===")
                if nodo.link_state_db:
                    for node, lsp_data in sorted(nodo.link_state_db.items()):
                        node_simple = node.split('@')[0] if '@' in node else node
                        print(f"Nodo {node_simple}:")
                        print(f"  Seq: {lsp_data.get('sequence', 'N/A')}")
                        neighbors = lsp_data.get('neighbors', {})
                        if neighbors:
                            print("  Vecinos:")
                            for neighbor, cost in neighbors.items():
                                neighbor_simple = neighbor.split('@')[0] if '@' in neighbor else neighbor
                                print(f"    {neighbor_simple}: {cost}")
                        print()
                else:
                    print("Base de datos vacía")
                print("==========================\n")

            elif comando == "vecinos":
                print(f"\nVecinos configurados:")
                for vecino in nodo.vecinos_reales:
                    vecino_simple = vecino.split('@')[0] if '@' in vecino else vecino
                    print(f"  {vecino_simple} ({vecino})")
                print(f"Costos: {nodo.neighbor_costs}\n")

            elif comando == "whoami":
                print(f"\nMi identidad:")
                print(f"  Nodo lógico: {nombre_nodo}")
                print(f"  Usuario real: {nodo.nombre}")
                print(f"  Grupo: grupo5")
                print(f"  Sección: sec10\n")

            elif comando == "estadisticas":
                print(f"\n=== ESTADÍSTICAS NODO {nombre_nodo} ===")
                print(f"Usuario real: {nodo.nombre}")
                print(f"INFO enviados: {nodo.lsp_sequence}")
                print(f"Nodos conocidos: {len(nodo.link_state_db)}")
                print(f"Rutas calculadas: {len(nodo.routing_table)}")
                print("=====================================\n")

            elif comando == "help" or comando == "ayuda":
                print("\nComandos disponibles:")
                print("  info                     - Anunciar mi topología")
                print("  hello <nodo> <mensaje>   - Enviar hello directo")
                print("  message <nodo> <mensaje> - Enviar mensaje con routing")
                print("  echo <nodo> <mensaje>    - Enviar echo")
                print("  tabla                    - Ver rutas calculadas")
                print("  lspdb                    - Ver base datos enlaces")
                print("  vecinos                  - Ver vecinos configurados")
                print("  whoami                   - Mostrar mi identidad")
                print("  estadisticas             - Ver estadísticas del nodo")
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
        await nodo.cerrar()
        print(f"[{nombre_nodo}] Cerrado.")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Uso: python test_lsr_interactivo.py <nombre_nodo> <archivo_topo> <archivo_nombres>")
        print("Ejemplos:")
        print("  python test_lsr_interactivo.py A topo2025-randomX-2025.txt names2025-randomX-2025.txt")
        print("  python test_lsr_interactivo.py B topo2025-randomX-2025.txt names2025-randomX-2025.txt")
        sys.exit(1)

    nombre_nodo = sys.argv[1]
    archivo_topo = sys.argv[2]
    archivo_nombres = sys.argv[3]

    print(f"Iniciando nodo: {nombre_nodo}")
    print(f"Archivo topología: {archivo_topo}")
    print(f"Archivo nombres: {archivo_nombres}")

    asyncio.run(main_lsr_manual(nombre_nodo, archivo_topo, archivo_nombres))