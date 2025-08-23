import sys
import time
from nodo import NodoRed


def main():
    if len(sys.argv) != 2:
        #print("Uso: python iniciar_nodo.py <nombre_nodo>")
        print("Nodos disponibles: A, B, C, D, E, F, G, H, I")
        sys.exit(1)

    nombre_nodo = sys.argv[1].upper()

    nodos_validos = ["A", "B", "C", "D", "E", "F", "G", "H", "I"]
    if nombre_nodo not in nodos_validos:
        print("Nodo no válido. Solo se pueden usar: A, B, C, D, E, F, G, H, I")
        sys.exit(1)

    try:
        nodo = NodoRed(nombre_nodo)

        if not nodo.iniciar_servidor():
            print("No se pudo iniciar")
            sys.exit(1)

        print("Esperando...")
        time.sleep(2)

        print("Conectando a vecinos...")
        nodo.conectar_vecinos()

        print("Esperando...")
        time.sleep(3)

        nodo.mostrar_conexiones()

        print(f"\nNodo {nombre_nodo} listo")
        print("\n Comandos disponibles:")
        print("  enviar <destino> <mensaje> - Enviar mensaje")
        print("  tabla - Mostrar tabla de rutas")
        print("  conexiones - Mostrar conexiones activas")
        print("  reconectar - Reconectar con vecinos")
        print("  salir - Terminar nodo")

        while True:
            try:
                comando = input(f"\n{nombre_nodo}> ").strip()

                if comando.startswith("enviar "):
                    partes = comando.split(" ", 2)
                    if len(partes) >= 3:
                        destino = partes[1].upper()
                        mensaje = partes[2]
                        if not nodo.enviar_mensaje(destino, mensaje):
                            print("Usa 'reconectar'")
                    else:
                        print("Uso adecuado: enviar <destino> <mensaje>")

                elif comando == "tabla":
                    nodo.mostrar_tabla_rutas()

                elif comando == "conexiones":
                    nodo.mostrar_conexiones()

                elif comando == "reconectar":
                    print("Reconectando con vecinos...")
                    for vecino in list(nodo.conexiones.keys()):
                        try:
                            nodo.conexiones[vecino].close()
                        except:
                            pass
                    nodo.conexiones.clear()
                    nodo.conectar_vecinos()
                    time.sleep(2)
                    nodo.mostrar_conexiones()

                elif comando == "salir":
                    break

                elif comando == "":
                    continue

                else:
                    print("Comando incorrecto")
                    print("Comandos disponibles: enviar, tabla, conexiones, reconectar, salir")

            except KeyboardInterrupt:
                print("\n Interrupción...")
                break
            except Exception as e:
                print(f"Error: {e}")

    except Exception as e:
        print(f"Error iniciando nodo: {e}")
    finally:
        if 'nodo' in locals():
            nodo.detener()
        print(f"👋 Nodo {nombre_nodo} terminado")


if __name__ == "__main__":
    main()