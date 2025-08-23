import socket
import threading
import json
import time
from config import CONFIG, NODOS_PUERTOS, TOPOLOGIA


class Grafo:
    def __init__(self):
        self.matriz = []
        self.tamano = 0
        self.nodo_a_indice = {}
        self.indice_a_nodo = {}

    def inicializar(self, topologia):
        todos_nodos = sorted(set(topologia.keys()).union(*topologia.values()))
        self.tamano = len(todos_nodos)

        self.nodo_a_indice = {nodo: idx for idx, nodo in enumerate(todos_nodos)}
        self.indice_a_nodo = {idx: nodo for idx, nodo in enumerate(todos_nodos)}

        self.matriz = [[0] * self.tamano for _ in range(self.tamano)]

        for nodo, vecinos in topologia.items():
            idx_nodo = self.nodo_a_indice[nodo]
            for vecino in vecinos:
                idx_vecino = self.nodo_a_indice[vecino]
                self.matriz[idx_nodo][idx_vecino] = 1
                self.matriz[idx_vecino][idx_nodo] = 1

    def obtener_indice(self, nodo):
        return self.nodo_a_indice.get(nodo, -1)

    def obtener_nodo(self, indice):
        return self.indice_a_nodo.get(indice, "")


class Dijkstra:
    def __init__(self, grafo):
        self.grafo = grafo
        self.tabla = {}

    def calcular(self, origen):
        idx_origen = self.grafo.obtener_indice(origen)
        if idx_origen == -1:
            return {}

        n = self.grafo.tamano
        dist = [float('inf')] * n
        visitado = [False] * n
        previo = [None] * n
        dist[idx_origen] = 0

        for _ in range(n):
            min_dist = float('inf')
            u = None

            for i in range(n):
                if not visitado[i] and dist[i] < min_dist:
                    min_dist = dist[i]
                    u = i

            if u is None:
                break

            visitado[u] = True

            for v in range(n):
                peso = self.grafo.matriz[u][v]
                if peso > 0 and not visitado[v] and dist[u] != float('inf'):
                    nueva_dist = dist[u] + peso
                    if nueva_dist < dist[v]:
                        dist[v] = nueva_dist
                        previo[v] = u

        self._construir_tabla(origen, idx_origen, dist, previo)
        return self.tabla

    def _construir_tabla(self, origen, idx_origen, dist, previo):
        self.tabla = {}
        for i in range(self.grafo.tamano):
            if i == idx_origen:
                continue
            destino = self.grafo.obtener_nodo(i)
            costo = dist[i]
            salto = self._encontrar_salto(idx_origen, i, previo)
            self.tabla[destino] = {
                "salto": self.grafo.obtener_nodo(salto) if salto is not None else None,
                "costo": costo
            }

    def _encontrar_salto(self, origen_idx, destino_idx, previo):
        actual = destino_idx
        while previo[actual] is not None and previo[actual] != origen_idx:
            actual = previo[actual]
        return actual


class NodoRed:
    def __init__(self, nombre_nodo):
        self.nombre = nombre_nodo
        self.puerto = NODOS_PUERTOS[nombre_nodo]
        self.vecinos = TOPOLOGIA[nombre_nodo]
        self.conexiones = {}
        self.grafo = Grafo()
        self.grafo.inicializar(TOPOLOGIA)
        self.dijkstra = Dijkstra(self.grafo)
        self.dijkstra.calcular(nombre_nodo)

        self.socket_servidor = None
        self.ejecutando = False

        print(f"Nodo {self.nombre} inicializado en puerto {self.puerto}")
        print(f"   Vecinos: {', '.join(self.vecinos)}")

    def iniciar_servidor(self):
        try:
            self.socket_servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket_servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket_servidor.bind((CONFIG['host'], self.puerto))
            self.socket_servidor.listen(5)
            self.ejecutando = True

            print(f"👂 Nodo {self.nombre} escuchando en {CONFIG['host']}:{self.puerto}")

            # Hilo para aceptar conexiones
            hilo_aceptar = threading.Thread(target=self._aceptar_conexiones)
            hilo_aceptar.daemon = True
            hilo_aceptar.start()

            return True

        except Exception as e:
            print(f"Error iniciando servidor: {e}")
            return False

    def _aceptar_conexiones(self):
        while self.ejecutando:
            try:
                cliente_socket, direccion = self.socket_servidor.accept()
                print(f"Conexión aceptada de {direccion}")

                hilo_cliente = threading.Thread(
                    target=self._manejar_cliente,
                    args=(cliente_socket,)
                )
                hilo_cliente.daemon = True
                hilo_cliente.start()

            except Exception as e:
                if self.ejecutando:
                    print(f"Error aceptando conexión: {e}")

    def _manejar_cliente(self, cliente_socket):
        try:
            while self.ejecutando:
                datos = cliente_socket.recv(CONFIG['buffer_size'])
                if not datos:
                    break

                try:
                    mensaje = json.loads(datos.decode('utf-8'))
                    self._procesar_mensaje(mensaje, cliente_socket)
                except json.JSONDecodeError:
                    print("Mensaje no válido")

        except Exception as e:
            print(f"Error manejando cliente: {e}")
        finally:
            cliente_socket.close()

    def _procesar_mensaje(self, mensaje, socket_cliente):
        tipo = mensaje.get('tipo', '')

        if tipo == 'saludo':
            self._manejar_saludo(mensaje, socket_cliente)
        elif tipo == 'mensaje':
            self._manejar_mensaje(mensaje)
        elif tipo == 'consulta_ruta':
            self._manejar_consulta_ruta(mensaje, socket_cliente)
        else:
            print(f" Tipo de mensaje desconocido: {tipo}")

    def _manejar_saludo(self, mensaje, socket_cliente):
        nombre_remitente = mensaje.get('de', '')
        print(f" Saludo recibido de {nombre_remitente}")

        respuesta = {
            'tipo': 'saludo_respuesta',
            'de': self.nombre,
            'para': nombre_remitente
        }
        try:
            socket_cliente.send(json.dumps(respuesta).encode('utf-8'))
        except:
            pass

    def _manejar_mensaje(self, mensaje):
        destino = mensaje.get('para', '')
        contenido = mensaje.get('contenido', '')
        remitente = mensaje.get('de', 'desconocido')

        if destino == self.nombre:
            print(f"\n MENSAJE RECIBIDO de {remitente}: {contenido}")
        else:
            print(f" Reenviando mensaje para {destino}")
            self._reenviar_mensaje(mensaje)

    def _manejar_consulta_ruta(self, mensaje, socket_cliente):
        respuesta = {
            'tipo': 'ruta_respuesta',
            'de': self.nombre,
            'para': mensaje.get('de', ''),
            'tabla_rutas': self.dijkstra.tabla
        }
        try:
            socket_cliente.send(json.dumps(respuesta).encode('utf-8'))
        except:
            pass

    def conectar_vecino(self, nombre_vecino, puerto_vecino, intentos=3):
        for intento in range(intentos):
            try:
                socket_vecino = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                socket_vecino.settimeout(2.0)
                socket_vecino.connect((CONFIG['host'], puerto_vecino))

                saludo = {
                    'tipo': 'saludo',
                    'de': self.nombre,
                    'para': nombre_vecino
                }
                socket_vecino.send(json.dumps(saludo).encode('utf-8'))

                self.conexiones[nombre_vecino] = socket_vecino

                hilo_escucha = threading.Thread(
                    target=self._escuchar_vecino,
                    args=(socket_vecino, nombre_vecino)
                )
                hilo_escucha.daemon = True
                hilo_escucha.start()

                print(f" Conectado a vecino {nombre_vecino}")
                return True

            except Exception as e:
                if intento < intentos - 1:
                    print(f" Intentando conectar a {nombre_vecino}... ({intento + 1}/{intentos})")
                    time.sleep(1)
                else:
                    print(f" Error conectando a {nombre_vecino}: {e}")
        return False

    def conectar_vecinos(self):
        print(" Conectando a vecinos...")
        for vecino in self.vecinos:
            puerto_vecino = NODOS_PUERTOS[vecino]
            self.conectar_vecino(vecino, puerto_vecino)
            time.sleep(0.5)

    def _escuchar_vecino(self, socket_vecino, nombre_vecino):
        try:
            while self.ejecutando:
                try:
                    datos = socket_vecino.recv(CONFIG['buffer_size'])
                    if not datos:
                        break

                    try:
                        mensaje = json.loads(datos.decode('utf-8'))
                        self._procesar_mensaje(mensaje, socket_vecino)
                    except json.JSONDecodeError:
                        print(f" Mensaje no válido de {nombre_vecino}")

                except socket.timeout:
                    continue
                except Exception as e:
                    break

        except Exception as e:
            print(f" Error escuchando a {nombre_vecino}: {e}")
        finally:
            if nombre_vecino in self.conexiones:
                try:
                    self.conexiones[nombre_vecino].close()
                except:
                    pass
                del self.conexiones[nombre_vecino]
                print(f" Desconectado de {nombre_vecino}")

    def enviar_mensaje(self, destino, mensaje):
        if destino == self.nombre:
            print(" No puedes enviarte un mensaje a ti mismo")
            return False

        if destino not in self.dijkstra.tabla:
            print(f" No hay ruta conocida para {destino}")
            return False

        siguiente_salto = self.dijkstra.tabla[destino]["salto"]
        if not siguiente_salto:
            print(f" No se puede alcanzar a {destino}")
            return False

        if siguiente_salto not in self.conexiones:
            print(f" No hay conexión con el siguiente salto {siguiente_salto}")
            print(f"   Conexiones activas: {list(self.conexiones.keys())}")
            return False

        mensaje_json = {
            'tipo': 'mensaje',
            'de': self.nombre,
            'para': destino,
            'contenido': mensaje
        }

        try:
            self.conexiones[siguiente_salto].send(
                json.dumps(mensaje_json).encode('utf-8')
            )
            print(f" Mensaje enviado a {destino} través de {siguiente_salto}")
            return True
        except Exception as e:
            print(f" Error enviando mensaje: {e}")

            if siguiente_salto in self.conexiones:
                del self.conexiones[siguiente_salto]
            return False

    def _reenviar_mensaje(self, mensaje):
        destino = mensaje.get('para', '')

        if destino not in self.dijkstra.tabla:
            print(f" No hay ruta para reenviar a {destino}")
            return False

        siguiente_salto = self.dijkstra.tabla[destino]["salto"]
        if not siguiente_salto or siguiente_salto not in self.conexiones:
            print(f" No se puede reenviar a {destino}")
            return False

        try:
            self.conexiones[siguiente_salto].send(
                json.dumps(mensaje).encode('utf-8')
            )
            print(f" Mensaje reenviado a {destino} través de {siguiente_salto}")
            return True
        except Exception as e:
            print(f" Error reenviando mensaje: {e}")
            return False

    def mostrar_tabla_rutas(self):
        print(f"\n TABLA DE RUTAS - NODO {self.nombre}")
        print("-" * 45)
        print(f"{'DESTINO':<10} {'SIGUIENTE SALTO':<15} {'COSTO':<8}")
        print("--" * 45)

        for destino, info in sorted(self.dijkstra.tabla.items()):
            salto = info["salto"] if info["salto"] else "✗ N/A"
            costo = str(info["costo"]) if info["costo"] != float('inf') else "∞"
            print(f"{destino:<10} {salto:<15} {costo:<8}")

    def mostrar_conexiones(self):
        print(f"\n CONEXIONES ACTIVAS - NODO {self.nombre}")
        print("-" * 30)
        print(f"Vecinos: {', '.join(self.vecinos)}")
        print(f"Conectados: {', '.join(self.conexiones.keys())}")

    def verificar_conexion(self, vecino):
        if vecino in self.conexiones:
            try:
                ping = {'tipo': 'saludo', 'de': self.nombre, 'para': vecino}
                self.conexiones[vecino].send(json.dumps(ping).encode('utf-8'))
                return True
            except:
                del self.conexiones[vecino]
                return False
        return False

    def detener(self):
        self.ejecutando = False
        if self.socket_servidor:
            try:
                self.socket_servidor.close()
            except:
                pass
        for nombre, conexion in self.conexiones.items():
            try:
                conexion.close()
            except:
                pass