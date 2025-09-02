import asyncio
import json
import redis.asyncio as redis
import time
from typing import Dict, List, Set, Tuple
import heapq
from collections import deque


class LSRNode:
    def __init__(self, usuario_real: str, host: str, port: int, password: str,
                 vecinos_reales: List[str], grupo: str = "grupo5", seccion: str = "sec10"):
        # Usar el usuario REAL del servidor
        self.nombre = usuario_real
        self.usuario_real = usuario_real
        self.vecinos_reales = vecinos_reales  # Lista de usuarios REALES de vecinos
        self.grupo = grupo
        self.seccion = seccion
        self.redis = redis.Redis(host=host, port=port, password=password, decode_responses=True)
        self.pubsub = self.redis.pubsub()

        # Estructuras para LSR
        self.lsp_sequence = 0
        self.link_state_db: Dict[str, Dict] = {}
        self.neighbor_costs: Dict[str, float] = {}
        self.routing_table: Dict[str, Tuple[str, float]] = {}
        self.received_lsps: Set[str] = set()

        # Control de paquetes duplicados
        self.processed_packets = deque(maxlen=100)
        self.packet_expiration = {}
        self.cleanup_task = None

    async def suscribirse_a_canales(self):
        """Se suscribe a su propio canal y al de sus vecinos (usuarios REALES)"""
        canales = [self.nombre] + self.vecinos_reales
        await self.pubsub.subscribe(*canales)
        print(f"[LSR {self.nombre}] Suscrito a canales: {canales}")

    async def enviar_mensaje_raw(self, tipo: str, destino: str, payload=None, hops: int = 0,
                                 seq_num: int = 0, neighbors: dict = None):
        """Envía mensaje siguiendo el protocolo exacto especificado"""
        if hops >= 10:  # TTL máximo
            print(f"[LSR {self.nombre}] TTL máximo alcanzado, no enviando a {destino}")
            return

        # Construir mensaje según protocolo exacto
        mensaje = {
            'type': tipo,
            'from': self.nombre,
            'to': destino,
            'hops': hops,
            'headers': {'alg': 'lsr'}
        }

        # Agregar campos específicos según el tipo
        if tipo == 'info':
            mensaje['seq_num'] = seq_num
            mensaje['neighbors'] = neighbors or {}
        elif tipo in ['message', 'hello', 'echo']:
            mensaje['payload'] = payload or ''

        await self.redis.publish(destino, json.dumps(mensaje))

    def is_packet_processed(self, packet_id: str) -> bool:
        """Verifica si el paquete ya fue procesado"""
        # Limpiar expirados primero
        current_time = time.time()
        expired_ids = [pid for pid, exp_time in self.packet_expiration.items()
                       if exp_time < current_time]
        for pid in expired_ids:
            if pid in self.processed_packets:
                self.processed_packets.remove(pid)
            del self.packet_expiration[pid]

        # Verificación simple - si el ID exacto existe
        return packet_id in self.processed_packets

    def mark_packet_processed(self, packet_id: str):
        """Marca paquete como procesado con expiración"""
        self.processed_packets.append(packet_id)
        self.packet_expiration[packet_id] = time.time() + 60

    async def enviar_info_manual(self):
        """Envía INFO siguiendo el protocolo exacto"""
        self.lsp_sequence += 1

        # Crear LSP data
        lsp_data = {
            "lsp_id": f"{self.nombre}_{self.lsp_sequence}",
            "origin": self.nombre,
            "sequence": self.lsp_sequence,
            "timestamp": time.time(),
            "neighbors": self.neighbor_costs.copy()
        }

        # Guardar en propia base de datos
        self.link_state_db[self.nombre] = lsp_data
        self.received_lsps.add(lsp_data["lsp_id"])

        # Enviar INFO a todos los vecinos REALES con protocolo exacto
        for vecino_real in self.vecinos_reales:
            await self.enviar_mensaje_raw(
                'info',
                vecino_real,
                payload=None,
                seq_num=self.lsp_sequence,
                neighbors=self.neighbor_costs.copy()
            )
            print(f"[LSR {self.nombre}] INFO enviado a {vecino_real}: seq={self.lsp_sequence}")

        # Recalcular rutas
        self.calcular_rutas()
        return self.lsp_sequence

    async def procesar_info(self, datos: dict):
        """Procesa mensajes INFO recibidos según el protocolo exacto"""
        seq_num = datos.get('seq_num', 0)
        neighbors = datos.get('neighbors', {})
        hops = datos.get('hops', 0)
        sender = datos['from']
        origin = sender  # En INFO, el origin es el sender

        # Verificar TTL
        if hops >= 10:
            print(f"[LSR {self.nombre}] INFO descartado por TTL máximo: {hops}")
            return

        # Crear LSP ID único (origen + secuencia)
        lsp_id = f"{origin}_{seq_num}"

        # Verificar si ya tenemos este LSP
        if lsp_id in self.received_lsps:
            print(f"[LSR {self.nombre}] INFO {lsp_id} ya conocido, ignorando")
            return

        # Crear ID único para este paquete (origen + secuencia + remitente)
        packet_id = f"info_{origin}_{seq_num}_{sender}"

        # Verificar si ya procesamos este paquete específico
        if self.is_packet_processed(packet_id):
            print(f"[LSR {self.nombre}] Paquete INFO ya procesado, ignorando")
            return

        self.mark_packet_processed(packet_id)
        self.received_lsps.add(lsp_id)

        print(f"[LSR {self.nombre}] INFO NUEVO de {origin}, seq={seq_num}, hops={hops}")
        print(f"[LSR {self.nombre}] Vecinos de {origin}: {neighbors}")

        # Guardar información LSP
        lsp_data = {
            "lsp_id": lsp_id,
            "origin": origin,
            "sequence": seq_num,
            "neighbors": neighbors,
            "timestamp": time.time()
        }
        self.link_state_db[origin] = lsp_data

        # Reenviar a vecinos REALES (excepto remitente) con TTL incrementado
        reenvios = 0
        for vecino_real in self.vecinos_reales:
            if vecino_real != sender:
                await self.enviar_mensaje_raw(
                    'info',
                    vecino_real,
                    payload=None,
                    hops=hops + 1,
                    seq_num=seq_num,
                    neighbors=neighbors
                )
                reenvios += 1

        if reenvios > 0:
            print(f"[LSR {self.nombre}] INFO reenviado a {reenvios} vecinos")

        # Recalcular rutas
        self.calcular_rutas()

    def calcular_rutas(self):
        """Calcula rutas usando Dijkstra"""
        if not self.link_state_db:
            # Si no hay información de topología, los vecinos directos son alcanzables
            print(f"[LSR {self.nombre}] Sin LSP DB, usando solo vecinos directos")
            self.routing_table = {}
            for vecino_real in self.vecinos_reales:
                self.routing_table[vecino_real] = (vecino_real, self.neighbor_costs.get(vecino_real, 1.0))
            return

        # Construir grafo
        graph = {}
        all_nodes = set()

        # Incluir mi propia información
        if self.nombre not in self.link_state_db:
            self.link_state_db[self.nombre] = {
                'neighbors': self.neighbor_costs.copy(),
                'sequence': self.lsp_sequence
            }

        for node, lsp_data in self.link_state_db.items():
            neighbors = lsp_data.get('neighbors', {})
            graph[node] = neighbors
            all_nodes.add(node)
            all_nodes.update(neighbors.keys())

        # Asegurar que mis vecinos estén en all_nodes
        all_nodes.update(self.vecinos_reales)

        print(f"[LSR {self.nombre}] Calculando rutas con {len(all_nodes)} nodos")

        if self.nombre not in all_nodes:
            return

        # Dijkstra
        dist = {node: float('inf') for node in all_nodes}
        prev = {node: None for node in all_nodes}
        visited = set()
        dist[self.nombre] = 0

        pq = [(0, self.nombre)]

        while pq:
            current_dist, current_node = heapq.heappop(pq)

            if current_node in visited:
                continue

            visited.add(current_node)

            # Usar vecinos del grafo, o vecinos directos si no están en el grafo
            neighbors_with_costs = graph.get(current_node, {})

            # Si soy yo y no tengo info en el grafo, usar mis vecinos directos
            if current_node == self.nombre and not neighbors_with_costs:
                neighbors_with_costs = self.neighbor_costs

            for neighbor, cost in neighbors_with_costs.items():
                if neighbor in visited:
                    continue

                new_dist = current_dist + cost
                if new_dist < dist[neighbor]:
                    dist[neighbor] = new_dist
                    prev[neighbor] = current_node
                    heapq.heappush(pq, (new_dist, neighbor))

        # Construir tabla de rutas
        old_table = self.routing_table.copy()
        self.routing_table = {}

        for node in all_nodes:
            if node == self.nombre or dist[node] == float('inf'):
                continue

            next_hop = None

            if node in self.vecinos_reales:
                # Si el destino es vecino directo
                next_hop = node
            else:
                # Reconstruir path y tomar el primer vecino
                path = []
                temp = node
                while temp is not None:
                    path.append(temp)
                    temp = prev[temp]
                path.reverse()

                if len(path) > 1 and path[1] in self.vecinos_reales:
                    next_hop = path[1]

            if next_hop and next_hop in self.vecinos_reales:
                self.routing_table[node] = (next_hop, dist[node])

        # Solo mostrar cambios
        if old_table != self.routing_table:
            print(f"[LSR {self.nombre}] Rutas actualizadas: {len(self.routing_table)} destinos")
            for dest, (nh, cost) in self.routing_table.items():
                print(f"[LSR {self.nombre}]   {dest} -> vía {nh} (costo {cost})")

    async def enviar_mensaje_usuario(self, destino_real: str, mensaje: str):
        """Envía mensaje de usuario siguiendo el protocolo exacto"""
        if destino_real == self.nombre:
            print(f"[LSR {self.nombre}] No puedes enviarte mensaje a ti mismo")
            return False

        if destino_real not in self.routing_table:
            print(f"[LSR {self.nombre}] No hay ruta para {destino_real}")
            print(f"[LSR {self.nombre}] Destinos disponibles: {list(self.routing_table.keys())}")
            return False

        next_hop, cost = self.routing_table[destino_real]

        if next_hop not in self.vecinos_reales:
            print(f"[LSR {self.nombre}] ERROR: {next_hop} no es vecino directo")
            return False

        print(f"[LSR {self.nombre}] Enviando mensaje a {destino_real} vía {next_hop} (costo: {cost})")

        # Protocolo EXACTO como especificado
        mensaje_data = {
            'type': 'message',
            'from': self.nombre,
            'to': destino_real,  # Destino FINAL (protocolo original)
            'hops': 0,
            'headers': {'alg': 'lsr'},
            'payload': mensaje
        }

        await self.redis.publish(next_hop, json.dumps(mensaje_data))
        print(f"[LSR {self.nombre}] Mensaje publicado en canal {next_hop}")
        return True

    async def procesar_mensaje_data(self, datos: dict):
        """Procesa mensajes de tipo 'message' según protocolo exacto"""
        payload = datos.get('payload', '')
        final_dest = datos['to']  # Destino final
        hops = datos.get('hops', 0)
        sender = datos['from']
        original_source = datos['from']

        # Crear ID único para este paquete (SIMPLE)
        packet_id = f"message_{original_source}_{final_dest}"

        # Verificar TTL
        if hops >= 10:
            print(f"[LSR {self.nombre}] Mensaje descartado por TTL máximo: {hops}")
            return

        # Verificar si ya procesamos este paquete
        if self.is_packet_processed(packet_id):
            print(f"[LSR {self.nombre}] Mensaje ya procesado, ignorando")
            return

        self.mark_packet_processed(packet_id)

        # PRIMERO: Verificar si soy el destino final
        if final_dest == self.nombre:
            print(f"\n*** MENSAJE RECIBIDO ***")
            print(f"De: {original_source}")
            print(f"Para: {final_dest}")
            print(f"Contenido: {payload}")
            print(f"Hops: {hops}")
            print(f"************************\n")
            return  # ¡IMPORTANTE! Salir aquí

        # Si no soy el destino, reenviar SOLO si tengo una ruta válida
        if final_dest not in self.routing_table:
            print(f"[LSR {self.nombre}] No puedo reenviar a {final_dest} - no hay ruta")
            return

        next_hop, cost = self.routing_table[final_dest]

        # No reenviar al que nos envió (evitar loops)
        if next_hop == sender:
            print(f"[LSR {self.nombre}] EVITANDO LOOP: No reenviando de vuelta a {sender}")
            return

        print(f"[LSR {self.nombre}] Reenviando mensaje para {final_dest} vía {next_hop} (hops: {hops + 1})")

        # Crear NUEVO mensaje (no modificar el original)
        nuevo_mensaje = {
            'type': 'message',
            'from': original_source,  # Mantener el origen original
            'to': final_dest,  # Mantener destino final (protocolo original)
            'hops': hops + 1,  # Incrementar hops
            'headers': {'alg': 'lsr'},
            'payload': payload
        }

        await self.redis.publish(next_hop, json.dumps(nuevo_mensaje))

    async def procesar_hello_echo(self, datos: dict, tipo: str):
        """Procesa mensajes hello y echo"""
        payload = datos.get('payload', '')
        sender = datos['from']
        destino_directo = datos['to']  # Destino directo

        # Solo procesar si soy el destino directo
        if destino_directo == self.nombre:
            print(f"\n*** {tipo.upper()} RECIBIDO ***")
            print(f"De: {sender}")
            print(f"Mensaje: {payload}")
            print(f"{'*' * (len(tipo) + 20)}\n")
        else:
            print(f"[LSR {self.nombre}] {tipo.upper()} no es para mí (soy {self.nombre}, destino {destino_directo})")

    async def procesar_mensaje(self, datos: dict):
        """Procesa mensajes recibidos según el tipo exacto"""
        msg_type = datos.get('type', '')
        sender = datos.get('from', 'unknown')

        if msg_type == 'info':
            await self.procesar_info(datos)
        elif msg_type == 'message':
            await self.procesar_mensaje_data(datos)
        elif msg_type == 'hello':
            await self.procesar_hello_echo(datos, 'hello')
        elif msg_type == 'echo':
            await self.procesar_hello_echo(datos, 'echo')
        else:
            print(f"[LSR {self.nombre}] Tipo de mensaje desconocido: {msg_type}")

    async def escuchar_mensajes(self):
        """Escucha mensajes de Redis"""
        async for mensaje in self.pubsub.listen():
            if mensaje['type'] == 'message':
                try:
                    datos = json.loads(mensaje['data'])
                    await self.procesar_mensaje(datos)
                except Exception as e:
                    print(f"[LSR {self.nombre}] Error procesando mensaje: {e}")

    async def inicializar(self):
        """Inicializa el nodo"""
        # Configurar costos de vecinos (todos con costo 1 por defecto)
        for vecino_real in self.vecinos_reales:
            self.neighbor_costs[vecino_real] = 1.0

        # Iniciar tarea de limpieza
        async def cleanup_loop():
            while True:
                await asyncio.sleep(30)
                current_time = time.time()
                expired = [pid for pid, exp_time in self.packet_expiration.items()
                           if exp_time < current_time]
                for pid in expired:
                    if pid in self.processed_packets:
                        self.processed_packets.remove(pid)
                    del self.packet_expiration[pid]
                if expired:
                    print(f"[LSR {self.nombre}] Limpiados {len(expired)} paquetes expirados")

        self.cleanup_task = asyncio.create_task(cleanup_loop())
        print(f"[LSR {self.nombre}] Nodo inicializado con vecinos: {self.vecinos_reales}")
        print(f"[LSR {self.nombre}] Usa 'info' para anunciar tu topología")

    async def cerrar(self):
        """Cierra el nodo limpiamente"""
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
        await self.redis.close()