import asyncio
import json
import redis.asyncio as redis
import time
from typing import Dict, List, Set, Tuple
import heapq
from collections import deque


class LSRNode:
    def __init__(self, nombre: str, host: str, port: int, password: str,
                 vecinos: List[str], grupo: str = "grupo5", seccion: str = "sec10"):
        self.nombre = nombre
        self.vecinos = vecinos
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

        # Control de paquetes duplicados con expiración
        self.processed_packets = deque(maxlen=1000)  # Limita el tamaño
        self.packet_expiration = {}  # {packet_id: expiration_time}

        # Para limpieza periódica
        self.cleanup_task = None

    def get_canal_nombre(self, nodo: str) -> str:
        return f"{self.seccion}.{self.grupo}.{nodo}"

    async def suscribirse_a_canales(self):
        canales = [self.get_canal_nombre(vecino) for vecino in self.vecinos] + [self.get_canal_nombre(self.nombre)]
        await self.pubsub.subscribe(*canales)
        print(f"[LSR {self.nombre}] Suscrito a canales: {canales}")

    async def enviar_mensaje_raw(self, tipo: str, destino: str, payload: dict, hops: int = 0):
        # Verificar TTL antes de enviar
        if hops >= 10:  # TTL máximo
            print(f"[LSR {self.nombre}] TTL máximo alcanzado, no enviando a {destino}")
            return

        mensaje = {
            "type": tipo,
            "from": self.nombre,
            "to": destino,
            "hops": hops,
            "payload": payload
        }
        canal_destino = self.get_canal_nombre(destino)
        await self.redis.publish(canal_destino, json.dumps(mensaje))

    def is_packet_processed(self, packet_id: str) -> bool:
        """Verifica si el paquete ya fue procesado y limpia expirados"""
        current_time = time.time()

        # Limpiar expirados primero
        expired_ids = [pid for pid, exp_time in self.packet_expiration.items()
                       if exp_time < current_time]
        for pid in expired_ids:
            if pid in self.processed_packets:
                self.processed_packets.remove(pid)
            del self.packet_expiration[pid]

        return packet_id in self.processed_packets

    def mark_packet_processed(self, packet_id: str):
        """Marca paquete como procesado con expiración"""
        self.processed_packets.append(packet_id)
        self.packet_expiration[packet_id] = time.time() + 60  # Expira en 60 segundos

    async def enviar_lsp_manual(self):
        """Envía LSP manualmente cuando se solicita"""
        self.lsp_sequence += 1
        lsp_id = f"{self.nombre}_{self.lsp_sequence}"

        lsp_data = {
            "lsp_id": lsp_id,
            "origin": self.nombre,
            "sequence": self.lsp_sequence,
            "neighbors": self.neighbor_costs.copy(),
            "timestamp": time.time()
        }

        # Guardar en propia base de datos
        self.link_state_db[self.nombre] = lsp_data
        self.received_lsps.add(lsp_id)

        # Enviar a todos los vecinos
        for vecino in self.vecinos:
            await self.enviar_mensaje_raw("lsp", vecino, lsp_data)
            print(f"[LSR {self.nombre}] LSP enviado a {vecino}: seq={self.lsp_sequence}")

        # Recalcular rutas
        self.calcular_rutas()
        return lsp_data

    async def procesar_lsp(self, datos: dict):
        """Procesa un LSP recibido"""
        lsp_data = datos['payload']
        lsp_id = lsp_data['lsp_id']
        origin = lsp_data['origin']
        sequence = lsp_data['sequence']
        hops = datos.get('hops', 0)
        sender = datos['from']

        # Verificar TTL
        if hops >= 10:
            print(f"[LSR {self.nombre}] LSP descartado por TTL máximo: {hops}")
            return

        # Crear ID único para este paquete
        packet_id = f"lsp_{lsp_id}_{sender}_{hops}"

        # Verificar si ya procesamos este paquete
        if self.is_packet_processed(packet_id):
            print(f"[LSR {self.nombre}] Paquete LSP ya procesado, ignorando")
            return

        self.mark_packet_processed(packet_id)

        # Verificar si ya tenemos este LSP (por ID)
        if lsp_id in self.received_lsps:
            print(f"[LSR {self.nombre}] LSP {lsp_id} ya conocido, ignorando")
            return

        # Verificar versiones antiguas
        if origin in self.link_state_db:
            old_seq = self.link_state_db[origin].get('sequence', 0)
            if sequence <= old_seq:
                print(f"[LSR {self.nombre}] LSP con secuencia antigua ignorado: {sequence} <= {old_seq}")
                return

        print(f"[LSR {self.nombre}] LSP NUEVO de {origin}, seq={sequence}, hops={hops}")
        self.received_lsps.add(lsp_id)
        self.link_state_db[origin] = lsp_data

        # Reenviar a vecinos (excepto remitente) con TTL incrementado
        reenvios = 0
        for vecino in self.vecinos:
            if vecino != sender:
                await self.enviar_mensaje_raw("lsp", vecino, lsp_data, hops + 1)
                reenvios += 1

        if reenvios > 0:
            print(f"[LSR {self.nombre}] LSP reenviado a {reenvios} vecinos")

        # Recalcular rutas
        self.calcular_rutas()

    def calcular_rutas(self):
        """Calcula rutas usando Dijkstra"""
        if not self.link_state_db:
            return

        # Construir grafo
        graph = {}
        all_nodes = set()

        for node, lsp_data in self.link_state_db.items():
            neighbors = lsp_data.get('neighbors', {})
            graph[node] = neighbors
            all_nodes.add(node)
            all_nodes.update(neighbors.keys())

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

            for neighbor, cost in graph.get(current_node, {}).items():
                if neighbor in visited:
                    continue

                new_dist = current_dist + cost
                if new_dist < dist[neighbor]:
                    dist[neighbor] = new_dist
                    prev[neighbor] = current_node
                    heapq.heappush(pq, (new_dist, neighbor))

        # Construir tabla de rutas CORRECTAMENTE
        old_table = self.routing_table.copy()
        self.routing_table = {}

        for node in all_nodes:
            if node == self.nombre or dist[node] == float('inf'):
                continue

            # Encontrar el PRIMER salto real (no el siguiente nodo en el path)
            if prev[node] == self.nombre:
                # Si el predecesor directo soy yo, el siguiente salto es el destino
                next_hop = node
            else:
                # Seguir la cadena de predecesores hasta encontrar el primer salto
                temp = node
                while prev[temp] is not None and prev[temp] != self.nombre:
                    temp = prev[temp]
                next_hop = temp

            self.routing_table[node] = (next_hop, dist[node])

        # Solo mostrar cambios
        if old_table != self.routing_table:
            print(f"[LSR {self.nombre}] Rutas actualizadas: {len(self.routing_table)} destinos")
            print(f"[LSR {self.nombre}] Tabla: {self.routing_table}")

    async def enviar_mensaje_usuario(self, destino: str, mensaje: str):
        """Envía mensaje usando la tabla de rutas"""
        if destino == self.nombre:
            print(f"[LSR {self.nombre}] No puedes enviarte mensaje a ti mismo")
            return False

        if destino not in self.routing_table:
            print(f"[LSR {self.nombre}] No hay ruta para {destino}")
            print(f"[LSR {self.nombre}] Destinos disponibles: {list(self.routing_table.keys())}")
            return False

        next_hop, cost = self.routing_table[destino]
        print(f"[LSR {self.nombre}] Enviando a {destino} vía {next_hop}")

        # Crear ID único para este mensaje (más específico)
        message_id = f"msg_{self.nombre}_{destino}_{int(time.time() * 1000)}"

        packet = {
            "message_id": message_id,
            "original_source": self.nombre,
            "final_destination": destino,
            "content": mensaje,
            "hops": 0,
            "timestamp": time.time()
        }

        await self.enviar_mensaje_raw("data", next_hop, packet)
        return True

    async def procesar_mensaje_data(self, datos: dict):
        """Procesa mensajes de datos - CORREGIDO"""
        payload = datos['payload']
        final_dest = payload['final_destination']
        hops = datos.get('hops', 0)
        message_id = payload.get('message_id', 'unknown')
        sender = datos['from']
        original_source = payload.get('original_source', sender)

        # Verificar TTL
        if hops >= 10:
            print(f"[LSR {self.nombre}] Mensaje DATA descartado por TTL máximo: {hops}")
            return

        # Crear ID único para este paquete
        packet_id = f"data_{message_id}_{sender}_{hops}"

        # Verificar si ya procesamos este paquete
        if self.is_packet_processed(packet_id):
            print(f"[LSR {self.nombre}] Mensaje DATA ya procesado, ignorando")
            return

        self.mark_packet_processed(packet_id)

        # Si soy el destino final, mostrar mensaje
        if final_dest == self.nombre:
            print(f"\n*** MENSAJE RECIBIDO ***")
            print(f"De: {original_source}")
            print(f"Contenido: {payload['content']}")
            print(f"Hops: {hops}")
            print(f"************************\n")
            return

        # CORRECCIÓN: Solo reenviar si NO soy el origen del mensaje
        if original_source == self.nombre:
            print(f"[LSR {self.nombre}] Soy el origen, no reenvío mensaje propio")
            return

        # Verificar si tengo ruta al destino
        if final_dest not in self.routing_table:
            print(f"[LSR {self.nombre}] No puedo reenviar a {final_dest} - no hay ruta")
            return

        next_hop, cost = self.routing_table[final_dest]

        # CRÍTICO: No reenviar al que nos envió el mensaje
        if next_hop == sender:
            print(f"[LSR {self.nombre}] EVITANDO LOOP: No reenviando de vuelta a {sender}")
            return

        # Actualizar hops en el payload
        payload['hops'] = hops + 1

        print(f"[LSR {self.nombre}] Reenviando para {final_dest} vía {next_hop} (hops: {payload['hops']})")
        await self.enviar_mensaje_raw("data", next_hop, payload, payload['hops'])

    async def procesar_mensaje(self, datos: dict):
        """Procesa mensajes recibidos"""
        msg_type = datos.get('type', '')

        if msg_type == 'lsp':
            await self.procesar_lsp(datos)
        elif msg_type == 'data':
            await self.procesar_mensaje_data(datos)
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
        # Configurar costos de vecinos
        for vecino in self.vecinos:
            self.neighbor_costs[vecino] = 1.0

        # Iniciar tarea de limpieza
        async def cleanup_loop():
            while True:
                await asyncio.sleep(30)  # Limpiar cada 30 segundos
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

        print(f"[LSR {self.nombre}] Nodo inicializado con vecinos: {self.vecinos}")
        print(f"[LSR {self.nombre}] Usa 'lsp' para anunciar tu topología")

    async def cerrar(self):
        """Cierra el nodo limpiamente"""
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
        await self.redis.close()