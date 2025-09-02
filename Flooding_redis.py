import asyncio
import json
import redis.asyncio as redis
import time
from typing import Dict, List, Set


class NodoFlooding:
    def __init__(self, nombre: str, host: str, port: int, password: str, vecinos: List[str],
                 nombres_config: dict, grupo: str = "grupo5", seccion: str = "sec10"):
        self.nombre = nombre
        self.vecinos = vecinos
        self.grupo = grupo
        self.seccion = seccion
        self.nombres_config = nombres_config  # Configuración de nombres
        self.redis = redis.Redis(host=host, port=port, password=password, decode_responses=True)
        self.pubsub = self.redis.pubsub()
        # Para evitar procesar mensajes duplicados (usamos ID de paquete)
        self.paquetes_procesados: Set[str] = set()
        self.seq_num_counter = 0  # Para mensajes de info

    def get_canal_nombre(self, nodo: str) -> str:
        if nodo in self.nombres_config:
            return self.nombres_config[nodo]
        else:
            print(f"[{self.nombre}] Nodo {nodo} no encontrado en configuración de nombres")
            return f"{self.seccion}.{self.grupo}.{nodo}"

    def get_nombre_completo(self) -> str:
        if self.nombre in self.nombres_config:
            return self.nombres_config[self.nombre]
        else:
            return f"{self.seccion}.{self.grupo}.{self.nombre}"

    async def suscribirse_a_canales(self):
        mi_canal = self.get_nombre_completo()
        await self.pubsub.subscribe(mi_canal)
        print(f"[{self.nombre}] Suscrito a mi canal: {mi_canal}")

        await asyncio.sleep(0.1)

    async def enviar_mensaje(self, tipo: str, destino: str, payload: str = "", headers: dict = None,
                             hops: int = 0, seq_num: int = None, neighbors: dict = None):

        if destino != "broadcast":
            destino_completo = self.get_canal_nombre(destino)
        else:
            destino_completo = "broadcast"

        mensaje = {
            "type": tipo,
            "from": self.get_nombre_completo(),
            "to": destino_completo,
            "hops": hops,
            "headers": headers or {"alg": "flooding"},
            "payload": payload,
            "msg_id": f"{self.nombre}_{int(time.time() * 1000)}_{hash(payload)}_{tipo}_{destino}"
        }

        if tipo == "info":
            if seq_num is None:
                self.seq_num_counter += 1
                seq_num = self.seq_num_counter
            mensaje["seq_num"] = seq_num

            if neighbors is None:
                neighbors = {self.get_canal_nombre(vecino): 1 for vecino in self.vecinos}
            mensaje["neighbors"] = neighbors

        print(f"[{self.nombre}] DEBUG: Enviando {tipo} a {destino_completo}: {mensaje}")

        if destino == "broadcast":
            # Enviar a todos los vecinos
            for vecino in self.vecinos:
                canal_destino = self.get_canal_nombre(vecino)
                await self.redis.publish(canal_destino, json.dumps(mensaje))
                print(f"[{self.nombre}] Broadcast → {vecino} ({canal_destino})")
        else:
            for vecino in self.vecinos:
                canal_vecino = self.get_canal_nombre(vecino)
                await self.redis.publish(canal_vecino, json.dumps(mensaje))
                print(f"[{self.nombre}] Enviado a vecino {vecino} ({canal_vecino}) para flooding hacia {destino}")

    async def flooding_reenviar(self, datos: dict):
        ttl = datos.get('hops', 0) + 1
        if ttl > 5:  # TTL máximo
            print(f"[{self.nombre}] Mensaje descartado por TTL máximo: {ttl}")
            return

        if datos.get('type') == 'hello' and datos.get('payload', '').startswith('Hello back from'):
            print(f"[{self.nombre}] No reenviando respuesta automática de hello")
            return

        mensaje_reenvio = {
            "type": datos['type'],
            "from": datos['from'],
            "to": datos['to'],
            "hops": ttl,
            "headers": datos.get('headers', {}),
            "payload": datos['payload'],
            "msg_id": datos.get('msg_id')
        }

        if 'seq_num' in datos:
            mensaje_reenvio['seq_num'] = datos['seq_num']
        if 'neighbors' in datos:
            mensaje_reenvio['neighbors'] = datos['neighbors']

        nodo_origen = datos['from'].split('.')[-1]

        reenviado_count = 0
        for vecino in self.vecinos:
            if vecino != nodo_origen:
                canal_vecino = self.get_canal_nombre(vecino)
                await self.redis.publish(canal_vecino, json.dumps(mensaje_reenvio))
                reenviado_count += 1

        if reenviado_count > 0:
            print(f"[{self.nombre}] Reenviado a {reenviado_count} vecinos")
        else:
            print(f"[{self.nombre}] No se reenviado")

    async def escuchar_mensajes(self):
        print(f"[{self.nombre}] Iniciando escucha de mensajes...")
        async for mensaje in self.pubsub.listen():
            if mensaje['type'] == 'message':
                try:
                    datos = json.loads(mensaje['data'])
                    print(f"[{self.nombre}] DEBUG: Mensaje recibido: {datos['type']} de {datos['from'].split('.')[-1]}")
                    await self.procesar_mensaje(datos, mensaje['channel'])
                except json.JSONDecodeError as e:
                    print(f"[{self.nombre}] Error JSON: {e} - Data: {mensaje['data']}")
                except Exception as e:
                    print(f"[{self.nombre}] Error procesando: {e}")

    async def procesar_mensaje(self, datos: dict, canal_origen: str = None):
        if 'msg_id' not in datos:
            payload_str = str(datos.get('payload', ''))
            timestamp = int(time.time() * 1000)
            datos['msg_id'] = f"{datos['from']}_{timestamp}_{hash(payload_str)}_{datos['type']}"

        paquete_id = datos['msg_id']

        if paquete_id in self.paquetes_procesados:
            return


        self.paquetes_procesados.add(paquete_id)

        tipo = datos.get('type', 'unknown')
        nodo_origen = datos['from'].split('.')[-1]

        destino = datos['to']
        mi_nombre_completo = self.get_nombre_completo()

        print(
            f"[{self.nombre}] DEBUG: Procesando {tipo} de {nodo_origen}, destino: {destino}, yo soy: {mi_nombre_completo}")

        if destino == mi_nombre_completo or destino.endswith(f".{self.nombre}"):
            print(f"[{self.nombre}] → Mensaje para mí: {tipo} de {nodo_origen}")
            await self.manejar_mensaje_destino(datos)
            return

        if destino != "broadcast":
            print(f"[{self.nombre}] → Reenviando {tipo} de {nodo_origen} hacia {destino.split('.')[-1]}")
            await self.flooding_reenviar(datos)
        else:
            print(f"[{self.nombre}] → Broadcast {tipo} de {nodo_origen}")
            await self.manejar_mensaje_destino(datos)

    async def manejar_mensaje_destino(self, datos: dict):
        tipo = datos.get('type', 'unknown')
        nodo_origen = datos['from'].split('.')[-1]
        payload = datos.get('payload', '')
        headers = datos.get('headers', {})

        if tipo == "hello":
            if headers.get('response'):
                print(f"[{self.nombre}] ¡RESPUESTA HELLO! De: {nodo_origen} → '{payload}'")
            else:
                print(f"[{self.nombre}] ¡HELLO! De: {nodo_origen} → '{payload}'")
                await self.responder_hello(nodo_origen)

        elif tipo == "message":
            print(f"[{self.nombre}] ¡MENSAJE! De: {nodo_origen} → '{payload}'")

        elif tipo == "info":
            seq_num = datos.get('seq_num', 0)
            neighbors = datos.get('neighbors', {})
            print(f"[{self.nombre}] ¡INFO! De: {nodo_origen} → Seq:{seq_num}, Vecinos:{len(neighbors)}")

        elif tipo == "echo":
            if headers.get('response'):
                print(f"[{self.nombre}] ¡Echo respuesta! De: {nodo_origen} → '{payload}' ✓")
            else:
                print(f"[{self.nombre}] ¡Echo recibido! De: {nodo_origen} → '{payload}'")
                # Responder automáticamente con el mismo payload
                await self.responder_echo(nodo_origen, payload)

        else:
            print(f"[{self.nombre}] Mensaje desconocido '{tipo}' de {nodo_origen}")

    async def responder_echo(self, nodo_origen: str, payload_original: str):
        mensaje = {
            "type": "echo",
            "from": self.get_nombre_completo(),
            "to": self.get_canal_nombre(nodo_origen),
            "hops": 0,
            "headers": {"alg": "direct", "response": True},
            "payload": payload_original,
            "msg_id": f"{self.nombre}_{int(time.time() * 1000)}_echo_response"
        }

        canal_destino = self.get_canal_nombre(nodo_origen)
        await self.redis.publish(canal_destino, json.dumps(mensaje))
        print(f"[{self.nombre}] Echo respondido a {nodo_origen}: '{payload_original}'")

    async def responder_hello(self, nodo_origen: str):
        mensaje = {
            "type": "hello",
            "from": self.get_nombre_completo(),
            "to": self.get_canal_nombre(nodo_origen),
            "hops": 0,
            "headers": {"alg": "direct", "response": True},
            "payload": f"Hello back from {self.nombre}!",
            "msg_id": f"{self.nombre}_{int(time.time() * 1000)}_response_hello"
        }

        canal_destino = self.get_canal_nombre(nodo_origen)
        await self.redis.publish(canal_destino, json.dumps(mensaje))
        print(f"[{self.nombre}] Respuesta hello enviada directamente a {nodo_origen}")

    async def enviar_hello(self, destino: str, mensaje: str = ""):
        await self.enviar_mensaje(
            tipo="hello",
            destino=destino,
            payload=mensaje or f"Hello from {self.nombre}!",
            hops=0
        )

    async def enviar_mensaje_usuario(self, destino: str, mensaje: str):
        await self.enviar_mensaje(
            tipo="message",
            destino=destino,
            payload=mensaje,
            hops=0
        )

    async def enviar_info(self, destino: str = "broadcast", neighbors: dict = None):
        if neighbors is None:
            neighbors = {self.get_canal_nombre(vecino): 1 for vecino in self.vecinos}

        await self.enviar_mensaje(
            tipo="info",
            destino=destino,
            payload="",
            hops=0,
            neighbors=neighbors
        )

    async def enviar_echo(self, destino: str, mensaje: str):
        await self.enviar_mensaje(
            tipo="echo",
            destino=destino,
            payload=mensaje,
            hops=0
        )

    async def enviar_a_canal_especifico(self, canal_completo: str, tipo: str, mensaje: str):
        mensaje_directo = {
            "type": tipo,
            "from": self.get_nombre_completo(),
            "to": canal_completo,
            "hops": 0,
            "headers": {"alg": "direct", "cross_group": True},
            "payload": mensaje,
            "msg_id": f"{self.nombre}_{int(time.time() * 1000)}_direct_{tipo}"
        }

        await self.redis.publish(canal_completo, json.dumps(mensaje_directo))
        print(f"[{self.nombre}] Mensaje directo enviado a {canal_completo}")

    async def ejecutar(self):
        await self.suscribirse_a_canales()
        print(f"[{self.nombre}] Nodo iniciado. Escuchando mensajes...")
        await self.escuchar_mensajes()
