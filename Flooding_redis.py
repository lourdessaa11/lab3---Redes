import asyncio
import json
import redis.asyncio as redis
from typing import Dict, List, Set

class NodoFlooding:
    def __init__(self, nombre: str, host: str, port: int, password: str, vecinos: List[str], grupo: str = "grupo5", seccion: str = "sec10"):
        self.nombre = nombre
        self.vecinos = vecinos
        self.grupo = grupo
        self.seccion = seccion
        self.redis = redis.Redis(host=host, port=port, password=password, decode_responses=True)
        self.pubsub = self.redis.pubsub()
        # Para evitar procesar mensajes duplicados (usamos ID de paquete)
        self.paquetes_procesados: Set[str] = set()

    def get_canal_nombre(self, nodo: str) -> str:
        return f"{self.seccion}.{self.grupo}.{nodo}"

    async def suscribirse_a_canales(self):
        canales = [self.get_canal_nombre(vecino) for vecino in self.vecinos] + [self.get_canal_nombre(self.nombre)]
        await self.pubsub.subscribe(*canales)
        print(f"[{self.nombre}] Suscrito a canales: {canales}")

    async def enviar_mensaje(self, tipo: str, destino: str, payload: str, headers: dict = None, hops: int = 0):
        mensaje = {
            "type": tipo,
            "from": self.nombre,
            "to": destino,
            "hops": hops,
            "headers": headers or {},
            "payload": payload
        }
        canal_destino = self.get_canal_nombre(destino)
        await self.redis.publish(canal_destino, json.dumps(mensaje))
        print(f"[{self.nombre}] Enviado a {canal_destino}: {mensaje}")

    async def flooding_reenviar(self, datos: dict):
        ttl = datos.get('hops', 0) + 1
        if ttl > 5:
            print(f"[{self.nombre}] Mensaje descartado por TTL máximo: {ttl}")
            return

        mensaje_reenvio = {
            "type": datos['type'],
            "from": datos['from'],
            "to": datos['to'],
            "hops": ttl,
            "headers": datos.get('headers', {}),
            "payload": datos['payload']
        }

        for vecino in self.vecinos:
            if vecino != datos['from']:
                canal_vecino = self.get_canal_nombre(vecino)
                await self.redis.publish(canal_vecino, json.dumps(mensaje_reenvio))
                print(f"[{self.nombre}] Reenviado a canal {canal_vecino}")

    async def escuchar_mensajes(self):
        async for mensaje in self.pubsub.listen():
            if mensaje['type'] == 'message':
                try:
                    datos = json.loads(mensaje['data'])
                    await self.procesar_mensaje(datos)
                except json.JSONDecodeError:
                    print(f"[{self.nombre}] Error al decodificar mensaje: {mensaje['data']}")

    async def procesar_mensaje(self, datos: dict):
        paquete_id = f"{datos['from']}_{datos['to']}_{datos.get('hops', 0)}_{hash(datos['payload'])}"
        if paquete_id in self.paquetes_procesados:
            print(f"[{self.nombre}] Paquete ya procesado, ignorando: {paquete_id}")
            return
        self.paquetes_procesados.add(paquete_id)

        print(f"[{self.nombre}] Procesando mensaje de {datos['from']} para {datos['to']}")

        if datos['to'] == self.nombre:
            print(f"[{self.nombre}] ¡;Mensaje Recibido! De: {datos['from']}, Contenido: {datos['payload']}")
            return

        await self.flooding_reenviar(datos)

    async def enviar_mensaje_usuario(self, destino: str, mensaje: str):
        await self.enviar_mensaje(
            tipo="message",
            destino=destino,
            payload=mensaje,
            hops=0
        )

    async def ejecutar(self):
        await self.suscribirse_a_canales()
        print(f"[{self.nombre}] Nodo iniciado. Escuchando mensajes...")
        await self.escuchar_mensajes()