class Nodo:
    def __init__(self, nombre, puerto, vecinos):
        self.nombre = nombre
        self.puerto = puerto
        self.vecinos = vecinos  
        self.socket = SocketServer(puerto)
        self.tabla_ruteo = {}
        
    def escuchar(self):
        while True:
            mensaje = self.socket.recibir()  
            self.procesar_mensaje(mensaje)
            
    def enviar(self, destino, mensaje):
        # Busca en su tabla a qué vecino enviar
        siguiente_salto = self.tabla_ruteo[destino]
        ip, puerto = siguiente_salto.split(":")
        self.socket.enviar(ip, int(puerto), mensaje)

class Graph:
    def __init__(self, size):
        self.adj_matrix = [[0] * size for _ in range(size)]
        self.size = size
        self.vertex_data = [''] * size

    def add_arista(self, u, v, peso):
        if 0 <= u < self.size and 0 <= v < self.size:
            self.adj_matrix[u][v] = peso
            self.adj_matrix[v][u] = peso

    def add_nodo(self, nodo, data):
        if 0 <= nodo < self.size:
            self.vertex_data[nodo] = data

    def flooding(self, origen, destino, mensaje, ttl=5):
      
        if origen < 0 or origen >= self.size or destino < 0 or destino >= self.size:
            return "Nodos inválidos"
        
        # Estructura para registrar paquetes ya procesados (evitar reprocesamiento)
        paquetes_procesados = set()
        paquete_id = f"{origen}_{destino}_{hash(mensaje)}"
        
        # Cola para procesamiento de paquetes (simula el envío)
        cola_paquetes = [(origen, mensaje, ttl, [origen], paquete_id)]
        resultados = []
        
        while cola_paquetes:
            nodo_actual, mensaje_actual, ttl_actual, camino, id_paquete = cola_paquetes.pop(0)
            
            # Si ya procesamos este paquete, lo saltamos
            if id_paquete in paquetes_procesados:
                continue
                
            paquetes_procesados.add(id_paquete)
            
            # Si llegamos al destino
            if nodo_actual == destino:
                resultados.append({
                    "camino": camino,
                    "mensaje": mensaje_actual,
                    "ttl_final": ttl_actual
                })
                continue
            
            # Si el TTL llegó a cero, descartamos el paquete
            if ttl_actual <= 0:
                continue
            
            # Encontrar todos los vecinos del nodo actual
            vecinos = []
            for i in range(self.size):
                if self.adj_matrix[nodo_actual][i] != 0 and i != nodo_actual:
                    vecinos.append(i)
            
            # Reenviar el paquete a todos los vecinos
            for vecino in vecinos:
                if vecino not in camino:  # Evitar ciclos simples
                    nuevo_camino = camino + [vecino]
                    nuevo_id = f"{id_paquete}_{vecino}"
                    cola_paquetes.append((
                        vecino, 
                        mensaje_actual, 
                        ttl_actual - 1, 
                        nuevo_camino,
                        nuevo_id
                    ))
        
        return resultados
    

