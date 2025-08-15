# Código obtenido de https://www.w3schools.com/dsa/dsa_algo_graphs_dijkstra.php

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

    def dijkstra(self, info_nodo):
        i_nodo = self.vertex_data.index(info_nodo)
        distancia = [float('inf')] * self.size
        distancia[i_nodo] = 0
        visitado = [False] * self.size

        for _ in range(self.size):
            dist_min = float('inf')
            u = None
            for i in range(self.size):
                if not visitado[i] and distancia[i] < dist_min:
                    dist_min = distancia[i]
                    u = i

            if u is None:
                break

            visitado[u] = True

            for v in range(self.size):
                if self.adj_matrix[u][v] != 0 and not visitado[v]:
                    alt = distancia[u] + self.adj_matrix[u][v]
                    if alt < distancia[v]:
                        distancia[v] = alt

        return distancia