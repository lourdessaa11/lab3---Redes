# Configuración de la red
CONFIG = {
    "host": "localhost",
    "base_port": 9000,
    "buffer_size": 4096,
    "timeout": 5
}

# Topología de la red (puertos asignados a cada nodo)
NODOS_PUERTOS = {
    "A": 9000,
    "B": 9001,
    "C": 9002,
    "D": 9003,
    "E": 9004,
    "F": 9005,
    "G": 9006,
    "H": 9007,
    "I": 9008
}

TOPOLOGIA = {
    "A": ["B", "C"],
    "B": ["A", "D", "E"],
    "C": ["A", "F"],
    "D": ["B", "G"],
    "E": ["B", "G", "H"],
    "F": ["C", "H"],
    "G": ["D", "E", "I"],
    "H": ["E", "F", "I"],
    "I": ["G", "H"]
}