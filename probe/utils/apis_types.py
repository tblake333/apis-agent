from enum import Enum

class Mutation(Enum):
    INSERT = 0
    UPDATE = 1
    DELETE = 2
    EMPTY = 3