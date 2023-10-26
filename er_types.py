#editor.detectIndentation#
from typing import List, Dict, Set
from enum import Enum

class RelationshipType(Enum):
    ONE_TO_ONE = 1
    ONE_TO_MANY = 2
    MANY_TO_ONE = 3
    MANY_TO_MANY = 4

class KeyType(Enum):
    PRIMARY = 1
    FOREIGN = 2
    COMPOSITE = 3

class ObjectType(Enum):
    ENTITY = 1
    ATTRIBUTE = 2
    RELATIONSHIP = 3

class Column:
    '''
    A column in a table.

    Fields:
        name: The name of the column.
        type: The type of the column.
        key_type: The key type of the column (primary, foreign, composite, or None).
    '''
    def __init__(self, name: str, type: type, key_type: KeyType = None):
        self.name = name
        self.type = type
        self.key_type = key_type
    
    def __repr__(self) -> str:
        if self.key_type:
            return f'{self.name} ({self.type.__name__}) {self.key_type.name}'
        else:
            return f'{self.name} ({self.type.__name__})'

class Table:
    '''
    A table in the ER schema.

    Fields:
        name: The name of the table.
        columns: The columns of the table.
    '''
    def __init__(self, name: str, columns: List[Column]):
        self.name = name
        self.columns = columns
    
    def __repr__(self) -> str:
        return f'{self.name} ({self.columns})'

class Node:
    '''
    A node in the ER graph.

    Fields:
        name: The name of the node.
        object_type: The type of the node.
    '''
    def __init__(self, name: str):
        self.name = name
        self.object_type = None

class Relationship:
    '''
    A relationship between two nodes.

    Fields:
        name: The name of the relationship.
        type: The type of the relationship.
        parent: The parent node (must be an entity).
        child: The child node (must be an entity or attribute).
    '''
    def __init__(self, name: str, type: RelationshipType, parent: Node, child: Node):
        self.name = name
        self.type = type
        self.parent = parent
        self.child = child
        self.object_type = ObjectType.RELATIONSHIP
        self.__check_rep()
    
    def __str__(self):
        return f'{self.name} ({self.type.name}) {self.parent.name} -> {self.child.name}'
    
    def __repr__(self):
        return f'{self.name} ({self.type.name}) {self.parent.name} -> {self.child.name}'
    
    def __check_rep(self):
        if self.parent.object_type != ObjectType.ENTITY:
            raise Exception(f'Parent must be an entity')
        if self.child.object_type != ObjectType.ENTITY and self.child.object_type != ObjectType.ATTRIBUTE:
            raise Exception(f'Child must be an entity or attribute')

class Attribute(Node):
    '''
    An attribute of an entity.

    Fields:
        name: The name of the attribute.
        type: The type of the attribute.
    '''
    def __init__(self, name: str, type: type):
        super().__init__(name)
        self.type = type
        self.object_type = ObjectType.ATTRIBUTE
    
    def __str__(self):
        return f'{self.name} ({self.type.__name__})'
    
    def __repr__(self):
        return f'{self.name} ({self.type.__name__})'

class Entity(Node):
    '''
    An entity.

    Fields:
        name: The name of the entity.
    '''
    def __init__(self, name: str):
        super().__init__(name)
        self.object_type = ObjectType.ENTITY

    def __str__(self):
        return f'{self.name}'
    
    def __repr__(self):
        return f'{self.name}'

class ERSchema:
    '''
    An ER schema.

    Fields:
        vertices: The vertices of the ER graph.
        edges: The edges of the ER graph.
    '''
    def __init__(self, vertices: List[Node], edges: List[Relationship]):
        self.vertices = vertices
        self.edges = edges
    
    def __str__(self):
        return f"ERSchema({self.vertices})"

    def __repr__(self):
        return self.__str__()