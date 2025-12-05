
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class FieldType(Enum):
    LITERAL = "literal"
    URI = "uri"
    EITHER = "either"
    LITERAL_NO_LANG = "nolanguage_literal"

@dataclass
class SkosFieldInfo:
    name: str
    field_type: FieldType
    multivalued: bool = True
    lang_dependent: bool = True
    
@dataclass
class TripleQueryParam:
    
    uri: Optional[str] = None
    skos_field: Optional[str] = None 
    value: Optional[str] = None 
    lang: Optional[str] = None
    
    is_concept_query: bool = False

@dataclass
class TriplePattern:
    
    def __init__(self, s=None, p=None, o=None):
        self.s = s
        self.p = p
        self.o = o
    
    @property
    def subject(self):
        return self.s

    @property
    def predicates(self):
        # support single or list
        if self.p is None:
            return None
        if isinstance(self.p, list):
            return self.p
        return [self.p]

    @property
    def object(self):
        return self.o
        