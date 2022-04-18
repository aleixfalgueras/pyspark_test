from dataclasses import dataclass
from collections import namedtuple


@dataclass(frozen=True)
class Recommendation:
    name: str
    age: int

Point = namedtuple('Point', ['x', 'y'])
