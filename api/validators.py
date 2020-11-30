from abc import ABC, abstractmethod
from pydoc import locate


class Validator(ABC):
    @abstractmethod
    def __call__(self, value):
        pass


class TypeValidator(Validator):
    def __init__(self, type_descr: str):
        self.type_ = locate(type_descr)

    def __call__(self, value):
        assert isinstance(self.type_, type)
        return isinstance(value, self.type_)
