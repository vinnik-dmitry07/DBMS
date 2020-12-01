from abc import ABC, abstractmethod
from pydoc import locate
import re


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


# https://html.spec.whatwg.org/multipage/input.html#email-state-(type=email)
class EmailValidator(Validator):
    def __call__(self, value):
        return re.match(r"^[a-zA-Z0-9.!#$%&'*+\\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?"
                        r"(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$", value)
