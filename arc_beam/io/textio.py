import json
from json import JSONEncoder

from arc_beam.coders import coders
from arc_beam.transforms.ptransform import PTransform

__all__ = [
    'ReadFromText',
    'WriteToText',
    'ReadFromSocket',
    'WriteToSocket',
]


class ReadFromText(PTransform):
    def __init__(self, path, coder=coders.StrUtf8Coder()):
        self.coder = coder
        self.path = path
        super(ReadFromText, self).__init__()

    def encode(self, name):
        return {
            "id": name,
            "kind": {
                "Source": {
                    "format": self.coder.format(),
                    "kind": {
                        "LocalFile": {"path": self.path}
                    }
                }
            }
        }


class WriteToText(PTransform):
    def __init__(self, path, coder=coders.StrUtf8Coder()):
        self.coder = coder
        self.path = path
        super(WriteToText, self).__init__()

    def encode(self, name):
        return {
            "id": name,
            "kind": {
                "Sink": {
                    "format": self.coder.format(),
                    "kind": {
                        "LocalFile": {"path": self.path}
                    }
                }
            }
        }


class ReadFromSocket(PTransform):
    def __init__(self, addr, coder=coders.StrUtf8Coder()):
        self.coder = coder
        self.addr = addr
        super(ReadFromSocket, self).__init__()

    def encode(self, name):
        return {
            "id": name,
            "kind": {
                "Source": {
                    "format": self.coder.format(),
                    "kind": {
                        "Socket": {"addr": self.addr}
                    }
                }
            }
        }


class WriteToSocket(PTransform):
    def __init__(self, addr, coder=coders.StrUtf8Coder()):
        self.coder = coder
        self.addr = addr
        super(WriteToSocket, self).__init__()

    def encode(self, name):
        return {
            "id": name,
            "kind": {
                "Sink": {
                    "format": self.coder.format(),
                    "kind": {
                        "Socket": {"addr": self.addr}
                    }
                }
            }
        }
