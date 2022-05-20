import dataclasses as pydataclasses


def _default_setter(self, state):
    self.__dict__ = state


def _patch_class_for_deserialization(cls):
    if getattr(cls, "__daft_patched", None) != id(pydataclasses._FIELD):
        assert pydataclasses.is_dataclass(cls) and isinstance(cls, type)
        fields = cls.__dict__["__dataclass_fields__"]
        for field in fields.values():
            if type(field._field_type) is type(pydataclasses._FIELD):
                field._field_type = pydataclasses._FIELD
        setattr(cls, "__daft_patched", id(pydataclasses._FIELD))
