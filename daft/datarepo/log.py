import abc
import base64
import copy
import json
import os
from datetime import datetime
from typing import Any, ClassVar, Dict, List, Optional

import ipdb
import pyarrow as pa
import pydantic
from sqlalchemy import true

from daft.dataclasses import is_daft_dataclass
from daft.filesystem import get_filesystem_from_path
from daft.schema import DaftSchema


class DaftActionModel(pydantic.BaseModel):
    version: Optional[int]
    timestamp: datetime
    user: str
    operation: str
    operation_params: Dict[str, str]
    source_version: int


class DaftActionModelList(pydantic.BaseModel):
    schema: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("version", pa.int64()),
            pa.field("timestamp", pa.date64()),
            pa.field("user", pa.string()),
            pa.field("operation", pa.string()),
            pa.field("operation_params", pa.map_(pa.string(), pa.string())),
            pa.field("source_version", pa.int64()),
        ]
    )

    actions: List[DaftActionModel]

    def to_arrow_table(self) -> pa.Table:
        data_list = self.dict()["actions"]
        for item in data_list:
            item["operation_params"] = list(item["operation_params"].items())
        return pa.Table.from_pylist(data_list, schema=DaftActionModelList.schema)


class DaftLakeAction:
    name = None

    def __init__(self, user: str, source_version: int) -> None:
        self._user = user
        self._source_version = source_version

    def timestamp(self) -> datetime:
        return datetime.now()

    def user(self) -> str:
        return self._user

    def operation(self) -> str:
        assert self.name is not None
        return self.name

    @abc.abstractmethod
    def operation_params(self) -> Dict[str, Any]:
        ...

    def source_version(self) -> int:
        return self._source_version

    def generate_model(self) -> DaftActionModel:
        return DaftActionModel(
            timestamp=self.timestamp(),
            user=self.user(),
            operation=self.operation(),
            operation_params=self.operation_params(),
            source_version=self.source_version(),
        )


class DaftLakeAddFile(DaftLakeAction):
    name = "add"

    def __init__(self, user: str, source_version: int, file: str) -> None:
        DaftLakeAction.__init__(self, user, source_version)
        self._file = file

    def operation_params(self) -> Dict[str, Any]:
        return {"file": self._file}


class DaftLakeRemoveFile(DaftLakeAction):
    name = "remove"

    def __init__(self, user: str, source_version: int, file: str) -> None:
        DaftLakeAction.__init__(self, user, source_version)
        self._file = file

    def operation_params(self) -> Dict[str, Any]:
        return {"file": self._file}


class DaftLakeUpdateMetadata(DaftLakeAction):
    name = "update_metadata"

    def __init__(self, user: str, source_version: int, key: str, value: str) -> None:
        DaftLakeAction.__init__(self, user, source_version)
        self._key = key
        self._value = value

    def operation_params(self) -> Dict[str, str]:
        return {self._key: self._value}


class DaftLakeLog:
    SUBDIR = "_log"

    def __init__(self, path: str) -> None:
        self.path = path
        self._logdir = os.path.join(self.path, self.SUBDIR)
        self._fs = get_filesystem_from_path(path)
        self._commits: DaftActionModelList = DaftActionModelList(actions=list())
        self._to_commit: DaftActionModelList = DaftActionModelList(actions=list())
        self._metadata: Dict[str, str] = dict()
        self._current_version: Optional[int] = None
        self._user: Optional[str] = None
        self._schema: Optional[pa.Schema] = None
        self._in_transaction = False

        self._load_state()

    def start_transaction(self, user: Optional[str] = None) -> bool:
        if user is None:
            self._user = "anon"
        else:
            self._user = user

        if self._current_version is None:
            self._current_version = 0

        self._in_transaction = True
        return True

    def _load_state(self):
        exists = self._fs.exists(self._logdir)
        if not exists:
            self._fs.makedir(self._logdir)
            return
        files = sorted(self._fs.glob(f"{self._logdir}/*.json"))

        for file in files:
            with self._fs.open(file) as f:
                self._commits.actions.extend(DaftActionModelList(**json.load(f)).actions)
        for action in self._commits.actions:
            self._current_version = action.version
            if action.operation == "update_metadata":
                if "schema" in action.operation_params:
                    self._schema = self.__deserialize_schema(action.operation_params["schema"])
                self._metadata.update(action.operation_params)

    def create(self, name: str, schema: pa.Schema, already_exist_ok: bool = False) -> bool:
        assert self._current_version is None, "log already exists"
        self.start_transaction()
        self.update_name(name)
        self.update_schema(schema)
        status = self.commit() != 0
        self._schema = schema
        return status

    def schema(self) -> pa.Schema:
        self._schema

    def commit(self) -> int:
        if len(self._to_commit.actions) == 0:
            return 0

        commit_version = self._current_version + 1

        file_name = f"{commit_version:020d}.json"
        file_to_commit = os.path.join(self._logdir, file_name)

        if self._fs.exists(file_to_commit):
            return 0
        for action_model in self._to_commit.actions:
            action_model.version = commit_version

        with self._fs.open(file_to_commit, "w") as f:
            f.write(self._to_commit.json())

        self._commits.actions.extend(self._to_commit.actions)
        self._to_commit.actions.clear()
        self._current_version = commit_version
        self._user = None
        self._in_transaction = False

    def history(self) -> DaftActionModelList:
        return copy.deepcopy(self._commits)

    def add_file(self, file: str) -> None:
        self._add_action(DaftLakeAddFile(user=self._user, source_version=self._current_version, file=file))

    def remove_file(self, file: str) -> None:
        self._add_action(DaftLakeRemoveFile(user=self._user, source_version=self._current_version, file=file))

    def update_schema(self, schema: pa.Schema) -> None:
        self._add_action(
            DaftLakeUpdateMetadata(
                user=self._user,
                source_version=self._current_version,
                key="schema",
                value=self.__serialize_schema(schema),
            )
        )

    def update_name(self, name: str) -> None:
        self._add_action(
            DaftLakeUpdateMetadata(user=self._user, source_version=self._current_version, key="name", value=name)
        )

    def __serialize_schema(self, arrow_schema: pa.Schema) -> bytes:
        empty_table = arrow_schema.empty_table()
        buffer = pa.serialize(empty_table).to_buffer()
        data = buffer.to_pybytes()
        return base64.b64encode(data).decode()

    def __deserialize_schema(self, string_data: str) -> pa.Schema:
        data = base64.b64decode(string_data.encode())
        table = pa.deserialize(data)
        return table.schema

    def _add_action(self, action: DaftLakeAction) -> int:
        assert self._in_transaction, "you can only add actions during a transaction"
        self._to_commit.actions.append(action.generate_model())
        return len(self._to_commit.actions)
