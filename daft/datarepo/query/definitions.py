from daft.dataclasses import dataclass

# Node IDs in the NetworkX graph are uuid strings
NodeId = str

@dataclass
class WriteDatarepoStageOutput:
    filepath: str
