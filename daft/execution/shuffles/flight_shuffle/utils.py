def get_shuffle_file_path(
    node_id: str,
    shuffle_stage_id: int,
    partition_id: int | None = None,
    mapper_id: int | None = None,
):
    if partition_id is None:
        return f"/tmp/daft_shuffle/node_{node_id}/shuffle_stage_{shuffle_stage_id}"
    elif mapper_id is None:
        return f"/tmp/daft_shuffle/node_{node_id}/shuffle_stage_{shuffle_stage_id}/partition_{partition_id}"
    else:
        return f"/tmp/daft_shuffle/node_{node_id}/shuffle_stage_{shuffle_stage_id}/partition_{partition_id}/{mapper_id}.arrow"
