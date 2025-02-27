import daft


@daft.udf(return_dtype=str)
def run_curator(data: daft.Series):
    from bespokelabs import curator

    llm = curator.LLM(model_name="gpt-4o-mini")
    ds = llm(data.to_pylist())
    return list(ds.to_pandas()["response"])
