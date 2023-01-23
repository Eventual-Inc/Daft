from __future__ import annotations

import polars as pl
import numpy as np
import daft
from tempfile import TemporaryDirectory
from daft.logical.state import State

"""
A state holds the logical plan and metadat needed to run the logical plan on new data.
This can save a lot of time when implementing ML pipelines.

The state.inference(df, filters:bool=False, reminder:bool=True) will apply all the calculations which was saved 
to the state on the df, ignoring filter (as we almost always want at inference time) 
and let new columns which were not introduce to the state to propagate and stay, so we may return them if needed.    
"""


def test_state_infer():
    """
    An infer function to deal with standard IO can help to build services quicker and easier.
    Any standard input coming for query, needs to first turned into a daft.DataFrame.

    Every user, who solves any problem will need to implement it themselves every time with small variation.
    """
    df = daft.DataFrame.read_csv('tests/assets/iris.csv').limit(1).to_pandas()

    state = State.from_dataframe(df)
    # Inference use cases
    isinstance(state.infer(df.to_dict(orient='records')), daft.DataFrame)  # [{"key":value}]
    isinstance(state.infer(df.to_json(orient='records')), daft.DataFrame)  # '[{"key":value}]'
    isinstance(state.infer(df.to_dict(orient='list')), daft.DataFrame)  # {"column":[value_1, value_2]}
    # Integrations use cases
    isinstance(state.infer(pl.from_pandas(df).to_arrow()), daft.DataFrame)  # pyarrow.Table
    isinstance(state.infer(df.values), daft.DataFrame)  # numpy


def test_state_inference():
    @daft.polars_udf(return_type=float)
    class RunExpensiveModel:

        def __init__(self):
            # Initialize and cache an "expensive" model between invocations of the UDF
            self.model = np.array([1.23, 4.56])

        def __call__(self, a_data: pl.Series, b_data: pl.Series):
            print(a_data, b_data)
            return np.matmul(self.model, np.array([a_data.to_numpy(), b_data.to_numpy()]))

    df = daft.DataFrame.read_csv('tests/assets/iris.csv')
    train, test = df.limit(10), df.limit(10)  # test should be different train

    train = train.with_column('model_results',
                              RunExpensiveModel(daft.col('sepal.length'), daft.col('sepal.width')).alias(
                                  'model_output'))
    state = State.from_dataframe(train)
    temepdir = TemporaryDirectory()
    state_path = temepdir.name + 'state.pkl'
    state.save(state_path)
    state = State.from_file(state)
    for data in (test, test.to_dict(orient='records'), df.to_dict(orient='records'), df.to_json(orient='records'),
                 df.to_dict(orient='list'), pl.from_pandas(df).to_arrow(), df.values):
        assert 'model_results' in state.inference(data, filters=False, remainder=True).column_names


def test_state_inference_filters():
    """
    Use cases where we want to get predictions for the entire data.
    Avoid cleaning missing values and outliers in production and metric evaluations. 
    """
    df = daft.DataFrame.read_csv('tests/assets/iris.csv')
    filtered = df[df['sepal.length'] > 5]  # filter
    state = State.from_dataframe(filtered)
    assert len(state.inference(df, filters=False)) == len(df)
    assert len(state.inference(df, filters=True)) == len(filtered)


def test_state_inference_reminder():
    """
    Use cases where we send additional information to the inference function, which we want to keep.
    For example, an identifier for each row, or a timestamp.
    """
    df = daft.DataFrame.read_csv('tests/assets/iris.csv').to_pandas()
    filtered = df.select('sepal.length', 'sepal.width')
    state = State.from_dataframe(filtered)
    assert len(state.inference(df, remainder=True).columns) == len(df.columns)
    assert len(state.inference(df, remainder=False).columns) == len(filtered.columns)
