from daft import col
from daft.table import MicroPartition

def test_table_expr_between_same_types() -> None:
    input = [1, 2, 3, 4]
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").between(1, 2)])
    expected = [True, True, False, False]
    pydict = daft_table.to_pydict()
    assert pydict["input"] == expected