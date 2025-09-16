#!/usr/bin/env python3

import daft
from daft import col
import datetime

def main():
    print('Testing date comparison operations...')

    # 创建一个测试 DataFrame，模拟 SQL 读取的情况
    # 这里 L_SHIPDATE 被识别为 Utf8 类型（就像从 SQLite 读取的情况）
    df = daft.from_pydict({
        'L_QUANTITY': [10.0, 20.0, 30.0],
        'L_EXTENDEDPRICE': [100.0, 200.0, 300.0],
        'L_DISCOUNT': [0.1, 0.2, 0.3],
        'L_TAX': [0.05, 0.10, 0.15],
        'L_RETURNFLAG': ['N', 'N', 'R'],
        'L_LINESTATUS': ['O', 'F', 'F'],
        'L_SHIPDATE': ['1998-08-01', '1998-07-01', '1998-06-01']  # 这些是字符串，就像 SQLite 中的日期
    })

    print('DataFrame schema:')
    schema_fields = df.schema()
    for field in schema_fields:
        print(f'  {field.name}: {field.dtype}')
    print()

    try:
        print('Testing date comparison...')
        
        # 测试日期比较操作（这可能会导致类型问题）
        print('1. Date comparison filter:')
        filtered_df = df.where(col("L_SHIPDATE") <= datetime.date(1998, 9, 2))
        print('   Date comparison filter: OK')
        
        # 检查过滤后的 schema
        print('   Filtered DataFrame schema:')
        for field in filtered_df.schema():
            print(f'     {field.name}: {field.dtype}')
        print()
        
        # 测试在过滤后进行聚合操作
        print('2. Testing aggregation after date filter:')
        discounted_price = col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))
        taxed_discounted_price = discounted_price * (1 + col("L_TAX"))
        
        result = (
            filtered_df.groupby(col("L_RETURNFLAG"), col("L_LINESTATUS"))
            .agg(
                col("L_QUANTITY").sum().alias("sum_qty"),
                col("L_EXTENDEDPRICE").sum().alias("sum_base_price"),
                discounted_price.sum().alias("sum_disc_price"),
                taxed_discounted_price.sum().alias("sum_charge"),
            )
        )
        print('   Aggregation after date filter: OK')
        result.show()
        
    except Exception as e:
        print(f'Error: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()