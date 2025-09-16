#!/usr/bin/env python3

import daft
from daft import col
import datetime

def main():
    print('Testing TPC-H Q1 expressions...')

    # 创建一个简单的测试 DataFrame
    df = daft.from_pydict({
        'L_QUANTITY': [10.0, 20.0, 30.0],
        'L_EXTENDEDPRICE': [100.0, 200.0, 300.0],
        'L_DISCOUNT': [0.1, 0.2, 0.3],
        'L_TAX': [0.05, 0.10, 0.15],
        'L_RETURNFLAG': ['N', 'N', 'R'],
        'L_LINESTATUS': ['O', 'F', 'F'],
        'L_SHIPDATE': ['1998-08-01', '1998-07-01', '1998-06-01']
    })

    print('DataFrame schema:')
    schema_fields = df.schema()
    for field in schema_fields:
        print(f'  {field.name}: {field.dtype}')
    print()

    # 测试表达式
    try:
        print('Testing individual expressions...')
        
        # 测试基本的 sum 操作
        print('1. Basic sum operations:')
        result1 = df.agg(
            col('L_QUANTITY').sum().alias('sum_qty'),
            col('L_EXTENDEDPRICE').sum().alias('sum_base_price')
        )
        print('   Basic sum operations: OK')
        
        # 测试复合表达式
        print('2. Testing discounted_price expression:')
        discounted_price = col('L_EXTENDEDPRICE') * (1 - col('L_DISCOUNT'))
        result2 = df.select(discounted_price.alias('discounted_price'))
        print('   Discounted price expression: OK')
        
        # 测试复合表达式的 sum
        print('3. Testing sum of discounted_price:')
        result3 = df.agg(discounted_price.sum().alias('sum_disc_price'))
        print('   Sum of discounted price: OK')
        
        # 测试更复杂的表达式
        print('4. Testing taxed_discounted_price expression:')
        taxed_discounted_price = discounted_price * (1 + col('L_TAX'))
        result4 = df.agg(taxed_discounted_price.sum().alias('sum_charge'))
        print('   Sum of taxed discounted price: OK')
        
        # 测试 groupby + agg
        print('5. Testing groupby with aggregations:')
        result5 = (
            df.groupby(col('L_RETURNFLAG'), col('L_LINESTATUS'))
            .agg(
                col('L_QUANTITY').sum().alias('sum_qty'),
                col('L_EXTENDEDPRICE').sum().alias('sum_base_price'),
                discounted_price.sum().alias('sum_disc_price'),
                taxed_discounted_price.sum().alias('sum_charge')
            )
        )
        print('   Groupby with aggregations: OK')
        result5.show()
        
    except Exception as e:
        print(f'Error: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()