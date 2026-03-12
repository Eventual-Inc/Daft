# Issue #2598 可行性分析

## 1. issue summary
为 `daft.DataFrame` 增加 `_repr_mimebundle_`，让不支持 `_repr_html_` 的 Jupyter 前端（例如 Zed）也能正确展示 DataFrame。

## 2. root cause
当前 `DataFrame` 只提供 `__repr__` 和 `_repr_html_`，部分前端优先走 `_repr_mimebundle_` 协议，缺失该协议会导致展示降级或不可用。

## 3. expected modification modules
- `daft/dataframe/dataframe.py`
- `tests/dataframe/test_repr.py`

## 4. implementation plan
1. 在 `DataFrame` 上新增 `_repr_mimebundle_(include=None, exclude=None)`。
2. 默认返回 `text/plain` 与 `text/html` 两种 mime。
3. 支持 `include`/`exclude` 过滤，兼容 IPython display 协议。
4. 新增测试覆盖默认返回与 include/exclude 过滤行为。

## 复杂度评估
- 预计修改文件数：2（<20）
- API 设计变更：无（仅新增兼容显示协议方法）
- 架构调整：无
- 多模块重构：无

结论：可直接实现，风险低。
