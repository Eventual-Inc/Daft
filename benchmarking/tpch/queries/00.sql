SELECT
    l_returnflag,
    l_linestatus,
    avg(l_quantity) AS avg_qty
FROM
    lineitem
WHERE
    l_shipdate <= CAST('1998-09-02' AS date)
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
