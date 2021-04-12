## StaffSchema的用途
主要是用来做技术分享时说明Calcite的核心流程，一般会使用下面的SQL查询：
```sql
SELECT
	u.NAME,
	u.age,
	c.NAME AS user_company 
FROM
	users u
	JOIN companies c ON u.company = c.id 
WHERE
	u.age >= 26 
ORDER BY
	u.age DESC
```

那么其生成的逻辑计划为：

```
LogicalSort(sort0=[$1], dir0=[DESC])
  LogicalProject(NAME=[$1], age=[$2], user_company=[$5])
    LogicalFilter(condition=[>=($2, 26)])
      LogicalJoin(condition=[=($3, $4)], joinType=[inner])
        EnumerableTableScan(table=[[staff, users]])
        EnumerableTableScan(table=[[staff, companies]])
```

其生成的物理计划为：

```
EnumerableCalc(expr#0..4=[{inputs}], proj#0..1=[{exprs}], name0=[$t4])
  EnumerableSort(sort0=[$1], dir0=[DESC])
    EnumerableHashJoin(condition=[=($2, $3)], joinType=[inner])
      EnumerableCalc(expr#0..3=[{inputs}], expr#4=[26], expr#5=[>=($t2, $t4)], name=[$t1], age=[$t2], company=[$t3], $condition=[$t5])
        EnumerableTableScan(table=[[staff, users]])
      EnumerableTableScan(table=[[staff, companies]])
```

通过上面这个例子，是可以很好地说明Calcite的各个核心流程。