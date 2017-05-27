# SQLServerSync
sqlserver db sync tools
sqlserver 表数据同步工具,底层采用sqldataAdapter+sqlCommand
1. 支持sqlserver 2005以上
2. 同步的两张表数据结构一致，目标表如果有自增列需要删除自增列
3. 支持源表--->目标表，目标表--->源表 双向同步copy
4. 支持源表到目标表 的只添加和修改，不删除目标表的数据，设置IsDeleteTargetRow=true
5. 当设置IsCheckTableSchema=true时，表结构不一致不能copy,默认false,只需要表的列的类型一致就可以
