package com.bytegriffin.datatunnel.read;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.bytegriffin.datatunnel.core.HandlerContext;
import com.bytegriffin.datatunnel.core.Param;
import com.bytegriffin.datatunnel.meta.HBaseContext;
import com.bytegriffin.datatunnel.meta.Record;
import com.bytegriffin.datatunnel.sql.Field;
import com.bytegriffin.datatunnel.sql.SelectObject;
import com.bytegriffin.datatunnel.sql.SqlMapper;
import com.bytegriffin.datatunnel.sql.SqlParser;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class HBaseReader implements Readable {

    private static final Logger logger = LogManager.getLogger(HBaseReader.class);

    @Override
    public void channelRead(HandlerContext ctx, Param msg) {
        Connection connection = Globals.getHBaseConnection(this.hashCode());
        OperatorDefine opt = Globals.operators.get(this.hashCode());
        String newsql = SqlParser.getReadSql(opt.getValue());
        List<Record> results = select(connection, newsql);
        msg.setRecords(results);
        ctx.write(msg);
        logger.info("线程[{}]调用MysqlReader执行任务[{}]", Thread.currentThread().getName(), opt.getKey());
    }

    /**
     * sql 查询引擎，类似：select cf.name, cf.age from student where row=22 and cf.b =3  <br>
     * 注意sql中必须包含column family，column family与column之间用点号隔开<br>
     * 目前版本查询支持简单的单表查询，且and和or不能同时出现
     *
     * @param connection
     * @param sql
     * @return
     */
    private List<Record> select(Connection connection, String sql) {
        if (Strings.isNullOrEmpty(sql)) {
            return null;
        }
        SelectObject select = SqlMapper.select(sql);
        List<Record> list = Lists.newArrayList();
        try {
            Scan scan = new Scan();
            Filter filter;
            List<String> columns = select.getColumn();
            if (columns.size() == 1 && columns.get(0).contains("*")) {
                //查询所有字段
            } else {//设置要查询的字段，类似 select name from table 中的name
                columns.forEach(column -> {
                    List<String> allcolumn = Splitter.on(HBaseContext.column_familiy_split).trimResults().omitEmptyStrings().splitToList(column);
                    scan.addColumn(Bytes.toBytes(allcolumn.get(0)), Bytes.toBytes(allcolumn.get(1)));
                });
            }
            List<Filter> listFilters = Lists.newArrayList();
            if (!Strings.isNullOrEmpty(select.getWhere())) {
                List<String> ands = select.getAndCondition(select.getWhere());
                List<String> ors = select.getOrCondition(select.getWhere());
                if (ands != null) {//用and连接的查询条件
                    ands.forEach(con -> setQueryFilter(con, listFilters));
                    filter = new FilterList(FilterList.Operator.MUST_PASS_ALL, listFilters);
                } else if (ors != null) {//用or连接的查询条件
                    ors.forEach(con -> setQueryFilter(con, listFilters));
                    filter = new FilterList(FilterList.Operator.MUST_PASS_ONE, listFilters);
                } else {//只存在一个查询条件
                    setQueryFilter(select.getWhere(), listFilters);
                    filter = new FilterList(listFilters);
                }
                scan.setFilter(filter);
            }
            Table table = connection.getTable(TableName.valueOf(select.getTableName()));
            ResultScanner resultset = table.getScanner(scan);
            resultset.forEach(result -> {
                Record record = new Record();
                //强制设置主键，如果writer是HBaseWriter那么操作必须要有row
                List<Field> fields = Lists.newArrayList(new Field(HBaseContext.row_key, Bytes.toString(result.getRow())));
                //设置其它column字段
                result.listCells().forEach(cell -> {
                    String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                    String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String columnValue = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    fields.add(new Field(columnFamily + HBaseContext.column_familiy_split + columnName, columnValue));
                });
                record.setFields(fields);
                list.add(record);
            });
        } catch (Exception e) {
            logger.error("HBaseReader查询数据时出错: {}", sql, e);
        }
        return list;
    }

    /**
     * 根据where条件设置查询Filter
     *
     * @param condition
     * @param listFilters
     */
    private void setQueryFilter(String condition, List<Filter> listFilters) {
        //获取类似 name = zhangsan 单个where条件 ，暂时不支持like/in查询
        List<String> conlist = Splitter.on("=").trimResults().omitEmptyStrings().splitToList(condition);
        String left = conlist.get(0).toLowerCase().trim();
        String right = SqlParser.removeSqlQuotes(conlist.get(1).trim());
        if (!left.contains(HBaseContext.column_familiy_split) && left.contains(HBaseContext.row_key)) {//主键row查询filter
            listFilters.add(new RowFilter(CompareOperator.EQUAL, new BinaryComparator(right.getBytes())));//直接根据主键查询
        } else {//字段查询filter
            List<String> allleft = Splitter.on(HBaseContext.column_familiy_split).trimResults().omitEmptyStrings().splitToList(left);
            listFilters.add(new SingleColumnValueFilter(Bytes.toBytes(allleft.get(0)), Bytes.toBytes(allleft.get(1)), CompareOperator.EQUAL, Bytes.toBytes(right)));
        }
    }

}
