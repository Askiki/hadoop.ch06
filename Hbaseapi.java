package com.mystudy1.hadoop.ch06.v029;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Hello world!
 *
 */
class Hbaseapi
{
    public static Configuration conf;
    static{
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.30.131");
        conf.set("hbase.rootdir","hdfs://192.168.30.131:8020");
        conf.set("hbase.cluster.distributed","true");
    }

    //创建表
    private static void createTable() throws Exception {
//        HBaseAdmin admin = new HBaseAdmin(conf);//HBaseAdmin(conf);
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //表名
        HTableDescriptor htd =new HTableDescriptor(TableName.valueOf("emp029"));
        //列族名
        String[] family = new String[]{"member_id","address","info"};
        for (int i=0;i<family.length;i++){
            htd.addFamily(new HColumnDescriptor(family[i]));
        }
        //创建表
        if (admin.tableExists(TableName.valueOf("emp029"))){
            System.out.println("表创建失败（表已经存在了）!!!\n");
            System.exit(0);
        } else {
            admin.createTable(htd);
            System.out.println("创建表成功!!!\n");
        }
        admin.close();
    }

    //插入(或更新数据)单条数据
    private static void inset() throws Exception{
        HTable table = new HTable(conf,"emp029");
        Put put = new Put(Bytes.toBytes("Rain"));
        put.addColumn(Bytes.toBytes("member_id"),
                Bytes.toBytes("id"),
                Bytes.toBytes("31"));
        put.addColumn(Bytes.toBytes("info"),
                Bytes.toBytes("age"),
                Bytes.toBytes("28"));
        put.addColumn(Bytes.toBytes("info"),
                Bytes.toBytes("birthday"),
                Bytes.toBytes("1990-05-01"));
        put.addColumn(Bytes.toBytes("info"),
                Bytes.toBytes("industry"),
                Bytes.toBytes("architect"));
        put.addColumn(Bytes.toBytes("address"),
                Bytes.toBytes("city"),
                Bytes.toBytes("ShenZhen"));
        put.addColumn(Bytes.toBytes("address"),
                Bytes.toBytes("country"),
                Bytes.toBytes("China"));
        table.put(put);
        System.out.println("数据插入成功!!!\n");
        table.close();
    }

    //获取行某列族
    private static void get() throws Exception{
        HTable table = new HTable(conf,"emp029");
        Get get = new Get(Bytes.toBytes("Rain"));
        //
        Result record = table.get(get);
        //获取行数据
        String[] age = {Bytes.toString(record.getValue(Bytes.toBytes("info"),Bytes.toBytes("age"))),
                Bytes.toString(record.getValue(Bytes.toBytes("info"),Bytes.toBytes("birthday"))),
                Bytes.toString(record.getValue(Bytes.toBytes("info"),Bytes.toBytes("industry")))};
        //输出结果

        System.out.println("查询表emp213列族为info，行键为Rain的数据：\n"+
                "rowkey：Rain  info:age  value="+age[0]+"\n"+
                "rowkey：Rain  info:birthday  value="+age[1]+"\n"+
                "rowkey：Rain  info:industry  value="+age[2]+"\n");
        table.close();
    }

    //扫描scan
    private static void scan() throws Exception{
        HTable table = new HTable(conf,"emp029");
        Scan scanner = new Scan();
        //scanner.setFilter(filter);//过滤器
        //返回游标
        ResultScanner rs = table.getScanner(scanner);
        //执行查询
        for (Result r:rs){
            System.out.println("“Rain”的info:birthday的属性为："+new String(Bytes.toString(r.getValue(Bytes.toBytes("info"),Bytes.toBytes(("birthday"))))));
        }
        table.close();
    }
    public static void main(String[] args) throws Exception{
        createTable();
        inset();
        get();
        scan();
    }
}
