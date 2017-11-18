#go 通过 odbc 链接hive

###安装

```
yum install unixODBC
yum install unixODBC-devel
## mysql driver require
yum install mysql-connector-odbc
## hive driver require
## https://cwiki.apache.org/confluence/display/Hive/HiveODBC

```
官方说不提供hive odbc driver libodbchive.so

此处说明： https://cwiki.apache.org/confluence/display/Hive/HiveODBC
> There is no ODBC driver available for HiveServer2 as part of Apache Hive. There are third party ODBC drivers available from different vendors, and most of them seem to be free.

找到了一个Cloudera公司的hiveodbc驱动，地址：
https://www.cloudera.com/downloads/connectors/hive/odbc/2-5-12.html

下载ClouderaHiveODBC-2.5.20.1006-1.el7.x86_64.rpm

```
$ rpm -ivh ClouderaHiveODBC-2.5.20.1006-1.el7.x86_64.rpm
error: Failed dependencies:
    cyrus-sasl-gssapi(x86-64) >= 2.1.26 is needed by ClouderaHiveODBC-2.5.20.1006-1.x86_64
    cyrus-sasl-plain(x86-64) >= 2.1.26 is needed by ClouderaHiveODBC-2.5.20.1006-1.x86_64

$ yum install cyrus-sasl-gssapi
$ yum install cyrus-sasl-plain
# 重新安装
$ rpm -ivh ClouderaHiveODBC-2.5.20.1006-1.el7.x86_64.rpm

```
###配置

配置参考这里：
https://www.ibm.com/support/knowledgecenter/SSCVKV_9.1.2/Campaign/DatabaseTableAdmin/Configuring_the_Cloudera_ODBC_driver.html

配置odbcinst.ini - 已经自动生成，不用配置
配置odbc.ini
vim /etc/odbc.ini

```
[ClouderaHive]
Driver=/opt/cloudera/hiveodbc/lib/64/libclouderahiveodbc64.so
Description=Hive Cloudera ODBC Driver
Host=<hostname or ip of Hive server on Hadoop Distribution machine> 
Port=<port number of Hive server on Hadoop Distribution machine> 
Schema=<database-name>
ServiceDiscoveryMode=0
ZKNamespace=
HiveServerType=2
#AuthMech=3
# 0=NOSASL 测试通过
AuthMech=0
ThriftTransport=1
UseNativeQuery=0
UID=cloudera
```
在/etc/odbcinst.ini中增加调试：
```
[ODBC]
Trace=Yes
TraceFile=/tmp/odbc.trace.log
```
遇到问题可tail -f /tmp/odbc.trace.log来查看

###测试

isql {dsnname} 
dsnname就是/etc/odbc.ini中的块名
```
$ isql ClouderaHive
+---------------------------------------+
| Connected!                            |
|                                       |
| sql-statement                         |
| help [tablename]                      |
| quit                                  |
|                                       |
+---------------------------------------+
SQL>
```

##go链接

github上go使用odbc主要有两种库:
- github.com/alexbrainman/odbc
- github.com/weigj/go-odbc


这里使用gohive "github.com/weigj/go-odbc" 可以顺利连接hive.


```
// Open
conn, err = gohive.Connect("DSN="+"ClouderaHive")
// 直接执行的提交
_, err := conn.ExecDirect("use "+hiveDbname)
// 查询
stmt, err := conn.Prepare(queryStr)
stmt.Execute()
// 获取列
colNum, err := op.NumFields()
var heads []string
for i:=0; i<colNum; i++ {
    // 注意取列要从下标1开始
    cols, err := op.FieldMetadata(i+1)
    heads = append(heads, cols.Name)
}
log.Printf("heads=%v", heads)
// 获取结果
rets, err := stmt.FetchAll()
log.Printf("rets=%v", rets)
// Close
stmt.Close()
conn.Close()


```

###问题及解决方案

测试发现isql可以连接本机，不能连接网络中的mysql服务器。mysql命令可以连接上网络中的mysql服务器。
遇到问题：
/etc/odbc.ini中，UserName字段无效，使用User字段代替。
遇到问题：
AuthMech要设置成0，0=NOSASL

使用gohive "github.com/weigj/go-odbc" 可以顺利连接hive.
遇到问题：
gohive.stmt.Prepare,Fetch,FetchAll等接口都返回不为nil的空err，需要手动处理。
