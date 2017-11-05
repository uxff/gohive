# gohive

使用golang链接hiveserver2的库。

对应hive版本是2.1。

go通过hive的thrift rpc接口链接hiveserver2。

实现了同步查询，异步查询等接口，并简化调用，优化性能，客户端不用处理复杂的thrift接口传输对象。

go get "github.com/uxff/gohive"

当前使用thrift-0.9.3版本，与hive的thrift对应。

如果需要更新到thrift-0.10以上版本，则删除tcliservice，将tcliservice.ctx重命名为tcliservice，进行go build会报错，将对应的thrift接口第一个参数增加context.Background(), 即可。

sql语法中查询字段设置别名必须用反引号 ` 。

例如:

```
select tcid, uid `用户id`, bid+1, sid/100, tcphone, tccharge, tccreated from mall_trade_charges_update_default limit 15
```
example:
```go
package main

import (
    "flag"
    "fmt"
    "github.com/uxff/gohive"
)


func main() {

    // usage : ./main -h 127.0.0.1:50000 -d default -q "show tables"

    hiveAddr := flag.String("h", "101.201.57.41:50000", "addr of hive")
    db := flag.String("d", "default", "db of hive")
    query := flag.String("q", "show tables", "hsql of query")
    flag.Parse()

    conn, err := gohive.Connect(*hiveAddr, gohive.DefaultOptions)
    if err != nil {
        fmt.Println("connect hive error:", err)
        return
    }

    conn.Exec("use " + *db)

    rets, err := conn.SimpleQuery(*query)

    if err != nil {
        fmt.Println("query errer:", err)
    }

    fmt.Println("hive query result=", rets)
    
}

```

以上用例已测试通过。后续将补充thrift-0.10版本。

