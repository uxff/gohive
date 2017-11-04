// Package hivething wraps the hiveserver2 thrift interface in a few
// related interfaces for more convenient use.
package gohive

import (
	//"context"
	"errors"
	"fmt"
    "log"
	"reflect"
	"time"

	inf "github.com/uxff/gohive/tcliservice"

	//"git.apache.org/thrift.git/lib/go/thrift"
    "github.com/apache/thrift/lib/go/thrift"
)

// Options for opened Hive sessions.
type Options struct {
	PollIntervalSeconds int64
	BatchSize           int64
	QueryTimeout        int64
}

var (
	DefaultOptions = Options{PollIntervalSeconds: 2, BatchSize: 1000, QueryTimeout: 100}
)

type Connection struct {
	thrift  *inf.TCLIServiceClient
	session *inf.TSessionHandle
	options Options
}

// Represents job status, including success state and time the
// status was updated.
type Status struct {
	state *inf.TOperationState
	Error error
	At    time.Time
}

func Connect(host string, options Options) (*Connection, error) {
	transport, err := thrift.NewTSocket(host)
	if err != nil {
		return nil, err
	}

	if err := transport.Open(); err != nil {
		return nil, err
	}

	if transport == nil {
		return nil, errors.New("nil thrift transport")
	}

	/*
		NB: hive 0.13's default is a TSaslProtocol, but
		there isn't a golang implementation in apache thrift as
		of this writing.
	*/
	protocol := thrift.NewTBinaryProtocolFactoryDefault()
	client := inf.NewTCLIServiceClientFactory(transport, protocol)
	s := inf.NewTOpenSessionReq()
	s.ClientProtocol = 6
	//	session, err := client.OpenSession(inf.NewTOpenSessionReq())
	session, err := client.OpenSession(s)
	if err != nil {
		return nil, err
	}

	return &Connection{client, session.SessionHandle, options}, nil
}

func (c *Connection) isOpen() bool {
	return c.session != nil
}

// Closes an open hive session. After using this, the
// connection is invalid for other use.
func (c *Connection) Close() error {
	if c.isOpen() {
		closeReq := inf.NewTCloseSessionReq()
		closeReq.SessionHandle = c.session
		resp, err := c.thrift.CloseSession(closeReq)
		if err != nil {
			return fmt.Errorf("Error closing session: ", resp, err)
		}

		c.session = nil
	}

	return nil
}

// Issue a query on an open connection, returning a RowSet, which
// can be later used to query the operation's status.
//func (c *Connection) Query(query string) (RowSet, error) {
//	executeReq := inf.NewTExecuteStatementReq()
//	executeReq.SessionHandle = c.session
//	executeReq.Statement = query
//	executeReq.RunAsync = true

//	resp, err := c.thrift.ExecuteStatement(executeReq)
//	if err != nil {
//		return nil, fmt.Errorf("Error in ExecuteStatement: %+v, %v", resp, err)
//	}

//	if !isSuccessStatus(resp.Status) {
//		return nil, fmt.Errorf("Error from server: %s", resp.Status.String())
//	}

//	log.Println("push query ok:", query)

//	return newRowSet(c.thrift, resp.OperationHandle, c.options), nil
//}

func (c *Connection) ExecMode(query string, isAsync bool) (*inf.TOperationHandle, error) {
	executeReq := inf.NewTExecuteStatementReq()
	executeReq.SessionHandle = c.session
	executeReq.Statement = query
	executeReq.RunAsync = isAsync
	executeReq.QueryTimeout = DefaultOptions.QueryTimeout

	resp, err := c.thrift.ExecuteStatement(executeReq)
	if err != nil {
		return nil, fmt.Errorf("Error in ExecuteStatement: %+v, %v", resp, err)
	}

	if !isSuccessStatus(resp.Status) {
		return nil, fmt.Errorf("Error from server: %s", resp.Status.String())
	}

	return resp.OperationHandle, err
}

func (c *Connection) Exec(query string) (*inf.TOperationHandle, error) {
	return c.ExecMode(query, false)
}

func isSuccessStatus(p *inf.TStatus) bool {
	status := p.GetStatusCode()
	return status == inf.TStatusCode_SUCCESS_STATUS || status == inf.TStatusCode_SUCCESS_WITH_INFO_STATUS
}

func (c *Connection) FetchOne(op *inf.TOperationHandle) (rows *inf.TRowSet, hasMoreRows bool, e error) {
	fetchReq := inf.NewTFetchResultsReq()
	fetchReq.OperationHandle = op
	fetchReq.Orientation = inf.TFetchOrientation_FETCH_NEXT
	fetchReq.MaxRows = DefaultOptions.BatchSize

	resp, err := c.thrift.FetchResults(fetchReq)
	if err != nil {
	    log.Printf("FetchResults failed: %v\n", err)
		return nil, false, err
	}

	if !isSuccessStatus(resp.Status) {
		log.Printf("FetchResults failed: %s\n", resp.Status.String())
		return nil, false, errors.New("FetchResult failed, status not ok: " + resp.Status.String())
	}

	rows = resp.GetResults() // return *TRowSet{StartRowOffset int64, Rows []*TRow, Columns []*TColumn, BinaryColumns []byte, ColumnCount *int32}

	//log.Println("the fetch rows=", rows)

	// for json debug
	//jret, jerr := json.Marshal(rows)
	//log.Println("json rows=", string(jret), jerr)

	// GetHasMoreRow()没生效，返回总是false
	return rows, resp.GetHasMoreRows(), nil
}

func (c *Connection) GetMetadata(op *inf.TOperationHandle) (*inf.TTableSchema, error) {
	req := inf.NewTGetResultSetMetadataReq()
	req.OperationHandle = op

	resp, err := c.thrift.GetResultSetMetadata(req)

	if err != nil {
		log.Println("GetMetadata failed:", err)
		return nil, err
	}

	schema := resp.GetSchema()

	// for json debug
	//jret, jerr := json.Marshal(schema)
	//log.Println("schema=", string(jret), jerr)

	return schema, nil
}

/*
	Simple Query
	1. Exec
	2. FetchResult
	3. GetMetadata
	4. Convert to map
*/
func (c *Connection) SimpleQuery(sql string) (rets []map[string]interface{}, err error) {
	operate, err := c.ExecMode(sql, true)
	if err != nil {
		return nil, err
	}

	// wait for ok
	status, err := c.WaitForOk(operate)
	if err != nil {
		log.Println("when waiting occur error:", err, " status=", status.String())
		return nil, err
	}

	schema, err := c.GetMetadata(operate)
	if err != nil {
		return nil, err
	}

	/*
		"columns": [
		       {
		           "i64Val": {
		               "values": [
		                   1,
		                   2
		               ],
		               "nulls": "AA=="
		           }
		       },
		       {
		           "stringVal": {
		               "values": [
		                   "f14581122165221",
		                   "t14581122175212"
		               ],
		               "nulls": "AA=="
		           }
		       },
			...
	*/

	// multiple fetch til all result have got
	var recvLen int
	for {
		var rowLen int

		rows, hasMore, err := c.FetchOne(operate)
		if rows == nil || err != nil {
			log.Println("the FetchResult is nil")
			return nil, err
		}

		var batchRets []map[string]interface{}
		batchRets, err = c.FormatRowsAsMap(rows, schema)

		rowLen = len(batchRets)
		rets = append(rets, batchRets...)

		recvLen += rowLen

		// hasMoreRow 没生效
		if !hasMore {
			log.Println("now more rows, this time rowlen=", rowLen, "StartRowOffset=", rows.StartRowOffset)
			//break
		} else {
			log.Println("has more rows, this time rowlen=", rowLen, "StartRowOffset=", rows.StartRowOffset)
		}
		// 需要从返回的数量里判断任务有没有进行完
		if rowLen <= 0 {
			log.Println("no more rows find, rowlen=", rowLen, "all got rowlen=", recvLen)
			break
		}
	}

	return rets, nil
}

// Issue a thrift call to check for the job's current status.
func (c *Connection) CheckStatus(operation *inf.TOperationHandle) (*Status, error) {
	req := inf.NewTGetOperationStatusReq()
	req.OperationHandle = operation

	//log.Println("will request GetOperationStatus")

	resp, err := c.thrift.GetOperationStatus(req)
	if err != nil {
		return nil, fmt.Errorf("Error getting status: %+v, %v", resp, err)
	}

	if !isSuccessStatus(resp.Status) {
		return nil, fmt.Errorf("GetStatus call failed: %s", resp.Status.String())
	}

	if resp.OperationState == nil {
		return nil, errors.New("No error from GetStatus, but nil status!")
	}

	//log.Println("OperationStatus", resp.GetOperationState(), "ProgressUpdate=", resp.GetProgressUpdateResponse())

	return &Status{resp.OperationState, nil, time.Now()}, nil
}

// Wait until the job is complete, one way or another, returning Status and error.
func (c *Connection) WaitForOk(operation *inf.TOperationHandle) (*Status, error) {
	for {
		status, err := c.CheckStatus(operation)

		if err != nil {
			return nil, err
		}

		if status.IsComplete() {
			if status.IsSuccess() {
				return status, nil
			}
			return nil, fmt.Errorf("Query failed execution: %s", status.state.String())
		}

		time.Sleep(time.Duration(DefaultOptions.PollIntervalSeconds) * time.Second)
	}
	return nil, errors.New("Cannot run here when wait for operation ok")
}

// Returns a string representation of operation status.
func (s Status) String() string {
	if s.state == nil {
		return "unknown"
	}
	return s.state.String()
}

// Returns true if the job has completed or failed.
func (s Status) IsComplete() bool {
	if s.state == nil {
		return false
	}

	switch *s.state {
	case inf.TOperationState_FINISHED_STATE,
		inf.TOperationState_CANCELED_STATE,
		inf.TOperationState_CLOSED_STATE,
		inf.TOperationState_ERROR_STATE:
		return true
	}

	return false
}

// Returns true if the job compelted successfully.

func (s Status) IsSuccess() bool {
	if s.state == nil {
		return false
	}

	return *s.state == inf.TOperationState_FINISHED_STATE
}

func DeserializeOp(handle []byte) (*inf.TOperationHandle, error) {
	ser := thrift.NewTDeserializer()
	var val inf.TOperationHandle

	if err := ser.Read(&val, handle); err != nil {
		return nil, err
	}

	return &val, nil
}

func SerializeOp(operation *inf.TOperationHandle) ([]byte, error) {
	ser := thrift.NewTSerializer()
	return ser.Write(operation)
}

/*
	将返回数据转换成map
*/
func (c *Connection) FormatRowsAsMap(rows *inf.TRowSet, schema *inf.TTableSchema) (rets []map[string]interface{}, err error) {
	var colValues = make(map[string]interface{}, 0)
	var rowLen int

	for cpos, tcol := range rows.Columns {
		// 此循环内遍历列名 取出所有列下的结果

		colName := schema.Columns[cpos].ColumnName

		switch true {
		case tcol.IsSetBinaryVal():
			colValues[colName] = tcol.GetBinaryVal().GetValues()
			rowLen = len(tcol.GetBinaryVal().GetValues())
		case tcol.IsSetBoolVal():
			colValues[colName] = tcol.GetBoolVal().GetValues()
			rowLen = len(tcol.GetBoolVal().GetValues())
		case tcol.IsSetByteVal():
			colValues[colName] = tcol.GetByteVal().GetValues()
			rowLen = len(tcol.GetByteVal().GetValues())
		case tcol.IsSetDoubleVal():
			colValues[colName] = tcol.GetDoubleVal().GetValues()
			rowLen = len(tcol.GetDoubleVal().GetValues())
		case tcol.IsSetI16Val():
			colValues[colName] = tcol.GetI16Val().GetValues()
			rowLen = len(tcol.GetI16Val().GetValues())
		case tcol.IsSetI32Val():
			colValues[colName] = tcol.GetI32Val().GetValues()
			rowLen = len(tcol.GetI32Val().GetValues())
		case tcol.IsSetI64Val():
			colValues[colName] = tcol.GetI64Val().GetValues()
			rowLen = len(tcol.GetI64Val().GetValues())
		case tcol.IsSetStringVal():
			colValues[colName] = tcol.GetStringVal().GetValues()
			rowLen = len(tcol.GetStringVal().GetValues())
		}
	}

	// 将列结构转换成行结构
	for i := 0; i < rowLen; i++ {
		formatedRow := make(map[string]interface{}, 0)
		for colName, colValueList := range colValues {
			// column => [v1, v2, v3, ...]
			formatedRow[colName] = reflect.ValueOf(colValueList).Index(i).Interface()
		}

		rets = append(rets, formatedRow)
	}

	return rets, nil
}

/*将hive返回的数据转换成内容行 不使用reflect 不占用两份内存*/
func (c *Connection) FormatRows(rows *inf.TRowSet, schema *inf.TTableSchema) (rets[][]string, err error) {
	//var colValues = make(map[string]interface{}, 0)
	var rowLen int
    var colLen = len(rows.Columns)
    //var retsMap []map[string]interface{}

    colNames := make([]string, 0)

	// 以下循环处理后 colValues=[col1=>[line1v1,line2v1,...], col2=>[line1v2,line2v2]]
    // 不知道rowlen,需要循环后才能知道
	for cpos, tcol := range rows.Columns {
		// 此循环内遍历列名 取出所有列下的结果

		colName := schema.Columns[cpos].ColumnName
        colNames = append(colNames, colName)


		switch true {
		case tcol.IsSetStringVal():
            //rowLen = c.convertColsToRows(cpos, colLen, tcol.GetStringVal().GetValues(), &rets)
            valuesOfCol := tcol.GetStringVal().GetValues()
			rowLen = len(valuesOfCol)
            if len(rets) == 0 {
                //log.Println("will remalloc rets", colName)
                rets = make([][]string, rowLen)
            }
            for i, oneCell := range valuesOfCol {
                if rets[i] == nil {
                    //log.Println("will remalloc rets[", i, "]->", "colName=", colName, "colType=",reflect.TypeOf(oneCell))
                    rets[i] = make([]string, colLen)
                }
                //log.Println("set rets[", i, "]->", cpos, colName, "=", oneCell, reflect.TypeOf(oneCell))
                rets[i][cpos] = fmt.Sprintf("%v",oneCell)
            }
		case tcol.IsSetBinaryVal():
			valuesOfCol := tcol.GetBinaryVal().GetValues()
			rowLen = len(valuesOfCol)
            if len(rets) == 0 {
                rets = make([][]string, rowLen)
            }
            for i, oneCell := range valuesOfCol {
                if rets[i] == nil {
                    rets[i] = make([]string, colLen)
                }
                rets[i][cpos] = fmt.Sprintf("%v",oneCell)
            }
		case tcol.IsSetBoolVal():
			valuesOfCol := tcol.GetBoolVal().GetValues()
			rowLen = len(valuesOfCol)//len(tcol.GetBoolVal().GetValues())
            if len(rets) == 0 {
                rets = make([][]string, rowLen)
            }
            for i, oneCell := range valuesOfCol {
                if rets[i] == nil {
                    rets[i] = make([]string, colLen)
                }
                rets[i][cpos] = fmt.Sprintf("%v",oneCell)
            }
		case tcol.IsSetByteVal():
			valuesOfCol := tcol.GetByteVal().GetValues()
			rowLen = len(valuesOfCol)//len(tcol.GetByteVal().GetValues())
            if len(rets) == 0 {
                rets = make([][]string, rowLen)
            }
            for i, oneCell := range valuesOfCol {
                if rets[i] == nil {
                    rets[i] = make([]string, colLen)
                }
                rets[i][cpos] = fmt.Sprintf("%v",oneCell)
            }
		case tcol.IsSetDoubleVal():
			valuesOfCol := tcol.GetDoubleVal().GetValues()
			rowLen = len(valuesOfCol)//len(tcol.GetDoubleVal().GetValues())
            if len(rets) == 0 {
                rets = make([][]string, rowLen)
            }
            for i, oneCell := range valuesOfCol {
                if rets[i] == nil {
                    rets[i] = make([]string, colLen)
                }
                rets[i][cpos] = fmt.Sprintf("%g",oneCell)
            }
		case tcol.IsSetI16Val():
		    valuesOfCol := tcol.GetI16Val().GetValues()
			rowLen = len(valuesOfCol)//len(tcol.GetI16Val().GetValues())
            if len(rets) == 0 {
                rets = make([][]string, rowLen)
            }
            for i, oneCell := range valuesOfCol {
                if rets[i] == nil {
                    rets[i] = make([]string, colLen)
                }
                rets[i][cpos] = fmt.Sprintf("%v",oneCell)
            }
		case tcol.IsSetI32Val():
			valuesOfCol := tcol.GetI32Val().GetValues()
			rowLen = len(valuesOfCol)//len(tcol.GetI32Val().GetValues())
            if len(rets) == 0 {
                rets = make([][]string, rowLen)
            }
            for i, oneCell := range valuesOfCol {
                if rets[i] == nil {
                    rets[i] = make([]string, colLen)
                }
                rets[i][cpos] = fmt.Sprintf("%v",oneCell)
            }
		case tcol.IsSetI64Val():
			valuesOfCol := tcol.GetI64Val().GetValues()
			rowLen = len(valuesOfCol)//len(tcol.GetI64Val().GetValues())
            if len(rets) == 0 {
                rets = make([][]string, rowLen)
            }
            for i, oneCell := range valuesOfCol {
                if rets[i] == nil {
                    rets[i] = make([]string, colLen)
                }
                rets[i][cpos] = fmt.Sprintf("%v",oneCell)
            }
		default:
			err = fmt.Errorf("the value is unsupported: %v", tcol)
			log.Println("when format rows:", err)
		}
	}

    log.Println("this fetch will format len=", rowLen, "rets=", rets)

    return rets, nil
/*
    rets = make([][]string, rowLen)

	// 将列结构转换成行结构
	for i := 0; i < rowLen; i++ {
		// 遍历列
        rets[i] = make([]string, len(retsMap[i]))

        for colNo, colName := range colNames {
            // 此处oneCellVal为乱序，从retsMap[i]中随机key取出
            rets[i][colNo] = fmt.Sprintf("%v", retsMap[i][colName])
        }

	}
*/
	return rets, nil
}

/*返回格式化后的表头*/
func (c *Connection) FormatHeads(schema *inf.TTableSchema) (outHead[]string, err error) {
	//colName := schema.Columns[cpos].ColumnName
	if schema == nil {
		err = fmt.Errorf("schema is nil shen FormatHeads")
		return
	}
	for _, col := range schema.Columns {
		outHead = append(outHead, col.GetColumnName())
	}
	return
}

func (c *Connection) GetOptions() *Options {
    return &c.options
}

