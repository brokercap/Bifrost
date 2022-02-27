package src

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"log"

	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	elastic "github.com/olivere/elastic/v7"
)

const VERSION = "v1.8.1-beta"
const BIFROST_VERION = "v1.6.0"

func init() {
	pluginDriver.Register("Elasticsearch", NewConn, VERSION, BIFROST_VERION)
}

type Conn struct {
	pluginDriver.PluginDriverInterface
	Uri    *string
	status string
	client *elastic.Client

	err error
	p   *PluginParam

	esServerInfo *EsServer
}

type TableDataStruct struct {
	Data       []*pluginDriver.PluginDataType
	CommitData []*pluginDriver.PluginDataType // commit 提交的数据列表，Data 每 BatchSize 数据量划分为一个最后提交的commit
}

type PluginParam struct {
	EsIndexName          string          `json: "EsIndexName"`
	PrimaryKey           string          `json: "PrimaryKey"`
	Mapping              string          `json: "Mapping"`
	primaryKeys          []string        `json: "primaryKeys"`
	hadMapping           map[string]bool `json: "hadMapping"`
	BifrostMustBeSuccess bool            `json: "BifrostMustBeSuccess"` // bifrost server 保留,数据是否能丢
	BatchSize            int             `json: "BatchSize"`
	Data                 *TableDataStruct
	SkipBinlogData       *pluginDriver.PluginDataType // 在执行 skip 的时候 ，进行传入进来的时候需要要过滤的 位点，在每次commit之后，这个数据会被清空
}

type EsServer struct {
	User       string
	Password   string
	Urls       []string
	Sniff      bool
	Timeout    int
	RetryCount int
}

func NewConn() pluginDriver.Driver {
	f := &Conn{status: "close", err: fmt.Errorf("close")}
	return f
}

func (This *Conn) SetOption(uri *string, param map[string]interface{}) {
	This.Uri = uri
	return
}

func (This *Conn) Open() error {
	This.esServerInfo = This.getUriParam(*This.Uri)
	This.Connect()
	return nil
}

func (This *Conn) GetUriExample() string {
	return "http://localhost:9200?user=root&password=rootroot"
}

func (This *Conn) GetParam(p interface{}) (*PluginParam, error) {
	s, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	var param PluginParam
	err2 := json.Unmarshal(s, &param)
	if err2 != nil {
		return nil, err2
	}
	if param.EsIndexName == "" {
		return nil, fmt.Errorf("EsIndexName can't be empty")
	}
	param.primaryKeys = strings.Split(param.PrimaryKey, ",")
	param.hadMapping = map[string]bool{}
	param.Data = NewTableData()
	if param.BatchSize == 0 {
		param.BatchSize = 100 // 默认100
	}
	return &param, nil
}

func (This *Conn) SetParam(p interface{}) (interface{}, error) {
	if p == nil {
		return nil, fmt.Errorf("param is nil")
	}
	switch p.(type) {
	case *PluginParam:
		This.p = p.(*PluginParam)
		return p, nil
	default:
		param, _ := This.GetParam(p)
		This.p = param
		return param, nil
	}
}

func (This *Conn) CheckUri() error {
	var err error
	This.Connect()
	if This.err != nil {
		return This.err
	}
	_, err = This.GetVersion()
	return err
}

func (This *Conn) getUriParam(uri string) (EsServerInfo *EsServer) {
	EsServerInfo = &EsServer{}
	EsServerInfo.Urls = make([]string, 0)
	for _, httpUrl := range strings.Split(uri, ",") {
		if httpUrl == "" {
			continue
		}
		urlInfo, _ := url.Parse(httpUrl)
		auths := urlInfo.Query()
		if len(auths["user"]) > 0 {
			EsServerInfo.User = auths["user"][0]
		}
		if len(auths["password"]) > 0 {
			EsServerInfo.Password = auths["password"][0]
		}
		if len(auths["sniff"]) > 0 {
			if auths["sniff"][0] == "true" {
				EsServerInfo.Sniff = true
			}
		}
		if len(auths["timeout"]) > 0 {
			n, _ := strconv.Atoi(auths["timeout"][0])
			if n > 0 {
				EsServerInfo.Timeout = n
			}
		}
		if len(auths["retryCount"]) > 0 {
			n, _ := strconv.Atoi(auths["retryCount"][0])
			if n > 0 {
				EsServerInfo.RetryCount = n
			}
		}
		index := strings.Index(httpUrl, "?")
		if index > 0 {
			EsServerInfo.Urls = append(EsServerInfo.Urls, httpUrl[0:index])
		} else {
			EsServerInfo.Urls = append(EsServerInfo.Urls, httpUrl)
		}
	}
	if EsServerInfo.Timeout == 0 {
		EsServerInfo.Timeout = 10
	}
	if EsServerInfo.RetryCount == 0 {
		EsServerInfo.RetryCount = 3
	}
	return
}

func (This *Conn) Connect() bool {

	// This.Uri   http://127.0.0.1:9200?user=root&password=rootroot
	EsServerInfo := This.getUriParam(*This.Uri)
	options := []elastic.ClientOptionFunc{
		elastic.SetURL(EsServerInfo.Urls...),
		elastic.SetSniff(EsServerInfo.Sniff),
	}
	if EsServerInfo.User != "" {
		options = append(options, elastic.SetBasicAuth(EsServerInfo.User, EsServerInfo.Password))
	}

	options = append(options, elastic.SetHttpClient(&http.Client{
		Timeout: time.Duration(EsServerInfo.Timeout) * time.Second,
	}))

	client, err := elastic.NewClient(options...)
	if err != nil {
		This.err = err
		return false
	}
	This.esServerInfo = EsServerInfo
	This.client = client
	This.err = nil
	This.status = "running"

	return true
}

func (This *Conn) ReConnect() bool {
	defer func() {
		if err := recover(); err != nil {
			This.err = fmt.Errorf(fmt.Sprint(err))
		}
	}()
	This.Close()
	This.Connect()
	return true
}

func (This *Conn) Close() bool {
	func() {
		defer func() {
			if err := recover(); err != nil {
				return
			}
		}()
	}()
	This.status = "close"
	This.client = nil
	This.err = fmt.Errorf("close")
	return true
}

func (This *Conn) GetVersion() (Version string, err error) {

	if This.err != nil {
		This.Connect()
	}
	EsServerInfo := This.getUriParam(*This.Uri)
	Version, err = This.client.ElasticsearchVersion(EsServerInfo.Urls[0])
	return
}

func NewTableData() *TableDataStruct {
	CommitData := make([]*pluginDriver.PluginDataType, 0)
	CommitData = append(CommitData, nil)
	return &TableDataStruct{
		Data:       make([]*pluginDriver.PluginDataType, 0),
		CommitData: CommitData,
	}
}

// 假如没有配置指定 PrimaryKey (es 中的文档ID) 的时候，将 原表中的 Pri 主键当作 es 的文档ID
func (This *Conn) initPrimaryKeys(data *pluginDriver.PluginDataType) {
	if This.p.PrimaryKey == "" {
		This.p.primaryKeys = data.Pri
	}
}

func (This *Conn) doCreateMapping() {
	EsIndexName := This.p.EsIndexName
	if This.p.Mapping == "" {
		This.p.hadMapping[EsIndexName] = true
		return
	}
	if _, ok := This.p.hadMapping[EsIndexName]; !ok {
		resp, err := This.client.GetMapping().Index(EsIndexName).Do(context.Background())

		if err == nil && resp != nil {
			if _, ok := resp[EsIndexName]; ok { // hadMapping
				This.p.hadMapping[EsIndexName] = true
				return
			}
		}
		var mapping map[string]interface{}
		err = json.Unmarshal([]byte(This.p.Mapping), &mapping)
		if err == nil {
			This.client.PutMapping().Index(EsIndexName).BodyJson(mapping).Do(context.Background())
		} else {
			log.Printf("output[elasticsearch] doCreateMapping json.Unmarshal err: %s , mapping:%s", err.Error(), mapping)
		}
		This.p.hadMapping[EsIndexName] = true
	}
}

func (This *Conn) doCommit(list []*pluginDriver.PluginDataType, n int) (errData *pluginDriver.PluginDataType, err error) {

	if len(list) > 0 {
		This.p.EsIndexName = strings.ToLower(fmt.Sprint(pluginDriver.TransfeResult(This.p.EsIndexName, list[0], 0)))
	}

	//This.doCreateMapping()
	errData, err = This.commitNormal(list, n)
	return
}

// 合并数据，提交到es里
func (This *Conn) AutoCommit() (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, e error) {
	defer func() {
		if err := recover(); err != nil {
			e = fmt.Errorf(string(debug.Stack()))
			This.err = e
			// log.Println(" This.conn.Err:", This.conn.Err)
		}
	}()
	if This.err != nil {
		This.ReConnect()
	}
	if This.err != nil {
		log.Println(" This.Err:", This.err)
		return nil, nil, This.err
	}
	if This.err != nil {
		log.Println("This.err:", This.err)
	}
	n := len(This.p.Data.Data)
	if n == 0 {
		return nil, nil, nil
	}

	if n > This.p.BatchSize {
		n = This.p.BatchSize
	}
	list := This.p.Data.Data[:n]

	dataMap := make(map[string][]*pluginDriver.PluginDataType, 0)
	var ok bool
	for _, PluginData := range list {
		key := PluginData.SchemaName + "." + PluginData.TableName
		if _, ok = dataMap[key]; !ok {
			dataMap[key] = make([]*pluginDriver.PluginDataType, 0)
		}
		dataMap[key] = append(dataMap[key], PluginData)
	}
	for _, dataList := range dataMap {
		ErrData, e = This.doCommit(dataList, len(dataList))
		// 假如数据不能丢，才需要 判断 是否有err，如果可以丢，直接错过数据
		if e != nil {
			This.err = e
			if This.p.BifrostMustBeSuccess {
				return nil, ErrData, This.err
			}
			if This.CheckDataSkip(ErrData) {
				continue
			}
		}
	}
	This.err = e
	var binlogEvent *pluginDriver.PluginDataType
	if len(This.p.Data.Data) <= int(This.p.BatchSize) {
		// log.Println("This.p.Data:", g.Export(This.p.Data))

		binlogEvent = This.p.Data.CommitData[0]
		//log.Println("binlogEvent:",*binlogEvent)
		This.p.Data = NewTableData()
	} else {
		This.p.Data.Data = This.p.Data.Data[n:]
		if len(This.p.Data.CommitData) > 0 {
			binlogEvent = This.p.Data.CommitData[0]
			This.p.Data.CommitData = This.p.Data.CommitData[1:]
		}
	}
	This.p.SkipBinlogData = nil
	return binlogEvent, nil, nil
}

// 将数据放到 list 里,假如满足条件，则合并提交数据到es里
func (This *Conn) sendToCacheList(data *pluginDriver.PluginDataType, retry bool) (
	*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	var n int
	if retry == false {
		This.p.Data.Data = append(This.p.Data.Data, data)
	}
	n = len(This.p.Data.Data)

	if This.p.BatchSize <= n {
		return This.AutoCommit()
	}
	return nil, nil, nil
}

func (This *Conn) Insert(data *pluginDriver.PluginDataType, retry bool) (
	*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	This.initPrimaryKeys(data)
	if len(This.p.primaryKeys) == 0 {
		return nil, data, fmt.Errorf("PrimaryKey is empty And Table No Pri!")
	}

	return This.sendToCacheList(data, retry)
}

func (This *Conn) Update(data *pluginDriver.PluginDataType, retry bool) (
	*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	This.initPrimaryKeys(data)
	if len(This.p.primaryKeys) == 0 {
		return nil, data, fmt.Errorf("PrimaryKey is empty And Table No Pri!")
	}

	return This.sendToCacheList(data, retry)
}

func (This *Conn) Del(data *pluginDriver.PluginDataType, retry bool) (
	*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	This.initPrimaryKeys(data)
	if len(This.p.primaryKeys) == 0 {
		return nil, data, fmt.Errorf("PrimaryKey is empty And Table No Pri!")
	}

	return This.sendToCacheList(data, retry)
}

func (This *Conn) Query(data *pluginDriver.PluginDataType, retry bool) (
	*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return nil, nil, nil
}

func (This *Conn) Commit(data *pluginDriver.PluginDataType, retry bool) (
	*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	n := len(This.p.Data.Data)
	if n == 0 {
		return data, nil, nil
	}

	n0 := n / This.p.BatchSize
	if len(This.p.Data.CommitData)-1 < n0 {
		This.p.Data.CommitData = append(This.p.Data.CommitData, data)
	} else {
		This.p.Data.CommitData[n0] = data
	}
	return nil, nil, nil
}

func (This *Conn) TimeOutCommit() (
	*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.AutoCommit()
}

// 设置跳过的位点
func (This *Conn) Skip(SkipData *pluginDriver.PluginDataType) error {
	This.p.SkipBinlogData = SkipData
	return nil
}

func (This *Conn) CheckDataSkip(data *pluginDriver.PluginDataType) bool {
	if This.p.SkipBinlogData != nil && This.p.SkipBinlogData.BinlogFileNum == data.BinlogFileNum && This.p.SkipBinlogData.BinlogPosition == data.BinlogPosition {
		if This.p.SkipBinlogData.BinlogFileNum == data.BinlogFileNum && This.p.SkipBinlogData.BinlogPosition >= data.BinlogPosition {
			return true
		}
		if This.p.SkipBinlogData.BinlogFileNum > data.BinlogFileNum {
			return true
		}
	}
	return false
}
