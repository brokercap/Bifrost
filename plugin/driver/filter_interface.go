package driver

// FilterPluginDriver 是可选接口。
// 插件实现后可在写入目标端前对事件做动态字段增删、改写或过滤。
type FilterPluginDriver interface {
	// keep=false 表示忽略当前事件；keep=true 表示继续同步。
	// 当 keep=true 时，newData 不能为空。
	Filter(data *PluginDataType, retry bool) (newData *PluginDataType, keep bool, err error)
}
