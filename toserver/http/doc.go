package http

func (MyConn *MyConn) GetDoc() string{
	var html string
	html = `
<p>不支持Type==list</p>
<p>以POST方式请求 配置的url 路径</p>
<p>
optype:$EventType
key:$key
data:$data
expir:$Expir
</p>
`
	return html
}
