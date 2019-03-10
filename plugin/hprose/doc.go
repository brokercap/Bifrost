package hprose

func (MyConn *MyConn) GetDoc() string{
	var html string
	html = `
<p>数据必须是json格式</p>
<p>key要求：请按自定义规则</p>
<p>Expir: 请根据自定义hprose server 而定</p>
`
	return html
}

