package activemq

func (MyConn *MyConn) GetDoc() string{
	var html string
	html = `
<p>数据可以是任何数据数据格式</p>
<p>key要求：queue_name#true|false</p>
<p>Expir: 有效</p>
<p>备注：MustBeSuccess 选择为Yes的时候,当前数据写到了内核就返回成功，并未等待服务器返回成功</p>
`
	return html
}

