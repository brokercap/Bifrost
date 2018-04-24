package kafka

func (MyConn *MyConn) GetDoc() string{
	var html string
	html = `
<p>数据可以是任何数据数据格式</p>
<p>key要求：topic[#Partition][#key]</p>
<p>Expir: 无效</p>
`
	return html
}

