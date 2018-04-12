package mongodb

func (MyConn *MyConn) GetDoc() string{
	var html string
	html = `
<p>Type == set 的时候,Key 数据格式要求:{$SchemaName}-{$TableName}-PrimaryField-{$PrimaryField}</p>
<p>Type == list 的时候,Key 数据格式要求:{$SchemaName}-{$TableName}</p>
<p>每条消息都会自动新加 createdAt 字段</p>
<p>Expir 过期设置，需要手工去mongodb 中创建 createdAt 索引</p>
`
	return html
}
