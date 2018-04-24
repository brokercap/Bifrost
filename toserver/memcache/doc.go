package memcache

func (MyConn *MyConn) GetDoc() string{
	var html string
	html = `
<p>数据可以是任何数据数据格式</p>
<p>Expir: set 方式下生效</p>
`
	return html
}

