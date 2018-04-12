package rabbitmq

func (MyConn *MyConn) GetDoc() string{
	var html string
	html = `
<p>不支持type==set的数据格式</p>
<p>当选择dataType == string的时候,Value 数据必须配置成json格式的字符串,否则不能提交</p>
<p>key要求：routingkey[-exchange][-DeliveryMode],队列名需要事先创建并且和交换机与路由绑定</p>
<p>DeliveryMode:是否持久化，0非持久化，1持久化，默认为1</p>
<p>如果数据有可能删除的,请选择AddEventType ,并且在数据处理的时候,要对AddEventType进行判断处理</p>
<p>MustBeSuccess 选择为Yes的时候,并不代表数据一定到了RabbitMQ服务器,只保证提交数据的时候,连接是正常的,当前并没有采用confirm机制
<p>Expir 有效</p>
`
	return html
}
