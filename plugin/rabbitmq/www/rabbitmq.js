function transferBool(b){
	if(b=="true" || b == "1"){
		return true;
	}else{
		return false;
	}
}
function doGetPluginParam(){
	var result = {data:{},status:false,msg:"error",batchSupport:true}
    var data = {};
	var Queue = {};
	var Exhcange = {}

	var declare = transferBool($("#RabbitMQ_Declare").val());
	
	var queue_name = $("#RabbitMQ_Plugin_Contair #queue_name").val();
	var queue_durable = transferBool($("#RabbitMQ_Plugin_Contair #queue_durable").val());
	var queue_auto_delete = transferBool($("#RabbitMQ_Plugin_Contair #queue_auto_delete").val());
	
	var exchange_name = $("#RabbitMQ_Plugin_Contair #exchange_name").val();
	var exchange_type = $("#RabbitMQ_Plugin_Contair #exchange_type").val();
	var exchange_durable = transferBool($("#RabbitMQ_Plugin_Contair #exchange_durable").val());
	var exchange_auto_delete = transferBool($("#RabbitMQ_Plugin_Contair #exchange_auto_delete").val());
	
	var RoutingKey = $("#RabbitMQ_Plugin_Contair #RoutingKey").val();
	
	var Confirm = transferBool($("#RabbitMQ_Plugin_Contair #RabbitMQ_Confirm").val());
	var Persistent = transferBool($("#RabbitMQ_Plugin_Contair #RabbitMQ_Persistent").val());
	
	if(RoutingKey == ""){
		data.msg = "RoutingKey can't empty";
		return data;
	}
	
	if (declare){
		if(queue_name == "" || exchange_name == ""){
			data.msg = "Queue and Exchange can't empty";
			return data;
		}
	}
	
	var Expir = $("#RabbitMQ_Plugin_Contair input[name='Expir']").val();

    if (Expir != "" && Expir != null && isNaN(Expir)){
		result.msg = "Expir must be int!"
        return result;
    }

	Queue["Name"] = queue_name;
	Queue["Durable"] = queue_durable;
	Queue["AutoDelete"] = queue_auto_delete;
	
	Exhcange["Name"] = exchange_name;
	Exhcange["Type"] = exchange_type;
	Exhcange["Durable"] = exchange_durable;
	Exhcange["AutoDelete"] = exchange_auto_delete;
	
	data["Queue"] = Queue;
	data["Exchange"] = Exhcange;
	data["Confirm"] = Confirm;
	data["Persistent"] = Persistent;
	
    data["RoutingKey"] = RoutingKey;
    data["Expir"] = parseInt(Expir);
	data["Declare"] = declare;
	result.data = data;
	result.msg = "success";
	result.status = true;
    return result;
}

function RabbitMQ_Declare_Onchange(){
	if ($("#RabbitMQ_Declare").val() == "true"){
		$(".RabbitMQ_Declare_show_div").show();
	}else{
		$(".RabbitMQ_Declare_show_div").hide();
	}
}
RabbitMQ_Declare_Onchange();

setPluginParamDefault("FilterQuery",false);