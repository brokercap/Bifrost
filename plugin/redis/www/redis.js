function doGetPluginParam() {
    let result = {data: {}, status: false, msg: "error", batchSupport: true}
    let data = {};

    let type = $("#Redis_Plugin_Container #Plugin_type").val();
    let keyConfig = $("#Redis_Plugin_Container #KeyConfig").val();
    let fieldKeyConfig = $("#Redis_FieldKeyConfigContainer #FieldKeyConfig").val();
    let sortedConfig = $("#Redis_SortConfigContainer #SortConfig").val();
    if (keyConfig === "") {
        result.msg = "Key must be not empty!"
        return result
    }
    if (fieldKeyConfig === "" && type === "hash") {
        result.msg = "Hash FieldKey must be not empty!"
        return result
    }
    if (sortedConfig === "" && type === "zset") {
        result.msg = "sorted sets sort must be not empty!"
        return result
    }

    let Expir = $("#Redis_Plugin_Container input[name='Expir']").val();

    if (!Expir && isNaN(Expir)) {
        result.msg = "expired must be int!"
        return result;
    }

    data["KeyConfig"] = keyConfig;
    data["FieldKeyConfig"] = fieldKeyConfig;
    data["SortedConfig"] = sortedConfig;
    data["Expir"] = parseInt(Expir);
    data["Type"] = type;
    result.batchSupport = true;
    result.data = data;
    result.msg = "success";
    result.status = true;
    return result;
}

function redisTypeChange() {
    let type = $("#Plugin_type").val();
    if (type === "hash") {
        $("#Redis_FieldKeyConfigContainer").show();
    } else {
        $("#Redis_FieldKeyConfigContainer").hide();
    }

    if (type === "zset") {
        $("#Redis_SortConfigContainer").show();
    } else {
        $("#Redis_SortConfigContainer").hide();
    }
}

redisTypeChange()