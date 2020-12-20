function doGetPluginParam() {
    var result = {data: {}, status: false, msg: "error", batchSupport: true}
    var data = {}

    var type = $("#Redis_Plugin_Container #Plugin_type").val()
    var keyConfig = $("#Redis_Plugin_Container #KeyConfig").val()
    if (keyConfig === "") {
        result.msg = "Key must be not empty!"
        return result
    }
    data["KeyConfig"] = keyConfig

    if (type === "string") {
        var expir = $("#Redis_Plugin_Container input[name='expir']").val()
        if (!expir && isNaN(expir)) {
            result.msg = "expired must be int!"
            return result
        }
        data["expir"] = parseInt(expir)
    } else {
        delete data["expir"]
    }

    if (type === "hash") {
        var fieldKeyConfig = $("#Redis_FieldKeyConfigContainer #FieldKeyConfig").val()
        if (fieldKeyConfig === "") {
            result.msg = "Hash FieldKey must be not empty!"
            return result
        }
        data["FieldKeyConfig"] = fieldKeyConfig
    } else {
        delete data["FieldKeyConfig"]
    }

    if (type === "zset") {
        var sortedConfig = $("#Redis_SortConfigContainer #SortConfig").val()
        if (sortedConfig === "" ) {
            result.msg = "sorted sets sort must be not empty!"
            return result
        }
        data["SortedConfig"] = sortedConfig
    } else {
        delete data["SortedConfig"]
    }

    data["Type"] = type
    result.batchSupport = false
    result.data = data
    result.msg = "success"
    result.status = true
    return result
}

function redisTypeChange() {
    var type = $("#Plugin_type").val()
    if (type === "hash") {
        $("#Redis_FieldKeyConfigContainer").show()
    } else {
        $("#Redis_FieldKeyConfigContainer").hide()
    }

    if (type === "zset") {
        $("#Redis_SortConfigContainer").show()
    } else {
        $("#Redis_SortConfigContainer").hide()
    }

    if (type === "string") {
        $("#Redis_Expir").show()
    } else {
        $("#Redis_Expir").hide()
    }
}

redisTypeChange()
