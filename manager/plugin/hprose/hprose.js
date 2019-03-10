function doGetParam(){
    var data = {};
    var addToServerType = $("#addToServerType").val();
    var addToServerDataType = $("#addToServerDataType").val();
    var KeyConfig = $("#KeyConfig").val();
    var ValueConfig = $("#ValueConfig").val();
    if (KeyConfig==""){
        alert("Key must be not empty!");
        return false;
    }
    if (addToServerDataType == "string" && ValueConfig==""){
        alert("DataType==string,ValueConfig muest be!");
        return false;
    }
    data["addToServerType"] = addToServerType;
    data["addToServerDataType"] = addToServerDataType;
    data["KeyConfig"] = KeyConfig;
    data["ValueConfig"] = ValueConfig;
    return data;
}