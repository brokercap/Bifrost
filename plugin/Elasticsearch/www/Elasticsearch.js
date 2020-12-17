function doGetPluginParam() {
  var result = { data: {}, status: false, msg: "error", batchSupport: true };
  var data = {};

  var EsIndexName = $("#Elasticsearch_IndexName").val();
  var PrimaryKey = $("#Elasticsearch_PrimaryKey").val();
  var BatchSize = $("#ES_BatchSize").val();

  if (EsIndexName == "") {
    result.msg = "EsIndexName can't be empty!";
    return result;
  }
  if (BatchSize != "" && BatchSize != null && isNaN(BatchSize)) {
    result.msg = "BatchSize must be int!";
    return result;
  }
  data["EsIndexName"] = EsIndexName;
  data["PrimaryKey"] = PrimaryKey;
  data["BatchSize"] = parseInt(BatchSize);

  result.data = data;
  result.msg = "success";
  result.status = true;
  return result;
}
