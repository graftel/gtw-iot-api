var shareUtil = require('./shareUtil.js');

var functions = {
  triggerCalculData : triggerCalculData
}

for (var key in functions) {
  module.exports[key] = functions[key];
}


function triggerCalculData(data, index) {
  if (index < data.length) {
    var varid = data[index].PutRequest.Item.VariableID;
    var value = data[index].PutRequest.Item.Value;
    var timestamp = data[index].PutRequest.Item.EpochTimeStamp;
    var param = {
      TableName : shareUtil.tables.variable,
      KeyConditionExpression : "VariableID = :v1",
      ExpressionAttributeValues : {':v1' : varid},
      ProjectionExpression : "RequiredBy"
    };
    shareUtil.awsclient.query(param, onQuery);
    function onQuery(err, data1) {
      if (err) {
        console.log(JSON.stringify(err, null, 2));
      } else {
        console.log("data = " + JSON.stringify(data1, null, 2));
        triggerCalculData(data, index+1);
        if (data1.Items[0].RequiredBy && data1.Items[0].RequiredBy.length > 0) {
          calculRequiredData(data1.Items[0].RequiredBy);
        }
      }
    }
  }
}

function calculRequiredData(data) {
  for (key in data) {
    console.log("data[key] = " + data[key]);
    var varid = data[key];
    var param = {
      TableName : shareUtil.tables.variable,
      KeyConditionExpression : "VariableID = :v1",
      ExpressionAttributeValues : {':v1' : data[key]},
      ProjectionExpression : "RequiredBy, Equation"
    };
    shareUtil.awsclient.query(param, onQuery);
    function onQuery(err, data1) {
      if (err) {
        console.log(JSON.stringify(err, null, 2));
      } else {
        console.log("reqBy, eq = " + JSON.stringify(data1, null, 2));
      }
    }
  }
}
