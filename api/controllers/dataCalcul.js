var shareUtil = require('./shareUtil.js');
var levelup = require('levelup');
var leveldown = require('leveldown');
var cacheVar = shareUtil.cacheVar;

var functions = {
  triggerCalculData : triggerCalculData
}

for (var key in functions) {
  module.exports[key] = functions[key];
}


function triggerCalculData(data, index) {
  if (index < data.length) {
    var varid = data[index].PutRequest.Item.VariableID;
    var varValue = data[index].PutRequest.Item.Value;
    var timestamp = data[index].PutRequest.Item.EpochTimeStamp;
    var param = {
      TableName : shareUtil.tables.variable,
      KeyConditionExpression : "VariableID = :v1",
      ExpressionAttributeValues : {':v1' : varid},
      ProjectionExpression : "VariableID, RequiredBy"
    };
    shareUtil.awsclient.query(param, onQuery);
    function onQuery(err, data1) {
      if (err) {
        console.log(JSON.stringify(err, null, 2));
      } else {
        //console.log("data = " + JSON.stringify(data1, null, 2));
        triggerCalculData(data, index+1);
        if (data1.Items[0].RequiredBy && data1.Items[0].RequiredBy.length > 0) {
          getVariableEquationInfo(data1.Items[0].RequiredBy, data1.Items[0].VariableID, timestamp, varValue);
          console.log();
          console.log("variableID =  " + data1.Items[0].VariableID);
          console.log("reqBy = " + data1.Items[0].RequiredBy);
        }
      }
    }
  }
}

function getVariableEquationInfo(reqByVar, varidReq, timestamp, varValue) {
  for (key in reqByVar) {
    //console.log("data[key] = " + data[key]);
    var varid = reqByVar[key];
    var param = {
      TableName : shareUtil.tables.variable,
      KeyConditionExpression : "VariableID = :v1",
      ExpressionAttributeValues : {':v1' : varid},
      ProjectionExpression : "VariableID, EquationInfo"
    };
    shareUtil.awsclient.query(param, onQuery);
    function onQuery(err, data) {
      if (err) {
        console.log(JSON.stringify(err, null, 2));
      } else {
      //  console.log("reqBy, eqInfo = " + JSON.stringify(data2, null, 2));
        handleCalculation(data.Items[0], varidReq, timestamp, varValue);
      }
    }
  }
}

function handleCalculation(equationInfo, varidReq, timestamp, varValue) {
  var variableID = equationInfo.VariableID;
  var equation = equationInfo.EquationInfo.Equation;
  var timeout = equationInfo.EquationInfo.TimeoutInterval;
  var variables = equationInfo.EquationInfo.Variables;
  console.log();
  console.log("variableID = " + variableID);
  console.log("requires = " + varidReq);
  console.log("equation = " + equation);
  console.log("TimeoutInterval = " + timeout);
  console.log("variables = " + JSON.stringify(variables, null, 2));
  cacheVar.get(variableID, function(err, value) {
    if (err) {
      var varidCache = {};
      varidCache.timestamp = timestamp;
      varidCache.Variables = {};
      varidCache.Variables[varidReq] = varValue;
      var varidCacheString = JSON.stringify(varidCache, null, 2);
      console.log("varidCache = " + varidCacheString);
      cacheVar.put(variableID, varidCache, function(err) {
        if (err) {
          console.log('put error', err);
        } else {
          console.log("varidCache created");
        }
      });
    } else {
      var varidCache = JSON.stringify(value);
      console.log("varidCache = " + varidCache);
      if (timestamp - varidCache.timestamp < timeout) {
        // calcul value of Var
      } else {
        varidCache.timestamp = timestamp;
        varidCache.Variables = {};
      }
      varidCache.Variables[varidReq] = varValue;
      var varidCacheString = JSON.stringify(varidCache, null, 2);
      console.log("varidCache = " + varidCacheString);
      cacheVar.put(variableID, varidCache, function(err) {
        if (err) {
          console.log('put error', err);
        } else {
          console.log("varidCache created");
        }
      });
    }
  });
}
