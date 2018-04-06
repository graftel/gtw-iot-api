var shareUtil = require('./shareUtil.js');
var dataManage = require('./dataManage.js');
var levelup = require('levelup');
var leveldown = require('leveldown');
var cacheVar = shareUtil.cacheVar;
var math = require('mathjs');

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
          //console.log();
          //console.log("variableID =  " + data1.Items[0].VariableID);
          //console.log("reqBy = " + data1.Items[0].RequiredBy);
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
        //console.log("reqBy, eqInfo = " + JSON.stringify(data2, null, 2));
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
  cacheVar.get(variableID, function(err, value) {
    if (err) {
      console.log('get error' , err);
      var varidCache = {};
      varidCache.timestamp = timestamp;
      varidCache.Variables = {};
      varidCache.Variables[varidReq] = varValue;
      if (Object.keys(variables).length == 1) {     // if not here, var that needs only 1 var are not calculated before a cache for this var is made
        calculVariable(varidCache, equation, variables, variableID);
      }
    } else {
      var varidCache = JSON.parse(value);
      var diff = timestamp - varidCache.timestamp;
      if (Object.keys(variables).length == 1 || diff < timeout) {
        varidCache.timestamp = timestamp;
        varidCache.Variables[varidReq] = varValue;
        // calcul value of Var
        calculVariable(varidCache, equation, variables, variableID);
      } else {    // erase cache because it is too old
        varidCache.Variables = {};
        varidCache.timestamp = timestamp;
        varidCache.Variables[varidReq] = varValue;
      }
    }
    var varidCacheString = JSON.stringify(varidCache, null, 2);
    cacheVar.put(variableID, varidCacheString, function(err) {
      if (err) {
        console.log('put error', err);
      } else {
       console.log("varidCache updated");
      }
    });
  });
}

function calculVariable(varidCache, equation, variables, variableID) {
  if (varidCache.Variables.length == variables.length) {
    // do the calcul here, then push calculated data to the DB with addDataByVariableIDINternal
    parseEquation(varidCache, equation, variables, variableID, function(ret, data) {
      if (ret) {
        //console.log("data calculated = " + data);
        var timestamp = varidCache.Timestamp;
        var dataArray = [];
        dataArray[0] = {
          "VariableID" : variableID,
          "Value" : data
        }
        dataManage.addDataByVariableIDINternal(dataArray, timestamp, function(ret1, data1) {
          if (ret) {
            console.log(variableID + " = " + data + " pushed");
          } else {
            console.log("push of " + variableID + " failed");
          }
        });
      }
    });
  }
}

function parseEquation(varidCache, equation, variables, variableID, callback) {
  var scope = {};
  for (key in variables) {
    scope[key] = varidCache.Variables[variables[key]];
  }
  console.log(variableID + " : equation  = " + equation);
  //console.log("scope = " + JSON.stringify(scope, null, 2));
  var c = math.eval(equation, scope);
  callback(true, c);
  var eqTest = "avg(A, 60)";
  var index = eqTest.indexOf("avg");
  console.log("index = " + index);
  var eqTest2 = eqTest.slice(index+4);
  console.log("eqTest2 = " + eqTest2);
}
