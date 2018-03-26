
var shareUtil = require('./shareUtil.js');
var variableManage = require('./variableManage.js');
var deviceManage = require('./deviceManage.js');
/*
 Once you 'require' a module you can reference the things that it exports.  These are defined in module.exports.

 For a controller in a127 (which this is) you should export the functions referenced in your Swagger document by name.

 Either:
  - The HTTP Verb of the corresponding operation (get, put, post, delete, etc)
  - Or the operationId associated with the operation in your Swagger document

  In the starter/skeleton project the 'get' operation on the '/hello' path has an operationId named 'hello'.  Here,
  we specify that in the exports of this module that 'hello' maps to the function named 'hello'
 */
module.exports = {
  getSingleDataByVariableID: getSingleDataByVariableID,
  getMultipleDataByVariableID: getMultipleDataByVariableID,
  addDataByDeviceID: addDataByDeviceID,
  addDataByVariableID: addDataByVariableID
};


function fillDataArray(dataArray, timestamp, itemsToAddArray, index, callback){
  if (index < dataArray.length)
  {
    var variableid = dataArray[index].VariableID;
    var value = dataArray[index].Value;
    if (variableid)
    {
      if(value)
      {
        variableManage.IsVariableExist(variableid, function(ret, data) {
          if (ret)
          {
            if(timestamp)
            {
              var itemToAdd =
              {
                PutRequest : {
                  Item : {
                    "VariableID" : variableid,
                    "Value" : value,
                    "EpochTimeStamp" : timestamp
                  }
                }
              }
            } else // no timestamp provided
            {
              var itemToAdd =
              {
                PutRequest : {
                  Item : {
                    "VariableID" : variableid,
                    "Value" : value,
                    "EpochTimeStamp" : Math.floor((new Date).getTime()/1000)
                  }
                }
              }
            }
            itemsToAddArray.push(itemToAdd);
          } else
          {
            var msg = "VariableID: " + variableid + " does not exist";
            callback(false, msg);
          }
          fillDataArray(dataArray, timestamp, itemsToAddArray, index + 1, callback);
        });
      } else
      {
        var msg = "missing Value for VariableID: " + variableid + " (item number " + index + ")";
        callback(false, msg);
      }
    } else
    {
      var msg = "missing VariableID for item number " + index;
      callback(false, msg);
    }
  } else {
    callback(true, itemsToAddArray);
  }
}

function batchAddData(itemsToAddArray, callback) {
  var dataParams = {
    RequestItems : {
      "Hx.Data" : itemsToAddArray
    }
  }
  //console.log("dataParams = " + JSON.stringify(dataParams, null, 2));
  shareUtil.awsclient.batchWrite(dataParams, onPut);
  function onPut(err, data) {
    if (err)
    {
      console.log(JSON.stringify(dataParams, null, 2));
      var msg = "Error:" +  JSON.stringify(err, null, 2);
      console.error(msg);
      callback(false,msg);
    } else
    {
      console.log("write items succeeded !");
      callback(true, null);
    }
  }
}

function addDataByVariableID(req, res) {     // !! Hx.Data hardcoded !!

  console.log("pushData entered")
  var dataobj = req.body;
  var dataArray = dataobj.Data;
  var timestamp = dataobj.Timestamp;
  var itemsToAddArray = [];

  fillDataArray(dataArray, timestamp, itemsToAddArray, 0, function(ret, data){
    if(ret)
    {
      batchAddData(itemsToAddArray, function(ret, data) {
        if (ret) {
          shareUtil.SendSuccess(res);
        } else {
          shareUtil.SendInternalErr(res, data);
        }
      });
    } else
    {
      shareUtil.SendNotFound(res, data);
    }
  });
}


function addDataByDeviceIDOld(req, res) {

  var deviceid = req.swagger.params.DeviceID.value;
  var dataobj = req.body;
  var dataArrayLength = req.body.Data.length;

  console.log("timestamp = " + dataobj.Timestamp);
  console.log( "dataArrayLength = " + dataArrayLength);


  /*if (typeof dataobj.TimeStamp == "undefined"){
    var msg = "Timestamp undefined";
    shareUtil.SendInternalErr(res, msg);
  } else {*/

  if (dataArrayLength > 0) {    // check if dataArray is empty or not

    var typeofTmstp = typeof(dataobj.Timestamp);
    console.log("type = " + typeofTmstp);

    console.log("function entered");

    addSingleData(deviceid, dataobj, 0, function() {
      shareUtil.SendSuccess(res);
    });
  }
  else
  {
    var msg = "Data[] and/or TImestamp empty";
    shareUtil.SendInternalErr(res, msg);
  }
}

function addDataByDeviceID(req, res) {
  var deviceid = req.swagger.params.DeviceID.value;
  var dataobj = req.body;
  var data = dataobj.Data;
  var timestamp = dataobj.Timestamp;
  if (!timestamp) {
    timestamp = Math.floor((new Date).getTime()/1000);
    console.log("timestamp = " + timestamp);
  }

  if(deviceid){
    deviceManage.getVariablesFromDevice(deviceid, function(ret1, data1){
      if (ret1) {
        var variableidList = data1.Variables;
        var getItems = [];
        console.log("variableidList = " + variableidList);
        batchGetItem(variableidList, getItems, function(ret2, data2){
          if(ret2){
            var varIDtoNameMap = data2.Responses["Hx.Variable"];
            console.log("varIDtoNameMap = " + JSON.stringify(varIDtoNameMap, null, 2));
            var dataObj = {};
            convertDataArrToObj(data.Data, dataObj, 0, function(ret3, data3) {
              if (ret3) {
                var varObj = {};
                convertVarIDtoVarNameArrayIntoObj(varIDtoNameMap, varObj, 0, function(ret7, data7) {
                  if (ret7) {
                    var valueToVarIDMap = [];
                    mapValueToVarID(data3, data7, valueToVarIDMap, 0, deviceid, function(ret4, data4) {
                      if (ret4) {
                        console.log("data4 = " + JSON.stringify(data4, null, 2));
                        var itemsToAddArray = [];
                        fillDataArray(data4, timestamp, itemsToAddArray, 0, function(ret5, data5) {
                          if (ret5) {
                            console.log("data5 = " + JSON.stringify(data5, null, 2));
                            batchAddData(data5, function(ret6, data6) {
                              if (ret6) {
                                shareUtil.SendSuccess(res);
                              } else {
                                shareUtil.SendInternalErr(res, data6);
                              }
                            });
                          } else {
                            shareUtil.SendInternalErr(res);
                          }
                        });
                      } else {
                        shareUtil.SendInternalErr(res, data4);
                      }
                    });
                  } else {
                    shareUtil.SendInternalErr(res);
                  }
                })
              } else {
                shareUtil.SendInvalidInput(res);
              }
            });
          } else {
            shareUtil.SendNotFound(res, data2);
          }
        });
      } else { // no Variables found in Device
        shareUtil.SendNotFound(res);
      }
    });
  } else {
      var msg = "DeviceID missing";
      shareUtil.SendInvalidInput(res, msg);
  }
}


function mapValueToVarID(varNameToValueMap, varIDtoNameMap, valueToVarIDMap, index, deviceid, callback) {
  //  console.log("varNameToValueMap = " + JSON.stringify(varNameToValueMap, null, 2));
  //  console.log(" length = " + Object.keys(varNameToValueMap).length);
  if (index < Object.keys(varNameToValueMap).length) {
    var varName = Object.keys(varNameToValueMap)[index];
    var varValue = varNameToValueMap[varName];
    var item = {};
    var indexOfName = Object.values(varIDtoNameMap).indexOf(varName);
    if (indexOfName > -1) {
      item.VariableID = Object.keys(varIDtoNameMap)[indexOfName];
      console.log(item.VariableID);
      item.Value = varValue;
      valueToVarIDMap.push(item);
      mapValueToVarID(varNameToValueMap, varIDtoNameMap, valueToVarIDMap, index+1, deviceid, callback);
    } else {
      //create a new varid
      var uuidv1 = require('uuid/v1');
      var variableID = uuidv1();
      createNewVariableFromName(varName, variableID, deviceid, function(ret, data){
        item.VariableID = variableID;
        console.log(item.VariableID);
        item.Value = varValue;
        valueToVarIDMap.push(item);
        mapValueToVarID(varNameToValueMap, varIDtoNameMap, valueToVarIDMap, index+1, deviceid, callback);
      });
    }
  } else {
    callback(true, valueToVarIDMap);
    console.log("valueToVarIDMap = " + JSON.stringify(valueToVarIDMap));
  }
}

function createNewVariableFromName(varName, varID, deviceid, callback) {

  var params = {
    TableName : shareUtil.tables.variable,
    Item : {
      VariableID: varID,
      AddTimeStamp: Math.floor((new Date).getTime()/1000),
      VariableName: varName
    },
    ConditionExpression : "attribute_not_exists(VariableID)"
  };
  shareUtil.awsclient.put(params, function(err, data) {
    if (err) {
      var msg = "Error:" + JSON.stringify(err, null, 2);
      callback(false, msg);
    } else {
        updateVariableIDInDevice(varID, deviceid, function(ret1, data1){
        if (ret1)
        {
          console.log("var created !");
          callback(true, null);
        } else
        {
          var msg = "Error:" + JSON.stringify(data1) + "update failed";
          callback(false, msg);
        }
      });
    }
  });
}

function convertVarIDtoVarNameArrayIntoObj(varIDtoNameMap, varObj, index, callback) {
  if( index < varIDtoNameMap.length) {
    varid = varIDtoNameMap[index].VariableID;
    //varName = varIDtoNameMap[index].VariableName;
    varName = Object.values(varIDtoNameMap[index])[1];
    varObj[varid] = varName;
    convertVarIDtoVarNameArrayIntoObj(varIDtoNameMap, varObj, index + 1, callback);
  } else {
    //    console.log("varIDtoNameMapconverted = " + JSON.stringify(varObj, null, 2));
    callback(true, varObj);
  }
}

function convertDataArrToObj(dataArray, dataObj, index, callback) {

  if (index < dataArray.length) {
    //var dataSorted = {};
    var key = Object.keys(dataArray[index]);
    var value2 = Object.values(dataArray[index]);
    dataObj[key] = value2[0];
    //console.log("dataSorted = " + JSON.stringify(dataObj, null, 2));
    convertDataArrToObj(dataArray, dataObj, index + 1, callback);
  } else {
    callback(true, dataObj);
  //  console.log(" dataObj = " +  JSON.stringify(dataObj, null, 2));
  }
}


function batchGetItem(variableidList, getItems, callback){
  fillBatchGetItem(variableidList, getItems, 0, function(ret, data) {
    if (ret) {
      var dataParams = {
        RequestItems : {
          "Hx.Variable" : {
            Keys : data,
            ProjectionExpression : "VariableID, VariableName"
          }
        }
      }
      shareUtil.awsclient.batchGet(dataParams, onGet);
      function onGet(err, data1) {
        if (err) {
          var msg = "Error:" +  JSON.stringify(err, null, 2);
          callback(false, msg);
        } else {
          callback(true, data1);
        }
      }
    } else {
      callback(false, data);
    }
  });
}

function fillBatchGetItem(variableidList, getItems, index, callback) {
  if (index < variableidList.length){
    var getItem = {
      "VariableID" : variableidList[index]
    }
    getItems.push(getItem);
    //console.log("getItem = " + JSON.stringify(getItem, null, 2));
    fillBatchGetItem(variableidList, getItems, index+1, callback);
  } else {
    callback(true, getItems);
  //  console.log("getItems = " + JSON.stringify(getItems, null, 2));
  }
}

function addSingleData(deviceid, dataobj, index, callback) {

  if (index < dataobj.Data.length)
  {
    var variableName = dataobj.Data[index].VariableName;
    console.log("variableName = " + variableName);
    var nameParams = {
      TableName: shareUtil.tables.variable,
      FilterExpression : "VariableName = :v1",
      ExpressionAttributeValues : {':v1' : variableName},
      ProjectionExpression : "VariableID"
    }
    shareUtil.awsclient.scan(nameParams, onScan);
    function onScan(err, data) {

      if (err)
      {
        var msg = "Error:" + JSON.stringify(err, null, 2);
        shareUtil.SendInternalErr(res, msg);
      } else
      {
        if (data.Count == 0)
        {
          var msg = "data.Count == 0"
          //shareUtil.SendNotFound(res, msg);
          // have to create a new var name
          createNewVariable(deviceid, dataobj, index, function(){
            addSingleData(deviceid, dataobj, index+1, callback);
          });
        } else
        {
          // varName found so add Value to RawData
          var variableid = data.Items[0].VariableID;
          console.log("variableid = " + variableid);
          var timestamp = dataobj.Timestamp;
          var value = dataobj.Data[index].Value;

          var dataParams = {
            TableName : shareUtil.tables.data,
            Item : {
              VariableID : variableid,
              EpochTimeStamp : timestamp,
              Value : value
            }
          }
          console.log("dataParams = "  + JSON.stringify(dataParams, null, 2));

          shareUtil.awsclient.put(dataParams, onPut);
          function onPut(err, data)
          {
            if (err)
            {
              var msg = "Error:" + JSON.stringify(err, null, 2);
              shareUtil.SendInternalErr(res, msg);
            } else
            {
              //shareUtil.SendSuccess(res);
              //update current value in Variable table
              var updateVarParams = {
                TableName : shareUtil.tables.variable,
                Key : {
                  VariableID: variableid
                },
                UpdateExpression : "set CurrentValue = :v1",
                ExpressionAttributeValues : {':v1' : value}
              }

              shareUtil.awsclient.update(updateVarParams, function(err, data) {
                if (err)
                {
                  var msg = "Unable to update the settings table.( POST /settings) Error JSON:" +  JSON.stringify(err, null, 2);
                  console.error(msg);
                } else
                {
                   console.log("device updated!");
                   addSingleData(deviceid, dataobj, index+1, callback);
                }
              });
            }
          //  addSingleData(deviceid, dataobj, index+1, callback);
          }
        }
      }
    }
  } else
  {
    return callback();
  }
}


function createNewVariable(deviceid, dataobj, index, callback) {

  var uuidv1 = require('uuid/v1');
  var crypto = require('crypto');
  var variableid = uuidv1();
  var variableName = dataobj.Data[index].VariableName;
  var timestamp = dataobj.Timestamp;
  var variableValue = dataobj.Data[index].Value;

  var params = {
    TableName : shareUtil.tables.variable,
    Item : {
      VariableID: variableid,
      AddTimeStamp: timestamp,
      VariableName: variableName,
      CurrentValue: variableValue
    },
    ConditionExpression :  "attribute_not_exists(VariableID)"
  };

  shareUtil.awsclient.put(params, onPut);
  function onPut(err, data) {
    if (err)
    {
      var msg = "Error:" + JSON.stringify(err, null, 2);
      shareUtil.SendInternalErr(res,msg);
    } else
    {
      updateVariableIDInDevice(variableid, deviceid, function(ret1, data){
        if (ret1)
        {
          //shareUtil.SendSuccess(res);
          console.log("timestamp in update = " + timestamp);
          addRawData(variableid, timestamp, variableValue, function() {
            addSingleData(deviceid, dataobj, index, callback);
          });
        }
        else{
          var msg = "Error:" + JSON.stringify(data);
          shareUtil.SendInternalErr(res,msg);
        }
       });
    }
  }
}


function updateVariableIDInDevice(variableID, deviceID, callback) {
  if(!deviceID)
  {
    return callback(false, null);
  }
  else {
    var updateParams = {
      TableName : shareUtil.tables.device,
      Key : {
        DeviceID : deviceID,
      },
      UpdateExpression : 'set #variable = list_append(if_not_exists(#variable, :empty_list), :id)',
      ExpressionAttributeNames: {
        '#variable': 'Variables'
      },
      ExpressionAttributeValues: {
        ':id': [variableID],
        ':empty_list': []
      }
    };

    shareUtil.awsclient.update(updateParams, function (err, data) {
      if (err) {
        var msg = "Error:" +  JSON.stringify(err, null, 2);
        console.error(msg);
        callback(false,msg);
      } else
      {
        return callback(true,null);
      }
    });
  }
}


function addRawData(variableid, timestamp, value, callback) {

  var dataParams = {
    TableName : shareUtil.tables.data,
    Item : {
      VariableID : variableid,
      EpochTimeStamp : timestamp,
      Value : value
    }
  }
  console.log("dataParams = "  + JSON.stringify(dataParams, null, 2));
  console.log("timestamp = " + timestamp);

  shareUtil.awsclient.put(dataParams, onPut);
  function onPut(err, data)
  {
    if (err)
    {
      var msg = "Error:" + JSON.stringify(err, null, 2);
      console.log(msg);
      //shareUtil.SendInternalErr(res, msg);
    } else
    {
      //shareUtil.SendSuccess(res);
      return callback();
    }
  }
}

function getSingleDataByVariableID(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var variableID = req.swagger.params.VariableID.value;
  var dataTimeStamp = req.swagger.params.TimeStamp.value;

   var params = {
     TableName: shareUtil.tables.data,
     KeyConditionExpression : "VariableID = :v1 and EpochTimeStamp = :v2",
     ExpressionAttributeValues : {':v1' : variableID.toString(),
                                  ':v2' : dataTimeStamp}
   };
   console.log(params)
   shareUtil.awsclient.query(params, function(err, data) {
   if (err) {
     var msg = "Error:" + JSON.stringify(err, null, 2);
     console.error(msg);
     shareUtil.SendInternalErr(res,msg);
   }else{
     console.log(data);
     if (data.Count == 0)
     {
       var msg = "Error: Cannot find data"
        shareUtil.SendInvalidInput(res, msg);
     }
     else if (data.Count == 1)
     {
        var out_data = {'Value' : data.Items[0]["Value"]};
        console.log(out_data);
        shareUtil.SendSuccessWithData(res, out_data);
     }
     else {
       var msg = "Error: data count is not 1"
        shareUtil.SendInternalErr(res,msg);
     }

   }
   });
  // this sends back a JSON response which is a single string

}

function getSingleCalculatedData(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var assetID = req.swagger.params.AssetID.value;
  var dataTimeStamp = req.swagger.params.TimeStamp.value;

  var params = {
    TableName: tables.calculatedData,
    KeyConditionExpression : "AssetID = :v1 and EpochTimeStamp = :v2",
    ExpressionAttributeValues : {':v1' : assetID.toString(),
                                ':v2' : dataTimeStamp}
  };
  console.log(params)
  docClient.query(params, function(err, data) {
  if (err)
  {
    var msg = "Error:" + JSON.stringify(err, null, 2);
    console.error(msg);
    shareUtil.SendInternalErr(res,msg);
  } else
  {
    console.log(data);
    if (data.Count == 0)
    {
      var msg = "Error: Cannot find data"
      shareUtil.SendInvalidInput(res,NOT_EXIST);
    }
    else if (data.Count == 1)
    {
      shareUtil.SendSuccessWithData(res, data.Items[0]);
    }
    else
    {
      var msg = "Error: data count is not 1"
      shareUtil.SendInternalErr(res,msg);
     }
    }
  });
}

function getMultipleDataByVariableID(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var variableID = req.swagger.params.VariableID.value;
  var dataTimeStampFrom = req.swagger.params.StartTimeStamp.value;
  var dataTimeStampTo = req.swagger.params.EndTimeStamp.value;

   var params = {
     TableName: shareUtil.tables.data,
     KeyConditionExpression : "VariableID = :v1 and EpochTimeStamp between :v2 and :v3",
     ExpressionAttributeValues : {':v1' : variableID.toString(),
                                  ':v2' : dataTimeStampFrom,
                                  ':v3' : dataTimeStampTo}
   };
   console.log(params)
   shareUtil.awsclient.query(params, function(err, data) {
   if (err) {
     var msg = "Error:" + JSON.stringify(err, null, 2);
     console.error(msg);
     shareUtil.SendInternalErr(res,msg);
   }else{
     console.log(data);
     if (data.Count == 0)
     {
      var msg = "Error: Cannot find data"
      shareUtil.SendInvalidInput(res, msg);
     }
     else {
        delete data["ScannedCount"];
        shareUtil.SendSuccessWithData(res, data);
     }
   }
 });
}


function getMultipleCalculatedData(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var assetID = req.swagger.params.AssetID.value;
  var dataTimeStampFrom = req.swagger.params.StartTimeStamp.value;
  var dataTimeStampTo = req.swagger.params.EndTimeStamp.value;

   var params = {
     TableName: tables.calculatedData,
     KeyConditionExpression : "AssetID = :v1 and EpochTimeStamp between :v2 and :v3",
     ExpressionAttributeValues : {':v1' : assetID.toString(),
                                  ':v2' : dataTimeStampFrom,
                                  ':v3' : dataTimeStampTo}
   };
   docClient.query(params, function(err, data) {
   if (err) {
     var msg = "Error:" + JSON.stringify(err, null, 2);
     console.error(msg);
     shareUtil.SendInternalErr(res,msg);
   }else{
     console.log(data);
     if (data.Count == 0)
     {
       var msg = "Error: Cannot find data"
        shareUtil.SendInvalidInput(res,NOT_EXIST);
     }
     else {
        delete data["ScannedCount"];
        shareUtil.SendSuccessWithData(res, data);
     }

   }
 });
  // this sends back a JSON response which is a single string
}

function getMultipleCalculatedDataWithParameter(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var assetID = req.swagger.params.AssetID.value;
  var dataTimeStampFrom = req.swagger.params.StartTimeStamp.value;
  var dataTimeStampTo = req.swagger.params.EndTimeStamp.value;
  var parameterid = req.swagger.params.ParameterID.value;

   var params = {
     TableName: tables.calculatedData,
     KeyConditionExpression : "AssetID = :v1 and EpochTimeStamp between :v2 and :v3",
     ExpressionAttributeValues : {':v1' : assetID.toString(),
                                  ':v2' : dataTimeStampFrom,
                                  ':v3' : dataTimeStampTo
                                }
   };
   console.log(params);
   docClient.query(params, function(err, data) {
   if (err) {
     var msg = "Error:" + JSON.stringify(err, null, 2);
     console.error(msg);
     shareUtil.SendInternalErr(res,msg);
   }else{
     if (data.Count == 0)
     {
       var msg = "Error: Cannot find data"
        shareUtil.SendInvalidInput(res,NOT_EXIST);
     }
     else {
        console.log(data);
        delete data["ScannedCount"];
        var out_data = {};
        out_data['values'] = [];
        out_data['timestamp'] = [];
        out_data['parameter'] = parameterid;
        //out_data['count'] = data.Count;
        for (var i in data.Items)
        {
          var singleData = data.Items[i];
          for (var j in singleData.Data)
          {
            if (singleData.Data[j].ParamID == parameterid)
            {
              out_data['timestamp'].push(singleData['EpochTimeStamp']);
              out_data['values'].push(singleData.Data[j].Value);
            }
          }
          out_data['count'] = out_data['timestamp'].length;
        }
        if (out_data['count'] == 0){
          shareUtil.SendInvalidInput(res,NOT_EXIST);
        } else
        {
          shareUtil.SendSuccessWithData(res, out_data);
        }
     }

   }
 });
  // this sends back a JSON response which is a single string
}
