var util = require('util');
var shareUtil = require('./shareUtil.js');
var AWS = require("aws-sdk");
const os = require('os');
AWS.config.loadFromPath(os.homedir() + '/.aws/config.json');
var docClient = new AWS.DynamoDB.DocumentClient();

const INVALID_INPUT = "Invalid Input";
const ALREADY_EXIST = "Item Already Exist";
const SUCCESS_MSG = "Success";
const NOT_EXIST = "Item Not Exist";

var tables = {
    company: "Hx.Company",
    users: "Hx.Users",
    assets: "Hx.Assets",
    deviceConfig: "Hx.DeviceConfiguration",
    rawData: "Hx.RawData",
    calculatedData: "Hx.CalculatedData",
    alerts: "Hx.Alerts",
    settings: "Hx.Settings"
};
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
  getSingleData: getSingleData,
  getSingleCalculatedData: getSingleCalculatedData,
  getMultipleData: getMultipleData,
  getMultipleCalculatedData: getMultipleCalculatedData,
  getMultipleCalculatedDataWithParameter: getMultipleCalculatedDataWithParameter,
  addDataByDeviceID: addDataByDeviceID
};


function addDataByDeviceID(req, res) {

  var deviceid = req.swagger.params.DeviceID.value;
  var dataobj = req.body;
  var dataArrayLength = req.body.Data.length;

  console.log("timestamp = " + dataobj.TimeStamp);
  console.log( "dataArrayLength = " + dataArrayLength);


  /*if (typeof dataobj.TimeStamp == "undefined"){
    var msg = "Timestamp undefined";
    shareUtil.SendInternalErr(res, msg);
  } else {*/

  if (dataArrayLength > 0) {    // check if dataArray is empty or not

    var typeofTmstp = typeof(dataobj.TimeStamp);
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
            TableName : shareUtil.tables.rawData,
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
  var timestamp = dataobj.TimeStamp;
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
    TableName : shareUtil.tables.rawData,
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



function getSingleData(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var variableID = req.swagger.params.VariableID.value;
  var dataTimeStamp = req.swagger.params.TimeStamp.value;

   var params = {
     TableName: tables.rawData,
     KeyConditionExpression : "VariableID = :v1 and EpochTimeStamp = :v2",
     ExpressionAttributeValues : {':v1' : variableID.toString(),
                                  ':v2' : dataTimeStamp}
   };
   console.log(params)
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



function getMultipleData(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var variableID = req.swagger.params.VariableID.value;
  var dataTimeStampFrom = req.swagger.params.StartTimeStamp.value;
  var dataTimeStampTo = req.swagger.params.EndTimeStamp.value;

   var params = {
     TableName: tables.rawData,
     KeyConditionExpression : "VariableID = :v1 and EpochTimeStamp between :v2 and :v3",
     ExpressionAttributeValues : {':v1' : variableID.toString(),
                                  ':v2' : dataTimeStampFrom,
                                  ':v3' : dataTimeStampTo}
   };
   console.log(params)
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
