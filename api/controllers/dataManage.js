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
  getMultipleCalculatedDataWithParameter: getMultipleCalculatedDataWithParameter
};


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
        shareUtil.SendSuccessWithData(res, data.Items[0]);
     }
     else {
       var msg = "Error: data count is not 1"
        shareUtil.SendInternalErr(res,msg);
     }

   }
   });
   // this sends back a JSON response which is a single string
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
