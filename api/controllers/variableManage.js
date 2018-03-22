var shareUtil = require('./shareUtil.js');
var asset = require('./asset.js');
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
  createVariable: createVariable,
  updateVariable: updateVariable,
  deleteVariable: deleteVariable,
  getVariablebyDevice: getVariablebyDevice,
  getVariableAttributes: getVariableAttributes,
  getVariableByAsset: getVariableByAsset
};


function updateVariableIDInDevice(variableID, deviceID, callback) {
  if(!deviceID)
  {
    callback(false, null);
  }
  else {
    checkVariableInDevice(variableID, deviceID, function(ret, msg1) {
      if (ret) {
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
            } else {
                callback(true,null);
            }
        });
      }
      else {
        callback(false, msg1 );
      }
    });
  }
}


function checkVariableInDevice(variableID, deviceID, callback) {

  var params = {
    TableName: shareUtil.tables.device,
    KeyConditionExpression : "DeviceID = :v1",
    ExpressionAttributeValues : {':v1' : deviceID.toString()}
  };
  shareUtil.awsclient.query(params, function(err, data) {
  if (err) {
    var msg = "Error:" + JSON.stringify(err, null, 2);
    callback(false,msg);
  }else{
    if (data.Count == 1) {
      if (typeof data.Items[0].Variables == "undefined")
      {
        callback(true,null);
      }
      else {
        if (data.Items[0].Variables.indexOf(variableID) > -1) {
          var msg = "Variable Already exists in Device";
          callback(false,msg);
        }
        else {
          console.log("true, null");
          callback(true,null);
        }
      }

    }
    else {
        var msg = "Error: Cannot find data"
        callback(false,msg);
    }

  }
});
}

function addVariableInternal(variableobj, deviceid, res) {
  var uuidv1 = require('uuid/v1');
  var crypto = require('crypto');
  if (typeof variableobj.VariableID == "undefined"){
    var variableID = uuidv1();
  }
  else
  {
    var variableID = variableobj.VariableID;
  }
  var params = {
    TableName : shareUtil.tables.variable,
    Item : {
      VariableID: variableID,
      AddTimeStamp: Math.floor((new Date).getTime()/1000)
    },
    ConditionExpression : "attribute_not_exists(VariableID)"
  };
  params.Item = Object.assign(params.Item, variableobj);
  delete params.Item['DeviceID'];

  shareUtil.awsclient.put(params, function(err, data) {
    if (err) {
        var msg = "Error:" + JSON.stringify(err, null, 2);
        shareUtil.SendInternalErr(res,msg);
    }else{
        if (deviceid)
        {
            updateVariableIDInDevice(variableID, deviceid, function(ret1, data){
              if (ret1){
                shareUtil.SendSuccess(res);
              }
              else{
                var msg = "Error:" + JSON.stringify(data) + "update failed";
                shareUtil.SendInternalErr(res,msg);
              }
             });
        }
        else
        {
          console.log("variableID = "+ variableID);
          shareUtil.SendSuccess(res);
        }

    }
  });
}


function createVariable(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var variableobj = req.body;
  var deviceid = req.swagger.params.DeviceID.value;
  console.log("deviceid = " + deviceid);
  if(deviceid)
  {
    deviceManage.IsDeviceExist(deviceid, function(ret1, data1){
      if (ret1)
      {
        if (variableobj.VariableID)
        {
          IsVariableExist(variableobj.VariableID, function(ret,data){
            if (ret) {
              var msg = "VariableID Already Exists";
              shareUtil.SendInvalidInput(res, msg);
            } else {
              addVariableInternal(variableobj, deviceid, res);
            }
          });
        } else
        {
          addVariableInternal(variableobj, deviceid, res);
        }
      } else
      {
        var msg = "DeviceID does not exist";
        shareUtil.SendNotFound(res, msg);
      }
    });
  } else {
    var msg = "DeviceID missing"
    shareUtil.SendNotFound(res, msg);
  }
}




function updateVariableIDInAsset(variableID, assetID, callback) {
  if(!assetID)
  {
    callback(false, null);
  }
  else {
    checkVariableInAsset(variableID, assetID, function(ret, msg1) {
      if (ret) {
        var updateVar = {
          TableName : shareUtil.tables.assets,
          Key : {
            AssetID : assetID,
                },
          UpdateExpression : 'set Variables = list_append(if_not_exists(Variables, :empty_list), :id)',
          ExpressionAttributeValues: {
            ':id': [variableID],
            ':empty_list': []
          }
        };

        shareUtil.awsclient.update(updateVar, function (err, data) {
            if (err) {
                var msg = "Error:" +  JSON.stringify(err, null, 2);
                console.error(msg);
                callback(false,msg);
            } else {
                callback(true,null);
            }
        });
      }
      else {
        callback(false, msg1 );
      }
    });

  }
}


function checkVariableInAsset(variableID, assetID, callback) {

  var variables = {
    TableName: shareUtil.tables.assets,
    KeyConditionExpression : "AssetID = :v1",
    ExpressionAttributeValues : {':v1' : assetID.toString()}
  };
  shareUtil.awsclient.query(variables, function(err, data) {
  if (err) {
    var msg = "Error:" + JSON.stringify(err, null, 2);
    callback(false,msg);
  }else{
    if (data.Count == 1) {
      if (typeof data.Items[0].Variables == "undefined")
      {
        callback(true,null);
      }
      else {
        if (data.Items[0].Variables.indexOf(variableID) > -1) {
          var msg = "variable Already exists in Asset";
          callback(false,msg);
        }
        else {
          callback(true,null);
        }
      }

    }
    else {
        var msg = "Error: Cannot find data"
        callback(false,msg);
      }

    }
  });


}


function addVariableInternalToAsset(variableobj, assetid, res) {
  var uuidv1 = require('uuid/v1');
  var crypto = require('crypto');
  if (typeof variableobj.VariableID == "undefined"){
    var variableID = uuidv1();
  }
  else
  {
    var variableID = variableobj.VariableID;
  }
  var params = {
    TableName : shareUtil.tables.variable,
    Item : {
      VariableID: variableID,
      AddTimeStamp: Math.floor((new Date).getTime()/1000)
    },
    ConditionExpression : "attribute_not_exists(VariableID)"
  };
  params.Item = Object.assign(params.Item, variableobj);
  delete params.Item['DeviceID'];

  shareUtil.awsclient.put(params, function(err, data) {
    if (err) {
        var msg = "Error:" + JSON.stringify(err, null, 2);
        shareUtil.SendInternalErr(res,msg);
    }else{
        if (assetid)
        {
            updateVariableIDInAsset(variableID, assetid, function(ret1, data){
              if (ret1){
                shareUtil.SendSuccess(res);
              }
              else{
                var msg = "Error:" + JSON.stringify(data) + "update failed";
                shareUtil.SendInternalErr(res,msg);
              }
             });
        }
        else
        {
          console.log("variableID = "+ variableID);
          shareUtil.SendSuccess(res);
        }

    }
  });
}


function addVariableToAsset(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var variableobj = req.body;
  var assetid = req.swagger.params.AssetID.value;
  console.log("assetid = " + assetid);
  if (variableobj.VariableID) {
    IsVariableExist(variableobj.VariableID, function(ret,data){
      if (ret) {
        var msg = "VariableID Already Exists";
        shareUtil.SendInvalidInput(res, msg);
      } else {
        addVariableInternalToAsset(variableobj, assetid, res);
      }
    });
  } else {
    if (assetid){
      addVariableInternalToAsset(variableobj, assetid, res);
    } else
    {
      msg = "INVALID_INPUT: no variableID, nor assetID given";
      shareUtil.SendInvalidInput(res, msg);
    }
  }
}




function updateVariable(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var variableobj = req.body;
  var isValid = true;
  console.log(variableobj);
  if(variableobj.constructor === Object && Object.keys(variableobj).length === 0) {
    SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
  }
  else {
    if(!variableobj.VariableID)
    {
      var errmsg = {message: "INVALID_INPUT"};
      res.status(400).send(errmsg);
       //SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
    }
    else {
      // check if asset exists
      IsVariableExist(variableobj.VariableID, function(ret1, data){
          if (ret1) {
            var updateItems = "set ";
            var expressvalues = {};

            var i = 0
            for (var key in variableobj)
            {
              if (variableobj.hasOwnProperty(key))
              {
                if (key != "VariableID") //&& key != "Type")
                {
                  updateItems = updateItems + key.toString() + " = :v" + i.toString() + ",";
                  expressvalues[":v" + i.toString()] = variableobj[key];
                  i++;
                }
              }
            }

            updateItems = updateItems.slice(0, -1);

            var updateParams = {
                  TableName : shareUtil.tables.variable,
                  Key : {
                    VariableID: data.Items[0].VariableID
                    //DeviceID : deviceobj.DeviceID.toString()  //,
                    //Type : data.Items[0].Type
                },
                UpdateExpression : updateItems,
                ExpressionAttributeValues : expressvalues
              };
            console.log("key = " + data.Items[0].VariableID);
            console.log(updateParams);
            shareUtil.awsclient.update(updateParams, function (err, data) {
                 if (err) {
                     var msg = "Unable to update the settings table.( POST /settings) Error JSON:" +  JSON.stringify(err, null, 2);
                     console.error(msg);
                     var errmsg = {
                       message: msg
                     };
                     res.status(500).send(errmsg);
                 } else {
                   var msg = {
                     message: "Success"
                   };
                   console.log("device updated!");
                   res.status(200).send(msg);
                 }
             });
          }
          else {
            console.log("isvalid=false2");
            //SendInvalidInput(res,NOT_EXIST);
            var errmsg = {message: "INVALID_INPUT"};
            res.status(400).send(errmsg);
          }
      });
    }
  }
  // this sends back a JSON response which is a single string
}



function deleteSingleVariable(variableID, callback) {

  var deleteParams = {
    TableName : shareUtil.tables.variable,
    Key : { VariableID : variableID }
  };
  console.log(deleteParams);
  shareUtil.awsclient.delete(deleteParams, onDelete);
  function onDelete(err, data)
  {
    if (err) {
      var msg = "Unable to delete the settings table.( POST /settings) Error JSON:" +  JSON.stringify(err, null, 2);
      console.error(msg);
      var errmsg = { message: msg };
      callback(false, msg);
      //res.status(500).send(errmsg);
      //shareUtil.SendInternalErr(msg);
    } else
    {
      var msg = { message: "Success" };
      console.log("device deleted!");
      //shareUtil.SendSuccess();
      callback(true, null);
      //res.status(200).send(msg);
    }
  }
}




function findVariableIndexInDevice(deviceID, variableID, callback){

  var devicesParams = {
    TableName : shareUtil.tables.device,
    KeyConditionExpression : "DeviceID = :V1",
    ExpressionAttributeValues :  { ':V1' : deviceID},
    ProjectionExpression : "Variables"
  };
  shareUtil.awsclient.query(devicesParams, onQuery);
  function onQuery(err, data)
  {
    if (err)
    {
      var msg = "Error:" + JSON.stringify(err, null, 2);
      shareUtil.SendInternalErr(res, msg);
    } else
    {
      if (data.Count == 0)
      {
        var errmsg = {message: "DeviceID does not exist or Device does not contain any Variable"};
      //  res.status(400).send(errmsg);
        callback(false, msg);
      }
      else
      {
        // find index of device in devices list coming from the result of the query in the Asset table
        var variables = data.Items[0].Variables;
        var variableIndex;
        var index = 0;
        if ( typeof variables == "undefined")
        {
          console.log("undefined");
          var errmsg = {message: "DeviceID does not exist or Device does not contain any Variable"};
          //res.status(400).send(errmsg);
          callback(false, msg);
        }
        else
        {
          while (index < variables.length)
          {
            console.log("variables.Items[0]: " + variables[index]);
            if (variables[index] == variableID)
            {
              variableIndex = index;
              index  = variables.length;
            } else
            {
              index +=1;
            }
          }
        }
      }
      if (index > 0){
        deleteVarFromDeviceList(variableIndex, deviceID, function(ret2, msg){
          if (ret2){
            callback(true);
          } else
          {
            callback(false, msg);
          }
        });
      }
    }
  }
}



function deleteVarFromDeviceList(variableIndex, deviceID, callback) {

  if (typeof variableIndex == "undefined"){
    var msg = "Variable not found in Device's list of Variables";
    callback(false, msg);
  } else
  {
    console.log("variable.index = " + variableIndex);
    var updateExpr = "remove Variables[" + variableIndex + "]";
    var updateDevice = {
      TableName : shareUtil.tables.device,
      Key : {DeviceID : deviceID},
      UpdateExpression : updateExpr
      //ExpressionAttributeValues : { ':V1' : deviceIndex}
    };
    shareUtil.awsclient.update(updateDevice, onUpdate);
    function onUpdate(err, data)
    {
      if (err)
      {
        var msg = "Unable to update the settings table.( POST /settings) Error JSON:" +  JSON.stringify(err, null, 2);
        console.error(msg);
        var errmsg = { message: msg };
        callback(false, msg);
      } else
      {
        callback(true);
      }
    }
  }
}




function findVariableIndexInAsset(assetID, variableID, callback){

  var assetsParams = {
    TableName : shareUtil.tables.assets,
    KeyConditionExpression : "AssetID = :V1",
    ExpressionAttributeValues :  { ':V1' : assetID},
    ProjectionExpression : "Variables"
  };
  shareUtil.awsclient.query(assetsParams, onQuery);
  function onQuery(err, data)
  {
    if (err)
    {
      var msg = "Error:" + JSON.stringify(err, null, 2);
      callback(false, msg)
    } else
    {
      if (data.Count == 0)
      {
        var errmsg = {message: "AssetID does not exist or Asset does not contain any Variable"};
        callback(false, msg);
      }
      else
      {
        // find index of device in devices list coming from the result of the query in the Asset table
        var variables = data.Items[0].Variables;
        var variableIndex;
        var index = 0;
        if ( typeof variables == "undefined")
        {
          console.log("undefined");
          var errmsg = {message: "AssetID does not exist or Asset does not contain any Variable"};
          res.status(400).send(errmsg);
        }
        else
        {
          while (index < variables.length)
          {
            console.log("variables.Items[0]: " + variables[index]);
            if (variables[index] == variableID)
            {
              variableIndex = index;
              index  = variables.length;
            } else
            {
              index +=1;
            }
          }
        }
      }
      if (index > 0){
        deleteVarFromAssetList(variableIndex, assetID, function(ret2, msg){
          if (ret2){
            callback(true);
          } else {
            callback(false, msg);
          }
        });
      }
    }
  }
}



function deleteVarFromAssetList(variableIndex, assetID, callback) {

  if (typeof variableIndex == "undefined"){
    var msg = "Variable not found in Asset's list of Variables";
    callback(false, msg);
  } else
  {
    console.log("variable.index = " + variableIndex);
    var updateExpr = "remove Variables[" + variableIndex + "]";
    var updateAsset = {
      TableName : shareUtil.tables.assets,
      Key : {AssetID : assetID},
      UpdateExpression : updateExpr
    };
    shareUtil.awsclient.update(updateAsset, onUpdate);
    function onUpdate(err, data)
    {
      if (err)
      {
        var msg = "Unable to update the settings table.( POST /settings) Error JSON:" +  JSON.stringify(err, null, 2);
        console.error(msg);
        var errmsg = { message: msg };
        res.status(500).send(errmsg);
      } else
      {
        callback(true);
      }
    }
  }
}



// Delete device by deviceID
// requires also AssetID in argument to delete the device from the table Asset in the Devices list attribute
function deleteVariable(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var variableID = req.swagger.params.VariableID.value;
  var deviceID = req.swagger.params.DeviceID.value;

  IsVariableExist(variableID, function(ret1, data)
  {
    if (ret1)
    {
      if (typeof deviceID == "undefined" && typeof assetID == "undefined")
      {   // in case we want to delete a Variable that is not in any Device no any Asset
        deleteSingleVariable(variableID, function(ret, data)
        {
          if (ret)
          {
            shareUtil.SendSuccess(res);
          } else
          {
            var msg = "Error:" + JSON.stringify(data);
            shareUtil.SendInternalErr(res, msg);
          }
        });
      }
      else
      {
        if (deviceID)
        {
          findVariableIndexInDevice(deviceID, variableID, function(ret2, data) {
            if (ret2)
            {
              deleteSingleVariable(variableID, function(ret, data)
              {
                if (ret)
                {
                  shareUtil.SendSuccess(res);
                } else
                {
                  var msg = "Error:" + JSON.stringify(data);
                  shareUtil.SendInternalErr(res, msg);
                }
              });
            } else
            {
              var msg = "Error:" + JSON.stringify(data);
              shareUtil.SendInternalErr(res, msg);
            }
          });
        }
        else
        {
          var msg = "No DeviceID given"
          shareUtil.SendNotFound(res, msg);
        }
      }
    }
    else
    {
      console.log("isvalid=false2");
      //var msg = " DeviceID does not exist";
      var errmsg = { message: "VariableID does not exist" };
      res.status(400).send(errmsg);
    }
  });
  // this sends back a JSON response which is a single string
}


function getVariablebyDeviceID(deviceid, callback) {
  var variablesParams = {
    TableName : shareUtil.tables.device,
    KeyConditionExpression : "DeviceID = :V1",
    ExpressionAttributeValues :  { ':V1' : deviceid},
    ProjectionExpression : "Variables"
  };
  shareUtil.awsclient.query(variablesParams, onQuery);
  function onQuery(err, data)
  {
    if (err)
    {
      console.log("deviceid = " + deviceid);
      var msg = "Error:" + JSON.stringify(err, null, 2);
      shareUtil.SendInternalErr(res, msg);
    } else
     {
      var sendData =
      {
        Items: [],
        Count: 0
      };
      if (data.Count == 0)
      {
        var msg = "DeviceID does not exist" ;
        callback(false, msg);
      } else
      {
        var variables = data.Items[0].Variables;
  //      console.log("variables = " + variables);
    //    console.log("data.count = " + data.Count);


        if (typeof variables == "undefined")
        {
          var msg = "DeviceID does not contain any variable";
          callback(false, msg);
        }
        else
        {
          if (variables.length == 0)
          {
            console.log("length  = 0");
            var msg = "No Variable found in Device";
            callback(false, msg);
          }
          else
          {
            console.log("variables: " + variables);
            console.log("variables.length = " + variables.length);
            var variablesToDelete = [];
            var deleteIndex = 0;
            getSingleVariableInternal(0, variables, deviceid, variablesToDelete, deleteIndex, null, function(variablesdata, variablesToDelete){
              console.log("variablesToDelete -> " + variablesToDelete);
              sendData.Items = variablesdata;
              sendData.Count = variablesdata.length;
              if (variablesToDelete.length == 0)    // no garbage Variables to delete in Device's list of Variables
              {
                callback(true, sendData);
                console.log("sendData = " + JSON.stringify(sendData, null, 2));
              } else
              {
                deleteGarbageVariablesInDevice(sendData, deviceid, variablesToDelete, function(sendData) {
                callback(true, sendData);
                });
              }
            });
          }
        }
      }
    }
  }
}

function getVariablebyDevice(req, res) {
  var deviceid = req.swagger.params.DeviceID.value;
  getVariablebyDeviceID(deviceid, function(ret, data) {
    if (ret)
    {
      shareUtil.SendSuccessWithData(res, data);
    } else
    {
      shareUtil.SendNotFound(res, data);
    }
  });
}



function deleteGarbageVariablesInDevice(sendData, deviceid, variablesToDelete, callback) {

  var updateExpr = "remove ";
  for (var k in variablesToDelete)
  {
    updateExpr = updateExpr + "Variables[" + variablesToDelete[k] + "], ";
  }

  console.log("updateExpr = " + updateExpr);
  var updateDevice = {
    TableName : shareUtil.tables.device,
    Key : {DeviceID : deviceid},
    UpdateExpression : updateExpr.slice(0, -2)        // slice to delete ", " at the end of updateExpr
  };
  shareUtil.awsclient.update(updateDevice, onUpdate);
  function onUpdate(err, data)
  {
    if (err)
    {
      var msg = "Unable to update the settings table.( POST /settings) Error JSON:" +  JSON.stringify(err, null, 2);
      console.error(msg);
      var errmsg = { message: msg };
    } else
    {
      console.log("variables deleted from Device list of Variables!");
      callback(sendData);
    }
  }
}


function getSingleVariableInternal(index, variables, deviceid, variablesToDelete, deleteIndex, variableout, callback) {
  if (index < variables.length)
  {
    if (index == 0)
    {
      variableout = [];
    }
  //  console.log("variables.Items[0]: " + variables[index]);
    var variablesParams = {
      TableName : shareUtil.tables.variable,
      KeyConditionExpression : "VariableID = :v1",
      ExpressionAttributeValues : { ':v1' : variables[index]}
    };
    shareUtil.awsclient.query(variablesParams, onQuery);
    function onQuery(err, data) {
      if (!err)
      {
      //  console.log("no error");
      //  console.log("data.count = " + data.Count);
        if (data.Count == 1)
        {
          variableout.push(data.Items[0]);
          //rsconsole.log("variableout: " + variableout);
        } else
        {
          variablesToDelete[deleteIndex] = index;
        //  console.log("variables[index] -> " + variables[index]);
          deleteIndex+=1;
        }
      }
      getSingleVariableInternal(index + 1, variables, deviceid, variablesToDelete, deleteIndex, variableout, callback);
    }
  }
  else
  {
    callback(variableout, variablesToDelete);
  }
}


function getVariablesFromDeviceArray(devices, index, variablesout, callback) {

  if(index < devices.length)
  {
    getVariablebyDeviceID(devices[index], function(ret, data) {
      if (ret) {
        var i = 0;
        for ( i in data.Items)
        {
          variablesout.push(data.Items[i]);
          console.log("i = " + i);
        }
        //console.log("data = " + JSON.stringify(data, null, 2));
      }
      getVariablesFromDeviceArray(devices, index+1, variablesout, callback);
    });
  } else
  {
    callback(true, variablesout)
    console.log("variablesout = " + JSON.stringify(variablesout, null, 2));
    console.log("var[] length = " + variablesout.length);
  }
}


function getVariableByAssetIDOld(req, res) {
  var assetid = req.swagger.params.AssetID.value;
  var parametersParams = {
    TableName : shareUtil.tables.assets,
    KeyConditionExpression : "AssetID = :V1",
    ExpressionAttributeValues :  { ':V1' : assetid},
    ProjectionExpression : "Variables"
  };
  shareUtil.awsclient.query(parametersParams, onQuery);
  function onQuery(err, data)
  {
    if (err)
    {
      var msg = "Error:" + JSON.stringify(err, null, 2);
      shareUtil.SendInternalErr(res, msg);
    } else
     {
      var sendData =
      {
        Items: [],
        Count: 0
      };
      if (data.Count == 0)
      {
        var msg = "AssetID not found";
        shareUtil.SendNotFound(res, msg);
      }
      else
      {
        var variables = data.Items[0].Variables;
        console.log("variables = " + variables);
        console.log("data.count = " + data.Count);


        if (typeof variables == "undefined")
        {
          console.log("Error msg : Variables undefined");
          msg = "No Variable found in this Asset";
          shareUtil.SendNotFound(res, msg);
        }
        else
        {
          if (variables.length == 0)
          {
            console.log("Error msg: Variables.length  = 0");
            msg = "No Variable found in this Asset";
            shareUtil.SendNotFound(res, msg);
          }
          else
          {
            console.log("variables: " + variables);
            console.log("variables.length = " + variables.length);
            var variablesToDelete = [];
            var deleteIndex = 0;
            getSingleVariableInternal(0, variables, assetid, variablesToDelete, deleteIndex,null, function(variablesdata, variablesToDelete){
              sendData.Items = variablesdata;
              sendData.Count = variablesdata.length;
              if(variablesToDelete.length == 0)
              {
              shareUtil.SendSuccessWithData(res, sendData);
            } else
            {
              deleteGarbageVariablesInAsset(sendData, assetid, variablesToDelete, function(sendData){
              shareUtil.SendSuccessWithData(res, sendData);
            });
            }
            });
          }
        }
      }
    }
  }
}

function getVariableByAsset(req, res) {

  var assetid = req.swagger.params.AssetID.value;

  var sendData =
  {
    Items: [],
    Count: 0
  };
  asset.getDevicesFromAsset(assetid, function(ret, data) {
    if (ret)
    {
      var devices = data.Devices;
      var variablesout = [];
  //    console.log("devices[] = " + devices);

      getVariablesFromDeviceArray(devices, 0, variablesout, function(ret, data) {
        if (ret){
          sendData.Items = variablesout;
          sendData.Count = variablesout.length;
          shareUtil.SendSuccessWithData(res, sendData);
        } else
        {
          shareUtil.SendNotFound(res, data);
        }
      });
    } else
    {
      // get Devices form Asset failed
      shareUtil.SendNotFound(res, data);
    }
  });
}




function deleteGarbageVariablesInAsset(sendData, assetid, variablesToDelete, callback) {

  var updateExpr = "remove ";
  for (var k in variablesToDelete)
  {
    updateExpr = updateExpr + "Variables[" + variablesToDelete[k] + "], ";
  }

  console.log("updateExpr = " + updateExpr);
  var updateAsset = {
    TableName : shareUtil.tables.assets,
    Key : {AssetID : assetid},
    UpdateExpression : updateExpr.slice(0, -2)        // slice to delete ", " at the end of updateExpr
  };
  shareUtil.awsclient.update(updateAsset, onUpdate);
  function onUpdate(err, data)
  {
    if (err)
    {
      var msg = "Unable to update the settings table.( POST /settings) Error JSON:" +  JSON.stringify(err, null, 2);
      console.error(msg);
      var errmsg = { message: msg };
    } else
    {
      console.log("variables deleted from Asset list !");
      callback(sendData);
    }
  }
}




function getVariableAttributes(req, res) {

  var variableid = req.swagger.params.VariableID.value;

  var variableParams = {
    TableName: shareUtil.tables.variable,
    KeyConditionExpression : "VariableID = :v1",
    ExpressionAttributeValues : { ':v1' : variableid.toString()}
  }

  shareUtil.awsclient.query(variableParams, onQuery);
  function onQuery(err, data) {
    if (err) {
      var msg =  "Unable to scan the variable table.(getVariable) Error JSON:" + JSON.stringify(err, null, 2);
      shareUtil.SendInternalErr(res,msg);
    }
    else {
      if (data.Count == 0)
      {
        shareUtil.SendNotFound(res);
      }
      else
      {
        console.log("data = " + JSON.stringify(data.Items[0], null, 2));
        shareUtil.SendSuccessWithData(res, data.Items[0]);
      }
    }
  }
}




function IsVariableExist(variableID, callback) {

  var Params = {
     TableName : shareUtil.tables.variable,
     KeyConditionExpression : "VariableID = :v1",
     ExpressionAttributeValues : {':v1' : variableID.toString()}
  };
  console.log(variableID.toString());
  shareUtil.awsclient.query(Params, onQuery);
  function onQuery(err, data)
  {
    if (err)
    {
      console.log("error");
      var msg = "Error:" + JSON.stringify(err, null, 2);
      SendInternalErr(res, msg);
    } else
    {
      if (data.Count == 0)
      {
        console.log("data cout  = 0");
        callback(false, data);
      }
      else
      {
        callback(true, data);
      }
    }
  }
}
