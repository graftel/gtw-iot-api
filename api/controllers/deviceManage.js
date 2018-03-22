//'use strict';

var shareUtil = require('./shareUtil.js');
var asset = require('./asset.js');
var userManage = require('./userManage.js')
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
  createDevice: createDevice,
  createDeviceToAsset: createDeviceToAsset,
  addExistingDeviceBySerialNumber: addExistingDeviceBySerialNumber,
  updateDevice: updateDevice,
  deleteDevice: deleteDevice,
  getDeviceByAsset: getDeviceByAsset,
  getDeviceAttributes: getDeviceAttributes,
  getDeviceByUser: getDeviceByUser,
  removeDeviceFromAsset: removeDeviceFromAsset,
  addDeviceToAsset: addDeviceToAsset,
  getDeviceByAssetID: getDeviceByAssetID,
  getVariablesFromDevice: getVariablesFromDevice,
  IsDeviceExist: IsDeviceExist
};


function removeDeviceFromAsset(req, res){

  var deviceobj = req.body;
  var assetid = deviceobj.AssetID;
  var deviceid = deviceobj.DeviceID

    asset.getDevicesFromAsset(assetid, function(ret, data) {
    if (ret)
    {
      var deviceIndex = data.Devices.indexOf(deviceid);
      var updateExpr = "remove Devices[" + deviceIndex + "]";

      var updateAsset = {
        TableName : shareUtil.tables.assets,
        Key : {AssetID : assetid},
        UpdateExpression : updateExpr
      };
      shareUtil.awsclient.update(updateAsset, onUpdate);
      function onUpdate(err, data)
      {
        if (err)
        {
          var msg = "Unable to update the settings table.( POST /settings) Error JSON:" +  JSON.stringify(err, null, 2);
          //console.error(msg);
        //  var errmsg = { message: msg };
          shareUtil.SendInternalErr(res, msg);
        } else
        {
          console.log("devices deleted from Asset list of Devices!");
          shareUtil.SendSuccess(res);
        }
      }
    } else
    {
      var msg = "Error:" + JSON.stringify(data, null, 2);
      shareUtil.SendInternalErr(res, msg);
    }
  });
}


function addDeviceToAsset(req, res) {
  var deviceobj = req.body;
  var assetid = deviceobj.AssetID;
  var deviceid = deviceobj.DeviceID

  IsDeviceExist(deviceid, function(ret, data) {
    if (ret)
    {
      console.log("device Exist");
      updateDeviceIDInAsset(deviceid, assetid, function(ret1, data1) {
        if (ret1)
        {
          shareUtil.SendSuccess(res);
        } else
        {
          var msg = "Error:" + JSON.stringify(data1);
          shareUtil.SendInternalErr(res,msg);
        }
      });
    } else
    {
      var msg = "Device not found";
      shareUtil.SendNotFound(res, msg);
    }
  });
}


function updateDeviceIDInUser(deviceID, userID, callback) {
  if(!userID)
  {
    callback(false, null);
  }
  else {
    console.log("userID exist = " + userID);
    checkDeviceInUser(deviceID, userID, function(ret, msg1) {
      if (ret) {
        console.log("device not in user");
        var updateParams = {
          TableName : shareUtil.tables.users,
          Key : {
            UserID : userID,
                },
          UpdateExpression : 'set #device = list_append(if_not_exists(#device, :empty_list), :id)',
          ExpressionAttributeNames: {
            '#device': 'Devices'
          },
          ExpressionAttributeValues: {
            ':id': [deviceID],
            ':empty_list': []
          }
        };

        shareUtil.awsclient.update(updateParams, function (err, data) {
            if (err) {
              console.log("update failed, params = " + JSON.stringify(updateParams, null, 2));
                var msg = "Error:" +  JSON.stringify(err, null, 2);
                console.error(msg);
                callback(false, msg);
            } else {
                callback(true, null);
            }
        });
      }
      else {
        console.log("device in user");
        callback(false, msg1 );
      }
    });

  }
}

function checkDeviceInUser(deviceID, userID, callback) {

  var params = {
    TableName: shareUtil.tables.users,
    KeyConditionExpression : "UserID = :v1",
    ExpressionAttributeValues : {':v1' : userID.toString()}
  };
  shareUtil.awsclient.query(params, function(err, data) {
    if (err)
    {
      var msg = "Error:" + JSON.stringify(err, null, 2);
      callback(false,msg);
    } else
    {
      if (data.Count == 1)
      {
        if (typeof data.Items[0].Devices == "undefined")
        {
          callback(true,null);
        }
        else
        {
          if (data.Items[0].Devices.indexOf(deviceID) > -1)
          {
            var msg = "Device Already exists in User";
            callback(false,msg);
          }
          else
          {
            callback(true,null);
          }
        }
      }
      else
      {
        //var msg = "Cannot find data";
        var msg = "UserID not found";
        callback(false,msg);
      }
    }
  });
}

function addDeviceInternal(deviceobj, res) {
  var uuidv1 = require('uuid/v1');
  var crypto = require('crypto');
  if (typeof deviceobj.DeviceID == "undefined")
  {
    var deviceID = uuidv1();
  }
  else
  {
    var deviceID = deviceobj.DeviceID;
  }
  var params = {
    TableName : shareUtil.tables.device,
    Item : {
      DeviceID: deviceID,
      AddTimeStamp: Math.floor((new Date).getTime()/1000)
    },
    ConditionExpression : "attribute_not_exists(DeviceID)"
  };

  isDisplayNameUniqueInUser(deviceobj.DisplayName, deviceobj.UserID, function(ret, data) {
    if (ret)
    {
      console.log("displayName unique");

      params.Item = Object.assign(params.Item, deviceobj);
      delete params.Item['UserID'];
      delete params.Item['AssetID'];

      shareUtil.awsclient.put(params, function(err, data) {
        if (err)
        {
          var msg = "Error:" + JSON.stringify(err, null, 2);
          console.error(msg);
          shareUtil.SendInternalErr(res,msg);
        } else
        {
          updateDeviceIDInUser(deviceID, deviceobj.UserID, function(ret1, data){
            if (ret1)
            {
              if (deviceobj.AssetID)
              {
                updateDeviceIDInAsset(deviceID, deviceobj.AssetID, function(ret2, data){
                  if (ret2){
                    shareUtil.SendSuccess(res);
                  } else
                  {
                    var msg = "Error:" + JSON.stringify(data);
                    shareUtil.SendInternalErr(res,msg);
                  }
                });
              } else
              {
                shareUtil.SendSuccess(res);
              }
            } else
            {
              var msg = "Error:" + JSON.stringify(data);
              shareUtil.SendInternalErr(res,msg);
            }
          });
        }
      });
    } else
    {
    console.log("displayName not unique")
    var uniqNumb = 1;
    var newDisplayName = deviceobj.DisplayName + uniqNumb;
    isDisplayNameUniqueInUser(newDisplayName, deviceobj.UserID, function(ret, data) {
      if (ret)
      {

      } else
      {

      }
    });
    shareUtil.SendInvalidInput(res, data);
  }
});
}



function updateDeviceIDInAsset(deviceID, assetID, callback) {
  if(!assetID)
  {
    callback(false, null);
  }
  else {
    checkDeviceInAsset(deviceID, assetID, function(ret, msg1) {
      if (ret) {
        var updateParams = {
          TableName : shareUtil.tables.assets,
          Key : {
            AssetID : assetID,
                },
          UpdateExpression : 'set #device = list_append(if_not_exists(#device, :empty_list), :id)',
          ExpressionAttributeNames: {
            '#device': 'Devices'
          },
          ExpressionAttributeValues: {
            ':id': [deviceID],
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
        console.log(" msg1 = " + msg1);
        callback(false, msg1);
      }
    });
  }
}

function checkDeviceInAsset(deviceID, assetID, callback) {

  var params = {
    TableName: shareUtil.tables.assets,
    KeyConditionExpression : "AssetID = :v1",
    ExpressionAttributeValues : {':v1' : assetID.toString()}
  };
  shareUtil.awsclient.query(params, function(err, data) {
    if (err)
    {
      var msg = "Error:" + JSON.stringify(err, null, 2);
      callback(false,msg);
    } else
    {
      console.log(JSON.stringify(data, null, 2));
      if (data.Count == 1)
      {
        if (typeof data.Items[0].Devices == "undefined")
        {
          callback(true,null);
        }
        else
        {
          if (data.Items[0].Devices.indexOf(deviceID) > -1)
          {
            var msg = "Device Already exists in Asset";
            callback(false, msg);
          }
          else
          {
            callback(true, null);
          }
        }
      }
      else
      {
        var msg = "AssetID not found";
        callback(false,msg);
      }
    }
  });
}





function addExistingDeviceBySerialNumber(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var deviceobj = req.body;
  var isValid = true;
  console.log(deviceobj);
  if(deviceobj.constructor === Object && Object.keys(deviceobj).length === 0) {
    console.log("is valid = false0");
    shareUtil.SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
  }
  else {
    if(!deviceobj.AssetID || !deviceobj.SerialNumber || !deviceobj.VerificationCode)
    {
      console.log("is valid = false1");
       shareUtil.SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
    }
    else {

      IsDeviceSerialNumberExist(deviceobj.SerialNumber, function(ret1, data){
        if (ret1) {
          // verify Code
          if (data.Items[0].VerificationCode === deviceobj.VerificationCode) {
            updateDeviceIDInAsset(data.Items[0].DeviceID, deviceobj.AssetID, function(ret2, data){
                if (ret2){
                  shareUtil.SendSuccess(res);
                }
                else{
                  shareUtil.SendInvalidInput(res, data);
                }
            });
          }
          else {
            shareUtil.SendInvalidInput(res,"Wrong VerificationCode");
          }
        } else {
          shareUtil.SendInvalidInput(res,"Serial Number Not exist");
        }
      });
    }
  }
}

function addDeviceToAssetInternal(deviceobj, res) {
  var uuidv1 = require('uuid/v1');
  var crypto = require('crypto');
  if (typeof deviceobj.DeviceID == "undefined")
  {
    var deviceID = uuidv1();
  }
  else
  {
    var deviceID = deviceobj.DeviceID;
  }

  var params = {
    TableName : shareUtil.tables.device,
    Item : {
      DeviceID: deviceID,
      AddTimeStamp: Math.floor((new Date).getTime()/1000)
    },
    ConditionExpression : "attribute_not_exists(DeviceID)"
  };
  params.Item = Object.assign(params.Item, deviceobj);
  delete params.Item['AssetID'];

  shareUtil.awsclient.put(params, function(err, data) {
    if (err)
    {
      var msg = "Error:" + JSON.stringify(err, null, 2);
      console.error(msg);
      shareUtil.SendInternalErr(res,msg);
    } else
    {
      if (deviceobj.AssetID)
      {
        updateDeviceIDInAsset(deviceID, deviceobj.AssetID, function(ret1, data){
          if (ret1)
          {
            shareUtil.SendSuccess(res);
          } else
          {
            var msg = "Error:" + JSON.stringify(data);
            shareUtil.SendInternalErr(res,msg);
          }
        });
      } else
      {
        shareUtil.SendSuccess(res);
      }
    }
  });
}

function createDeviceToAsset(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var deviceobj = req.body;
  IsUserExist(deviceobj.UserID, function(ret, data){
    if (ret)
    {
      if(deviceobj.DeviceID)
      {
        IsDeviceExist(deviceobj.DeviceID, function(ret1, data1)
        {
          if (ret1)
          {
            var msg = "DeviceID already exists";
            shareUtil.SendInvalidInput(res, msg);
          } else
          {
            if (deviceobj.SerialNumber)
            {
              IsDeviceSerialNumberUniqueInUser(deviceobj.SerialNumber, function(ret,data)
              {
                if (ret)
                {
                  addDeviceInternal(deviceobj, res);
                } else
                {
                  var msg = "Serial Number Already Exists";
                  shareUtil.SendInvalidInput(res, msg);
                }
              });
            } else
            {
              addDeviceInternal(deviceobj, res);
            }
          }
        });
      } else
      {
        addDeviceInternal(deviceobj, res);
      }
    } else
    {
      msg = "UserID not found";
      shareUtil.SendNotFound(res, msg);
    }
  });
}



function createDevice(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var deviceobj = req.body;
  var displayName = deviceobj.DisplayName;
  var userid = deviceobj.UserID;

  IsUserExist(deviceobj.UserID, function(ret, data){
    if (ret){
      if(deviceobj.DeviceID)
      {
        IsDeviceExist(deviceobj.DeviceID, function(ret1, data1)
        {
          if (ret1)
          {
            var msg = "DeviceID already exists";
            shareUtil.SendInvalidInput(res, msg);
          } else
          {
            if (deviceobj.SerialNumber)
            {
              IsDeviceSerialNumberUniqueInUser(deviceobj.SerialNumber, deviceobj.UserID, function(ret,data)
              {
                if (ret)
                {
                  addDeviceInternal(deviceobj, res);
                } else
                {
                  var msg = "Serial Number Already Exists";
                  shareUtil.SendInvalidInput(res, msg);
                }
              });
            } else
            {
              addDeviceInternal(deviceobj, res);
            }
          }
        });
      } else {
        addDeviceInternal(deviceobj, res);
      }
    } else
    {
      msg = "UserID not found";
      shareUtil.SendNotFound(res, msg);
    }
  });
}


function IsDeviceSerialNumberUniqueInUser(serialNumber, userid, callback) {

  userManage.getDevicesFromUser(userid, function(ret, data){
    if (ret)
    {
      var devices = data.Devices;
      var serialNumberList = []
      getSerialNumberList(devices, 0, serialNumberList, function(ret1, serialNumberList, data1) {
        if (ret1)
        {
          console.log("serialNumberList" + JSON.stringify(serialNumberList, null, 2));
          isItemInList(serialNumber, serialNumberList, function(ret2, data2) {
            if (ret2)
            {
              callback(true, null);
            } else
            {
              var msg = "SerialNumber not unique";
              callback(false, msg)
            }
          });
        } else
        {
          callback(false, null, data1);
        }
      })

    } else
    {
      callback(false, data);
    }
  });
}

function getSerialNumberList(devicesArrayID, index, serialNumberList, callback) {   // Can improve speed of this function by doing onl one query with all teh DeviceID rather than doing a query for each DeviceID

  if (index < devicesArrayID.length)
  {
    var deviceid = devicesArrayID[index];
    var devicesParams = {
      TableName : shareUtil.tables.device,
      KeyConditionExpression : "DeviceID = :v1",
      ExpressionAttributeValues : {':v1' : deviceid},
      ProjectionExpression : "SerialNumber"
    }
    shareUtil.awsclient.query(devicesParams, onQuery);
    function onQuery(err, data) {
      if (err) {
        var msg = "Error:" + JSON.stringify(err, null, 2);
        callback(false, null, msg);
      } else
      {
        serialNumberList.push(data.Items[0].SerialNumber);
      }
      getSerialNumberList(devicesArrayID, index+1, serialNumberList, callback);
    }
  } else
  {
    callback(true, serialNumberList, null);
  }
}



function isDisplayNameUniqueInUser(displayName, userid, callback){

  userManage.getDevicesFromUser(userid, function(ret, data){
    if (ret)
    {
      var devices = data.Devices;
      var displayNameList = []
      getDisplayNameList(devices, 0, displayNameList, function(ret1, displayNameList, data1) {
        if (ret1)
        {
          console.log("displayNameList" + JSON.stringify(displayNameList, null, 2));
          isItemInList(displayName, displayNameList, function(ret2, data2) {
            if (ret2)
            {
              callback(true, null);
            } else
            {
              var msg = "displayName not unique";
              callback(false, msg)
            }
          });
        } else
        {
          callback(false, null, data1);
        }
      })

    } else
    {
      callback(false, data);
    }
  });
}

function isItemInList(item, itemList, callback){

  if (itemList.indexOf(item) > -1)
  {
    // item is in list
    var msg = "item is the list";
    callback(false, msg);
  } else
  {
    callback(true, null);
  }
}

function getDisplayNameList(devicesArrayID, index, displayNameList, callback) {   // Can improve speed of this function by doing onl one query with all teh DeviceID rather than doing a query for each DeviceID

  if (index < devicesArrayID.length)
  {
    var deviceid = devicesArrayID[index];
    var devicesParams = {
      TableName : shareUtil.tables.device,
      KeyConditionExpression : "DeviceID = :v1",
      ExpressionAttributeValues : {':v1' : deviceid},
      ProjectionExpression : "DisplayName"
    }
    shareUtil.awsclient.query(devicesParams, onQuery);
    function onQuery(err, data) {
      if (err) {
        var msg = "Error:" + JSON.stringify(err, null, 2);
        callback(false, null, msg);
      } else
      {
        displayNameList.push(data.Items[0].DisplayName);
      }
      getDisplayNameList(devicesArrayID, index+1, displayNameList, callback);
    }
  } else
  {
    callback(true, displayNameList, null);
  }
}




function updateDevice(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var deviceobj = req.body;
  var isValid = true;
  console.log(deviceobj);
  if(deviceobj.constructor === Object && Object.keys(deviceobj).length === 0) {
    shareUtil.SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
  }
  else {
    if(!deviceobj.DeviceID)
    {
      shareUtil.SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
    }
    else {
      // check if asset exists
      IsDeviceExist(deviceobj.DeviceID, function(ret1, data){
          if (ret1) {
            var updateItems = "set ";
            var expressvalues = {};

            var i = 0
            for (var key in deviceobj)
            {
              if (deviceobj.hasOwnProperty(key))
              {
                if (key != "DeviceID")
                {
                  updateItems = updateItems + key.toString() + " = :v" + i.toString() + ",";
                  expressvalues[":v" + i.toString()] = deviceobj[key];
                  i++;
                }
              }
            }

            updateItems = updateItems.slice(0, -1);

            var updateParams = {
                  TableName : shareUtil.tables.device,
                  Key : {
                    DeviceID: data.Items[0].DeviceID
                },
                UpdateExpression : updateItems,
                ExpressionAttributeValues : expressvalues
              };
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
                   console.log("asset updated!");
                   res.status(200).send(msg);
                 }
             });
          }
          else {
            console.log("isvalid=false2");
            shareUtil.SendInvalidInput(res,shareUtil.NOT_EXIST);
          }
      });
    }
  }
  // this sends back a JSON response which is a single string
}



// Delete device by deviceID or by AssetID
// requires also AssetID in argument to delete the device from the table Asset in the Devices list attribute
function deleteDeviceOld(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var deviceID = req.swagger.params.DeviceID.value;
  var assetID = req.swagger.params.AssetID.value;

  IsDeviceExist(deviceID, function(ret1, data)
  {
    if (ret1)
    {
      if (typeof assetID == "undefined")
      {   // in case we want to delete a Device that is not in any Asset
        var deleteParams = {
          TableName : shareUtil.tables.device,
          Key : { DeviceID : deviceID }
        };
        console.log(deleteParams);
        shareUtil.awsclient.delete(deleteParams, onDelete);
        function onDelete(err, data)
        {
          if (err) {
            var msg = "Unable to delete the settings table.( POST /settings) Error JSON:" +  JSON.stringify(err, null, 2);
            console.error(msg);
            var errmsg = { message: msg };
            res.status(500).send(errmsg);
          } else
          {
            var msg = { message: "Success" };
            console.log("device deleted!");
            res.status(200).send(msg);
          }
        }
      }
      else
      {
      console.log("assetID = " + assetID);
      // 1st -> get index of device to delete
      var assetsParams = {
        TableName : shareUtil.tables.assets,
        KeyConditionExpression : "AssetID = :V1",
        ExpressionAttributeValues :  { ':V1' : assetID},
        ProjectionExpression : "Devices"
      };
      shareUtil.awsclient.query(assetsParams, onQuery);
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
            var errmsg = {message: "AssetID does not exist or Asset does not contain any Device"};
            res.status(400).send(errmsg);
          }
          else
          {
            // find index of device in devices list coming from the result of the query in the Asset table
            var devices = data.Items[0].Devices;
            var deviceIndex;
            var index = 0;
            while (index < devices.length)
            {
              console.log("devices.Items[0]: " + devices[index]);
              if (devices[index] == deviceID)
              {
                deviceIndex = index;
                index  = devices.length;
              } else
              {
                index +=1;
              }
            }
          }
          if (index > 0)  // to make sure the update is made after the deviceIndex is found
          {
            if ( typeof deviceIndex == "undefined")
            {
              var msg = "Device not found in Asset's list of Devices";
              shareUtil.SendNotFound(res, msg);
            } else
            {
             console.log("device.index = " + deviceIndex);
             var updateExpr = "remove Devices[" + deviceIndex + "]";
             var updateAsset = {
               TableName : shareUtil.tables.assets,
               Key : {AssetID : assetID},
               UpdateExpression : updateExpr
               //ExpressionAttributeValues : { ':V1' : deviceIndex}
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
                 var deleteParams = {
                   TableName : shareUtil.tables.device,
                   Key : { DeviceID : deviceID }
                 };
                 console.log(deleteParams);
                 shareUtil.awsclient.delete(deleteParams, onDelete);
                 function onDelete(err, data)
                 {
                   if (err)
                    {
                     var msg = "Unable to delete the settings table.( POST /settings) Error JSON:" +  JSON.stringify(err, null, 2);
                     console.error(msg);
                     var errmsg = { message: msg };
                     res.status(500).send(errmsg);
                    } else
                    {
                     var msg = { message: "Success" };
                     console.log("device deleted!");
                     res.status(200).send(msg);
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    }
    else
    {
      console.log("isvalid=false2");
      //var msg = " DeviceID does not exist";
      var errmsg = { message: "DeviceID does not exist" };
      res.status(400).send(errmsg);
    }
  });
  // this sends back a JSON response which is a single string
}

function deleteDevice(req, res) {

  var deviceid = req.swagger.params.DeviceID.value;
  var userid = req.swagger.params.UserID.value;
  var assetid = req.swagger.params.AssetID.value;

  userManage.getDevicesFromUser(userid, function(ret, data) {
    if (ret)
    {
      var devices = data.Devices;
      var deviceIndex = devices.indexOf(deviceid);
      console.log("devices = " + devices);
      console.log("deviceIndex = " + deviceIndex);

      if(devices.length > 0)
      {
        if(deviceIndex > -1)
        {
          // DeviceID is in User
          removeDeviceFromUser(userid, deviceIndex, function(ret1, data1){
            if (ret1)
            {
              deleteDeviceVariables(deviceid, function(ret2, data2) {
                if (ret2)
                {
                  if (assetid)
                  {
                    removeDeviceFromAssetInternal(deviceid, assetid, function(ret3, data3) {
                      if (ret3)
                      {
                        deleteDeviceByID(deviceid, function(ret4, data4) {
                          if (ret4){
                            shareUtil.SendSuccess(res);
                          } else {
                            shareUtil.SendNotFound(res, data4);
                          }
                        });
                      } else
                      {
                        var msg = "DeviceID not found in Asset";
                        shareUtil.SendNotFound(res, data3);
                      }
                    });
                  } else
                  {
                    // no AssetID provided
                    deleteDeviceByID(deviceid, function(ret4, data4) {
                      if (ret4){
                        shareUtil.SendSuccess(res);
                      } else {
                        shareUtil.SendNotFound(res, data4);
                      }
                    });
                  }
                } else
                {
                  // deleteVariables failed
                  var msg = "Error " + JSON.stringify(data2, null, 2);
                  shareUtil.SendNotFound(res, msg);
                }
              });
            } else
            {
              // remove device from User failed
              shareUtil.SendNotFound(res, data1);
            }
          });
        } else
        {
          var msg = "Device Not Found in User";
          shareUtil.SendNotFound(res, msg);
        }
      } else
      {
        var msg = "No Devices found in User";
        shareUtil.SendNotFound(res, msg);
      }
    } else
    {
      var msg = "UserID does not exist or User does not contain any Variable";
      shareUtil.SendNotFound(res, data);
    }
  });
}

function deleteDeviceByID(deviceid, callback) {

  var deleteParams = {
    TableName : shareUtil.tables.device,
    Key : { DeviceID : deviceid }
  };
  shareUtil.awsclient.delete(deleteParams, onDelete);
  function onDelete (err, data) {
    if (err)
    {
      var msg = "Unable to delete the settings table.( POST /settings) Error JSON:" +  JSON.stringify(err, null, 2);
      callback(false, msg);
    } else
    {
      callback(true, null);
    }
  }
}


function removeDeviceFromAssetInternal(deviceid, assetid, callback) {

  asset.getDevicesFromAsset(assetid, function(ret, data) {
    if (ret)
    {
      var deviceIndex = data.Devices.indexOf(deviceid);
      var updateExpr = "remove Devices[" + deviceIndex + "]";

      var updateAsset = {
        TableName : shareUtil.tables.assets,
        Key : {AssetID : assetid},
        UpdateExpression : updateExpr
      };
      shareUtil.awsclient.update(updateAsset, onUpdate);
      function onUpdate(err, data)
      {
        if (err)
        {
          var msg = "Unable to update the settings table.( POST /settings) Error JSON:" +  JSON.stringify(err, null, 2);
          callback(false, msg);
        } else
        {
          console.log("devices deleted from Asset list of Devices!");
          callback(true, null);
        }
      }
    } else
    {
      var msg = "Error:" + JSON.stringify(data, null, 2);
      callback(false, msg);
    }
  });
}

function getVariablesFromDevice(deviceid, callback){

  var devicesParams = {
    TableName : shareUtil.tables.device,
    KeyConditionExpression : "DeviceID = :V1",
    ExpressionAttributeValues :  { ':V1' : deviceid},
    ProjectionExpression : "Variables"
  };
  shareUtil.awsclient.query(devicesParams, onQuery);
  function onQuery(err, data)
  {
    if (err)
    {
    var msg = "Error:" + JSON.stringify(err, null, 2);
    callback(false, msg);
    } else
    {
      if (data.Count == 0)
      {
        var errmsg = {message: "DeviceID does not exist or Device does not contain any Variable"};
        callback(false, msg);
      }
      else
      {
        console.log("data.Items[0] = " + JSON.stringify(data.Items[0], null, 2));
        callback(true, data.Items[0]);
      }
    }
  }
}


function deleteDeviceVariables(deviceid, callback){   // !! Hx.Variable hardcoded !!

  getVariablesFromDevice(deviceid, function(ret, data) {
    if (ret)
    {
      var variables = data.Variables;
      var itemsToDeleteArray = [];

      if (typeof variables == "undefined")
      {
        // Device dooes not contain any Variables
        callback(true, null);
      } else
      {



      for (index in variables){
        var itemToDelete =
        {
          DeleteRequest : {
            Key : {
              "VariableID" : variables[index]
            }
          }
        }
        itemsToDeleteArray.push(itemToDelete);
      }

      var VariableTableName = shareUtil.tables.variable;
      var deviceParams = {
        RequestItems : {
          "Hx.Variable" : itemsToDeleteArray
        }
      }

      console.log(JSON.stringify(deviceParams, null, 2));

      shareUtil.awsclient.batchWrite(deviceParams, onDelete);
      function onDelete(err, data1) {
        if (err)
        {
          console.log("deleteVar failed");
          callback(false, data1);
        }
        else
        {
          console.log("deleteVar success");
          callback(true, null);   // variables deleted from Variable table
        }
      }
    }
    } else
    {
      callback(false, data);
    }
  });
}

function removeDeviceFromUser(userid, index, callback) {

  var updateExpr = "remove Devices[" + index + "]";

  var updateParams = {
    TableName : shareUtil.tables.users,
    Key : {UserID : userid},
    UpdateExpression : updateExpr
  };
  shareUtil.awsclient.update(updateParams, onUpdate);
  function onUpdate(err, data) {
    if (err)
    {
      var msg = "Unable to update the settings table.( POST /settings) Error JSON:" +  JSON.stringify(err, null, 2);
      callback(false, msg);
    } else
    {
      callback(true, null);
    }
  }
}


function getDeviceByAsset(req, res){
  var assetid = req.swagger.params.AssetID.value;
  getDeviceByAssetID(assetid, function(ret, data) {
    if (ret){
      shareUtil.SendSuccessWithData(res, data);
    } else {
      shareUtil.SendNotFound(res, data);
    }
  });
}

function getDeviceByAssetID(assetid, callback) {
  //var assetid = req.swagger.params.AssetID.value;
  var devicesParams = {
    TableName : shareUtil.tables.assets,
    KeyConditionExpression : "AssetID = :V1",
    ExpressionAttributeValues :  { ':V1' : assetid},
    ProjectionExpression : "Devices"
  };
  shareUtil.awsclient.query(devicesParams, onQuery);
  function onQuery(err, data)
  {
    if (err)
    {
      var msg = "Error:" + JSON.stringify(err, null, 2);
      //shareUtil.SendInternalErr(res, msg);
      callback(false, msg);
    } else
     {
      var sendData =
      {
        Items: [],
        Count: 0
      };
      if (data.Count == 0)
      {
        var resErr = {ErrorMsg: "AssetID does not exit or Asset does not contain any Device"};
        console.log(resErr);
        var msg = "AssetID does not exit or Asset does not contain any Device";
        //shareUtil.SendSuccessWithData(res, sendData);
        //shareUtil.SendSuccessWithData(res, resErr);
        callback(false, msg);
      }
      else
      {
        console.log("devices = " + devices);
        console.log("data.count = " + data.Count);
        var devices = data.Items[0].Devices;

        if (typeof devices == "undefined")
        {
          var msg = "undefined";
          console.log(msg);
          //shareUtil.SendSuccessWithData(res, sendData);
          callback(false, msg);
        }
        else
        {
          if (devices.length == 0)
          {
            var msg = "No devices found in Asset";
            console.log(msg);
            //shareUtil.SendSuccessWithData(res, sendData);
            callback(false, msg);
          }
          else
          {
            console.log("devices: " + devices);
            console.log("devices.length = " + devices.length);
            var devicesToDelete = [];
            var deleteIndex = 0;
            getSingleDeviceInternal(0, devices, devicesToDelete, deleteIndex, null, function(devicesdata, devicesToDelete){
              console.log("devicesToDelete -> " + devicesToDelete);
              sendData.Items = devicesdata;
              sendData.Count = devicesdata.length;
              if (devicesToDelete.length == 0){     // no garbage Devices to delete in Asset's list of Devices
                  //shareUtil.SendSuccessWithData(res, sendData);
                  callback(true, sendData);
              } else
              {
                deleteGarbageDevicesInAsset(assetid, devicesToDelete, function(){
                //shareUtil.SendSuccessWithData(res, sendData);
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


//get list of devices by AssetID
function getDeviceByAssetOld(req, res) {
  var assetid = req.swagger.params.AssetID.value;
  var devicesParams = {
    TableName : shareUtil.tables.assets,
    KeyConditionExpression : "AssetID = :V1",
    ExpressionAttributeValues :  { ':V1' : assetid},
    ProjectionExpression : "Devices"
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
      var sendData =
      {
        Items: [],
        Count: 0
      };
      if (data.Count == 0)
      {
        var resErr = {ErrorMsg: "AssetID does not exit or Asset does not contain any Device"};
        console.log(resErr);
        //shareUtil.SendSuccessWithData(res, sendData);
        shareUtil.SendSuccessWithData(res, resErr);
      }
      else
      {
        console.log("devices = " + devices);
        console.log("data.count = " + data.Count);
        var devices = data.Items[0].Devices;

        if (typeof devices == "undefined")
        {
          console.log("undefined");
          shareUtil.SendSuccessWithData(res, sendData);
        }
        else
        {
          if (devices.length == 0)
          {
            console.log("length  = 0");
            shareUtil.SendSuccessWithData(res, sendData);
          }
          else
          {
            console.log("devices: " + devices);
            console.log("devices.length = " + devices.length);
            var devicesToDelete = [];
            var deleteIndex = 0;
            getSingleDeviceInternal(0, devices, devicesToDelete, deleteIndex, null, function(devicesdata, devicesToDelete){
              console.log("devicesToDelete -> " + devicesToDelete);
              sendData.Items = devicesdata;
              sendData.Count = devicesdata.length;
              if (devicesToDelete.length == 0){     // no garbage Devices to delete in Asset's list of Devices
                  shareUtil.SendSuccessWithData(res, sendData);
              } else
              {
                deleteGarbageDevicesInAsset(assetid, devicesToDelete, function(){
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

function deleteGarbageDevicesInAsset(assetid, devicesToDelete, callback) {

  var updateExpr = "remove ";
  for (var k in devicesToDelete)
  {
    updateExpr = updateExpr + "Devices[" + devicesToDelete[k] + "], ";
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
      console.log("devices deleted from Asset list of Devices!");
      callback();
    }
  }
}

function getSingleDeviceInternal(index, devices, devicesToDelete, deleteIndex, deviceout, callback) {
  if (index < devices.length)
  {
    if (index == 0)
    {
      deviceout = [];
    }
    console.log("devices.Items[0]: " + devices[index]);
    var devicesParams = {
      TableName : shareUtil.tables.device,
      KeyConditionExpression : "DeviceID = :v1",
      ExpressionAttributeValues : { ':v1' : devices[index]}
    };
    shareUtil.awsclient.query(devicesParams, onQuery);
    function onQuery(err, data)
    {
      if (!err)
      {
        console.log("no error");
        console.log("data.count = " + data.Count);
        if (data.Count == 1)
        {
          deviceout.push(data.Items[0]);
        //  console.log("deviceout: " + JSON.stringify(deviceout, null, 2));
        }
        else
        {
          devicesToDelete[deleteIndex] = index;
          console.log("devices[index] -> " + devices[index]);
          deleteIndex+= 1;
        }
      }
      getSingleDeviceInternal(index + 1, devices, devicesToDelete, deleteIndex, deviceout, callback);
    }
  }
  else
  {
    callback(deviceout, devicesToDelete);
  }
}



//get list of devices by UserID
function getDeviceByUser(req, res) {
  var userid = req.swagger.params.UserID.value;
  var devicesParams = {
    TableName : shareUtil.tables.users,
    KeyConditionExpression : "UserID = :V1",
    ExpressionAttributeValues :  { ':V1' : userid},
    ProjectionExpression : "Devices"
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
      var sendData =
      {
        Items: [],
        Count: 0
      };
      if (data.Count == 0)
      {
        var resErr = {ErrorMsg: "UserID does not exit or Asset does not contain any Device"};
        console.log(resErr);
        //shareUtil.SendSuccessWithData(res, sendData);
        shareUtil.SendSuccessWithData(res, resErr);
      }
      else
      {
        console.log("devices = " + devices);
        console.log("data.count = " + data.Count);
        var devices = data.Items[0].Devices;

        if (typeof devices == "undefined")
        {
          console.log("undefined");
          shareUtil.SendSuccessWithData(res, sendData);
        }
        else
        {
          if (devices.length == 0)
          {
            console.log("length  = 0");
            shareUtil.SendSuccessWithData(res, sendData);
          }
          else
          {
            console.log("devices: " + devices);
            console.log("devices.length = " + devices.length);
            var devicesToDelete = [];
            var deleteIndex = 0;
            getSingleDeviceInternal(0, devices, devicesToDelete, deleteIndex, null, function(devicesdata, devicesToDelete){
              console.log("devicesToDelete -> " + devicesToDelete);
              sendData.Items = devicesdata;
              sendData.Count = devicesdata.length;
              if (devicesToDelete.length == 0){     // no garbage Devices to delete in Asset's list of Devices
                  shareUtil.SendSuccessWithData(res, sendData);
              } else
              {
                deleteGarbageDevicesInUser(userid, devicesToDelete, function(){
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

function deleteGarbageDevicesInUser(userid, devicesToDelete, callback) {

  var updateExpr = "remove ";
  for (var k in devicesToDelete)
  {
    updateExpr = updateExpr + "Devices[" + devicesToDelete[k] + "], ";
  }

  console.log("updateExpr = " + updateExpr);
  var updateAsset = {
    TableName : shareUtil.tables.users,
    Key : {UserID : userid},
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
      console.log("devices deleted from User list of Devices!");
      callback();
    }
  }
}





function getDeviceAttributes(req, res) {

  var deviceid = req.swagger.params.DeviceID.value;

  var devicesParams = {
    TableName: shareUtil.tables.device,
    KeyConditionExpression : "DeviceID = :v1",
    ExpressionAttributeValues : { ':v1' : deviceid.toString()}
  }

  shareUtil.awsclient.query(devicesParams, onQuery);
  function onQuery(err, data) {
    if (err) {
      var msg =  "Unable to scan the device table.(getDevice) Error JSON:" + JSON.stringify(err, null, 2);
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




function IsDeviceSerialNumberExist(serialNumber, callback) {

  var Params = {
     TableName : shareUtil.tables.device,
     FilterExpression : "SerialNumber = :v1",
     ExpressionAttributeValues : {':v1' : serialNumber.toString()}
  };
  shareUtil.awsclient.scan(Params, onQuery);
  function onQuery(err, data) {
       if (err) {
           var msg = "Error:" + JSON.stringify(err, null, 2);
           shareUtil.SendInternalErr(res, msg);
           var errmsg = { message: msg };
           //res.status(400).send(errmsg);
           console.log("error msg :" + msg);
       } else {
         if (data.Count == 0)
         {
           callback(false, data);
         }
         else {
           callback(true, data);
         }

       }
   }
}



function IsDeviceExist(deviceID, callback) {

  var Params = {
     TableName : shareUtil.tables.device,
     KeyConditionExpression : "DeviceID = :v1",
     ExpressionAttributeValues : {':v1' : deviceID.toString()}
  };
  console.log(deviceID.toString());
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
        console.log("data cout  = 0")
        callback(false, data);
      }
      else
      {
        callback(true, data);
      }
    }
  }
}


function IsUserExist(userID, callback) {

  var Params = {
    TableName : shareUtil.tables.users,
    KeyConditionExpression : "UserID = :v1",
    ExpressionAttributeValues : {':v1' : userID.toString()}
  };
  console.log(userID.toString());
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
        console.log("data cout  = 0")
        callback(false, data);
      } else
      {
      callback(true, data);
      }
    }
  }
}
