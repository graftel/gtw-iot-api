var shareUtil = require('./shareUtil.js');
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
  addDevice: addDevice,
  addExistingDeviceBySerialNumber: addExistingDeviceBySerialNumber,
  updateDevice: updateDevice,
  deleteDevice: deleteDevice,
  getDevice: getDevice,
  getDeviceParameters: getDeviceParameters
};

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
        callback(false, msg1 );
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
  if (err) {
    var msg = "Error:" + JSON.stringify(err, null, 2);
    callback(false,msg);
  }else{
    if (data.Count == 1) {
      if (typeof data.Items[0].Devices == "undefined")
      {
        callback(true,null);
      }
      else {
        if (data.Items[0].Devices.indexOf(deviceID) > -1) {
          var msg = "Device Already exists in Asset";
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

function addDeviceInternal(deviceobj, res) {
  var uuidv1 = require('uuid/v1');
  var crypto = require('crypto');
  if (typeof deviceobj.DeviceID == "undefined"){
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
    if (err) {
        var msg = "Error:" + JSON.stringify(err, null, 2);
        console.error(msg);
        shareUtil.SendInternalErr(res,msg);
    }else{
        if (deviceobj.AssetID)
        {
            updateDeviceIDInAsset(deviceID, deviceobj.AssetID, function(ret1, data){
              if (ret1){
                shareUtil.SendSuccess(res);
              }
              else{
                var msg = "Error:" + JSON.stringify(data);
                shareUtil.SendInternalErr(res,msg);
              }
             });
        }
        else {
          shareUtil.SendSuccess(res);
        }

    }
  });
}

function addDevice(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var deviceobj = req.body;
  if (deviceobj.SerialNumber) {
    IsDeviceSerialNumberExist(deviceobj.SerialNumber, function(ret,data){
      if (ret) {
        var msg = "Serial Number Already Exists";
        shareUtil.SendInvalidInput(res, msg);
      } else {
        addDeviceInternal(deviceobj, res);
      }
    });
  } else {
    addDeviceInternal(deviceobj, res);
  }
}


function updateDevice(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var deviceobj = req.body;
  var isValid = true;
  console.log(deviceobj);
  if(deviceobj.constructor === Object && Object.keys(deviceobj).length === 0) {
    SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
  }
  else {
    if(!deviceobj.DeviceID)
    {
      var errmsg = {message: "INVALID_INPUT"};
      res.status(400).send(errmsg);
       //SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
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
                if (key != "DeviceID") //&& key != "Type")
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
                    //DeviceID : deviceobj.DeviceID.toString()  //,
                    //Type : data.Items[0].Type
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
            //SendInvalidInput(res,NOT_EXIST);
            var errmsg = {message: "INVALID_INPUT"};
            res.status(400).send(errmsg);
          }
      });
    }
  }
  // this sends back a JSON response which is a single string
}



// Delete device by deviceID
// requires also AssetID in argument to delete the device from the table Asset in the Devices list attribute
function deleteDevice(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var deviceID = req.swagger.params.DeviceID.value;
  var assetID = req.swagger.params.AssetID.value;

  IsDeviceExist(deviceID, function(ret1, data)
  {
    if (ret1)
    {
      if (typeof assetID == "undefined"){   // in case we want to delete a Device that is not in any Asset
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
      else {
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
          if (index > 0)
          {  // to make sure the update is made after the deviceIndex is found
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



function getDevice(req, res) {
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
            getSingleDeviceInternal(0, devices, null, function(devicesdata){
              sendData.Items = devicesdata;
              sendData.Count = devicesdata.length;
              shareUtil.SendSuccessWithData(res, sendData);
            });
          }
        }
      }
    }
  }
}


function getDeviceParameters(req, res) {

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



function getSingleDeviceInternal(index, devices, deviceout, callback) {
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
    function onQuery(err, data) {
      if (!err)
      {
        console.log("no error");
        console.log("data.count = " + data.Count);
        if (data.Count == 1)
        {
          deviceout.push(data.Items[0]);
          console.log("deviceout: " + deviceout);
        }
      }
      getSingleDeviceInternal(index + 1, devices, deviceout, callback);
    }
  }
  else
  {
    callback(deviceout);
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
