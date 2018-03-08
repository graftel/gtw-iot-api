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
  getDevice: getDevice
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
    SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
  }
  else {
    if(!deviceobj.AssetID || !deviceobj.SerialNumber || !deviceobj.VerificationCode)
    {
      console.log("is valid = false1");
       SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
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
            SendInvalidInput(res,"Wrong VerificationCode");
          }
        } else {
          SendInvalidInput(res,"Serial Number Not exist");
        }
      });
    }
  }
}

function addDeviceInternal(deviceobj, res) {
  var uuidv1 = require('uuid/v1');
  var crypto = require('crypto');
  var deviceID = uuidv1();
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
        if (deviceobj.assetID)
        {
            updateDeviceIDInAsset(deviceID, deviceobj.assetID, function(ret1, data){
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
       SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
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
                if (key != "DeviceID" && key != "Type")
                {
                  updateItems = updateItems + key.toString() + " = :v" + i.toString() + ",";
                  expressvalues[":v" + i.toString()] = deviceobj[key];
                  i++;
                }
              }
            }

            updateItems = updateItems.slice(0, -1);

            var updateParams = {
                  TableName : shareUtil.tables.deviceConfig,
                  Key : {
                    DeviceID : data.Items[0].DeviceID,
                    Type : data.Items[0].Type
                },
                UpdateExpression : updateItems,
                ExpressionAttributeValues : expressvalues
              };
            console.log(updateParams);
            docClient.update(updateParams, function (err, data) {
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
            SendInvalidInput(res,NOT_EXIST);
          }
      });
    }
  }



  // this sends back a JSON response which is a single string

}


function deleteDevice(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var deviceID = req.swagger.params.DeviceID.value;
  // check if asset exists
  IsDeviceExist(deviceID, function(ret1, data){
      if (ret1) {
        var deleteParams = {
              TableName : shareUtil.tables.deviceConfig,
              Key : {
                DeviceID : data.Items[0].DeviceID,
                Type : data.Items[0].Type
            }
          };
        console.log(deleteParams);
        docClient.delete(deleteParams, function (err, data) {
             if (err) {
                 var msg = "Unable to delete the settings table.( POST /settings) Error JSON:" +  JSON.stringify(err, null, 2);
                 console.error(msg);
                 var errmsg = {
                   message: msg
                 };
                 res.status(500).send(errmsg);
             } else {
               var msg = {
                 message: "Success"
               };
               console.log("deivce deleted!");
               res.status(200).send(msg);
             }
         });
      }
      else {
        console.log("isvalid=false2");
        SendInvalidInput(res,NOT_EXIST);
      }
  });



  // this sends back a JSON response which is a single string

}

function getDevice(req, res) {
  var assetID = req.swagger.params.AssetID.value;
  var Params = {
     TableName : shareUtil.tables.deviceConfig,
     FilterExpression : "AssetID = :v1",
     ExpressionAttributeValues : {':v1' : assetID.toString()}
  };
  console.log(Params);
  shareUtil.awsclient.scan(Params, onScan);
  function onScan(err, data) {
       if (err) {
           var msg = "Error:" + JSON.stringify(err, null, 2);
           shareUtil.SendInternalErr(res,msg);
       } else {
         if (data.Count == 0)
         {
           shareUtil.SendNotFound(res);
         }
         else {
           shareUtil.SendSuccessWithData(res, data);
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
  shareUtil.awsclient.scan(Params, onScan);
  function onScan(err, data) {
       if (err) {
           var msg = "Error:" + JSON.stringify(err, null, 2);
           SendInternalErr(res,msg);
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
     TableName : shareUtil.tables.deviceConfig,
     FilterExpression : "DeviceID = :v1",
     ExpressionAttributeValues : {':v1' : deviceID.toString()}
  };
  docClient.scan(Params, onScan);
  function onScan(err, data) {
       if (err) {
           var msg = "Error:" + JSON.stringify(err, null, 2);
           SendInternalErr(res,msg);
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