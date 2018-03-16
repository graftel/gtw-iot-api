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
  addVariable: addVariable,
  updateVariable: updateVariable,
  deleteVariable: deleteVariable,
  getVariable: getVariable,
  getVariableParameters: getVariableParameters
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


function addVariableInternal(variableobj, res) {
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
        if (variableobj.DeviceID)
        {
            updateVariableIDInDevice(variableID, variableobj.DeviceID, function(ret1, data){
              if (ret1){
                shareUtil.SendSuccess(res);
              }
              else{
                var msg = "Error:" + JSON.stringify(data);
                shareUtil.SendInternalErr(res,msg);
              }
             });
        }
        else
        {
          shareUtil.SendSuccess(res);
        }

    }
  });
}

function addVariable(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var variableobj = req.body;
  if (variableobj.VariableID) {
    IsVariableExist(variableobj.VariableID, function(ret,data){
      if (ret) {
        var msg = "VariableID Already Exists";
        shareUtil.SendInvalidInput(res, msg);
      } else {
        addVariableInternal(variableobj, res);
      }
    });
  } else {
    addVariableInternal(variableobj, res);
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
      if (typeof deviceID == "undefined")
      {   // in case we want to delete a Variable that is not in any Device
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
        // 1st -> get index of variable to delete
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
              res.status(400).send(errmsg);
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
            if (index > 0)  // to make sure the update is made after the variableIndex is found
            {
              if (typeof variableIndex == "undefined"){
                var msg = "Variable not found in Device's list of Variables";
                shareUtil.SendNotFound(res, msg);
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
                    res.status(500).send(errmsg);
                  } else
                  {
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
                        res.status(500).send(errmsg);
                      } else
                      {
                        var msg = { message: "Success" };
                        console.log("variable deleted!");
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
      var errmsg = { message: "VariableID does not exist" };
      res.status(400).send(errmsg);
    }
  });
  // this sends back a JSON response which is a single string
}


// get list of Variable by DeviceID
function getVariable(req, res) {
  var deviceid = req.swagger.params.DeviceID.value;
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
        var resErr = {ErrorMsg: "DeviceID does not exit"};
        console.log(resErr);
        //shareUtil.SendSuccessWithData(res, sendData);
        var errmsg = { message: "DeviceID does not exist" };
        res.status(400).send(errmsg);      }
      else
      {
        var variables = data.Items[0].Variables;
        console.log("variables = " + variables);
        console.log("data.count = " + data.Count);


        if (typeof variables == "undefined")
        {
          console.log("undefined");
          var errmsg = { message: "DeviceID does not contain any variable" };
          res.status(400).send(errmsg);
          //shareUtil.SendSuccessWithData(res, sendData);
        }
        else
        {
          if (variables.length == 0)
          {
            console.log("length  = 0");
            shareUtil.SendSuccessWithData(res, sendData);
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
                shareUtil.SendSuccessWithData(res, sendData);
              } else
              {
                deleteGarbageVariables(sendData, deviceid, variablesToDelete, function(sendData) {
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


function deleteGarbageVariables(sendData, deviceid, variablesToDelete, callback) {

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
    console.log("variables.Items[0]: " + variables[index]);
    var variablesParams = {
      TableName : shareUtil.tables.variable,
      KeyConditionExpression : "VariableID = :v1",
      ExpressionAttributeValues : { ':v1' : variables[index]}
    };
    shareUtil.awsclient.query(variablesParams, onQuery);
    function onQuery(err, data) {
      if (!err)
      {
        console.log("no error");
        console.log("data.count = " + data.Count);
        if (data.Count == 1)
        {
          variableout.push(data.Items[0]);
          //rsconsole.log("variableout: " + variableout);
        } else
        {
          variablesToDelete[deleteIndex] = index;
          console.log("variables[index] -> " + variables[index]);
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

function getVariableParameters(req, res) {

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
