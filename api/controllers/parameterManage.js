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
  addParam: addParam,
  updateParam: updateParam,
  deleteParam: deleteParam,
  getParamByAssetID: getParamByAssetID,
  getParamAttributes: getParamAttributes
};

function updateParamIDInAsset(paramID, assetID, callback) {
  if(!assetID)
  {
    callback(false, null);
  }
  else {
    checkParamInAsset(paramID, assetID, function(ret, msg1) {
      if (ret) {
        var updateParams = {
          TableName : shareUtil.tables.assets,
          Key : {
            AssetID : assetID,
                },
          UpdateExpression : 'set #param = list_append(if_not_exists(#param, :empty_list), :id)',
          ExpressionAttributeNames: {
            '#param': 'Parameters'
          },
          ExpressionAttributeValues: {
            ':id': [paramID],
            ':empty_list': []
          }
        };

        shareUtil.awsclient.update(updateParams, function (err, data) {
            if (err) {
                var msg = "Error:" +  JSON.stringify(err, null, 2);
                console.error(msg);
                callback(false, msg);
            } else {
                callback(true, null);
            }
        });
      }
      else {
        callback(false, msg1 );
      }
    });

  }
}

function checkParamInAsset(paramID, assetID, callback) {

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
      if (typeof data.Items[0].Params == "undefined")
      {
        callback(true,null);
      }
      else {
        if (data.Items[0].Params.indexOf(paramID) > -1) {
          var msg = "Param Already exists in Asset";
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


function addParamInternal(paramobj, res) {
  var uuidv1 = require('uuid/v1');
  var crypto = require('crypto');
  if (typeof paramobj.ParamID == "undefined"){
    var paramID = uuidv1();
  }
  else
  {
    var paramID = paramobj.ParamID;
  }

  var params = {
    TableName : shareUtil.tables.param,
    Item : {
      ParamID: paramID,
      AddTimeStamp: Math.floor((new Date).getTime()/1000)
    },
    ConditionExpression : "attribute_not_exists(ParamID)"
  };
  params.Item = Object.assign(params.Item, paramobj);
  delete params.Item['AssetID'];

  shareUtil.awsclient.put(params, function(err, data) {
    if (err) {
        var msg = "Error:" + JSON.stringify(err, null, 2);
        console.error(msg);
        shareUtil.SendInternalErr(res,msg);
    }else{
        if (paramobj.AssetID)
        {
            updateParamIDInAsset(paramID, paramobj.AssetID, function(ret1, data){
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

function addParam(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var paramobj = req.body;
  if (paramobj.ParamID) {
    IsParamExist(paramobj.ParamID, function(ret,data){
      if (ret) {
        var msg = "ParamID Already Exists";
        shareUtil.SendInvalidInput(res, msg);
      } else {
        addParamInternal(paramobj, res);
      }
    });
  } else {
    addParamInternal(paramobj, res);
  }
}


function updateParam(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var paramobj = req.body;
  var isValid = true;
  console.log(paramobj);
  if(paramobj.constructor === Object && Object.keys(paramobj).length === 0) {
    shareUtil.SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
  }
  else {
    if(!paramobj.ParamID)
    {
      shareUtil.SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
    }
    else {
      // check if asset exists
      IsParamExist(paramobj.ParamID, function(ret1, data){
          if (ret1) {
            var updateItems = "set ";
            var expressvalues = {};
            var expressnames = {};

            var i = 0
            for (var key in paramobj)
            {
              if (paramobj.hasOwnProperty(key))
              {
                if (key != "ParamID")
                {
                  //updateItems = updateItems + key.toString() + " = :v" + i.toString() + ",";

                  expressvalues[":v" + i.toString()] = paramobj[key];
                  expressnames["#n" + i.toString()] = key.toString();
                  updateItems = updateItems + "#n" + i.toString() + " = :v" + i.toString() + ",";
                  i++;
                }
              }
            }

            updateItems = updateItems.slice(0, -1);

            var updateParams = {
                  TableName : shareUtil.tables.param,
                  Key : {
                    ParamID: data.Items[0].ParamID
                },
                UpdateExpression : updateItems,
                ExpressionAttributeValues : expressvalues,
                ExpressionAttributeNames : expressnames
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



// Delete param by paramID
// requires also AssetID in argument to delete the param from the table Asset in the Params list attribute
function deleteParam(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var paramID = req.swagger.params.ParamID.value;
  var assetID = req.swagger.params.AssetID.value;

  IsParamExist(paramID, function(ret1, data)
  {
    if (ret1)
    {
      if (typeof assetID == "undefined")
      {   // in case we want to delete a Param that is not in any Asset
        var deleteParams = {
          TableName : shareUtil.tables.param,
          Key : { ParamID : paramID }
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
            console.log("param deleted!");
            res.status(200).send(msg);
          }
        }
      }
      else
      {
      console.log("assetID = " + assetID);
      // 1st -> get index of param to delete
      var assetsParams = {
        TableName : shareUtil.tables.assets,
        KeyConditionExpression : "AssetID = :V1",
        ExpressionAttributeValues :  { ':V1' : assetID},
        ProjectionExpression : "#p",
        ExpressionAttributeNames : {"#p" : "Parameters" }
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
          console.log("else entered");
          if (data.Count == 0)
          {
            var msg = "AssetID does not exist";
            shareUtil.SendNotFound(res, msg);
          }
          else
          {
            console.log("else 2 entered");
            // find index of param in params list coming from the result of the query in the Asset table
            var param = data.Items[0].Parameters;
            var paramIndex;
            var index = 0;
            if ( typeof param == "undefined" || param.length == 0){
            //  var errmsg = {message: "Asset does not contain any Param"};
              //res.status(400).send(errmsg);
              var msg = "Asset does not contain any Param";
              shareUtil.SendNotFound(res, msg);
            }
            else
            {
              console.log("else 3 entered");
              console.log("param.length = " + param.length);
              while (index < param.length)
              {
                console.log("param.Items[0]: " + param[index]);
                if (param[index] == paramID)
                {
                  paramIndex = index;
                  index  = param.length;
                } else
                {
                  index +=1;
                }
              }
            }
          }
          if (index > 0)
          {  // to make sure the update is made after the paramIndex is found
            console.log("parameters.index = " + paramIndex);
            var updateExpr = "remove #p[" + paramIndex + "]";
            var updateAsset = {
              TableName : shareUtil.tables.assets,
              Key : {AssetID : assetID},
              UpdateExpression : updateExpr,
              ExpressionAttributeNames : {'#p' : 'Parameters'}
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
                  TableName : shareUtil.tables.param,
                  Key : { ParamID : paramID }
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
                    console.log("param deleted!");
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
      //var msg = " ParamID does not exist";
      var errmsg = { message: "ParamID does not exist" };
      res.status(400).send(errmsg);
    }
  });
  // this sends back a JSON response which is a single string
}



function getParamByAssetID(req, res) {
  var assetid = req.swagger.params.AssetID.value;
  var parametersParams = {
    TableName : shareUtil.tables.assets,
    KeyConditionExpression : "AssetID = :V1",
    ExpressionAttributeValues :  { ':V1' : assetid},
    ProjectionExpression : "#p",
    ExpressionAttributeNames : {"#p" : "Parameters" }
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
        var parameters = data.Items[0].Parameters;
        console.log("parameters = " + parameters);
        console.log("data.count = " + data.Count);


        if (typeof parameters == "undefined")
        {
          console.log("Error msg : Parameters undefined");
          msg = "No Parameters found in this Asset";
          shareUtil.SendNotFound(res, msg);
        }
        else
        {
          if (parameters.length == 0)
          {
            console.log("Error msg: Parameters.length  = 0");
            msg = "No Parameters found in this Asset";
            shareUtil.SendNotFound(res, msg);
          }
          else
          {
            console.log("parameters: " + parameters);
            console.log("parameters.length = " + parameters.length);
            var parametersToDelete = [];
            var deleteIndex = 0;
            getSingleParamInternal(0, parameters, assetid, parametersToDelete, deleteIndex,null, function(paramsdata, parametersToDelete){
              sendData.Items = paramsdata;
              sendData.Count = paramsdata.length;
              if(parametersToDelete.length == 0)
              {
              shareUtil.SendSuccessWithData(res, sendData);
            } else
            {
              deleteGarbageParameters(sendData, assetid, parametersToDelete, function(sendData){
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



function deleteGarbageParameters(sendData, assetid, parametersToDelete, callback) {

  var updateExpr = "remove ";
  for (var k in parametersToDelete)
  {
    updateExpr = updateExpr + "#params[" + parametersToDelete[k] + "], ";
  }

  console.log("updateExpr = " + updateExpr);
  var updateAsset = {
    TableName : shareUtil.tables.assets,
    Key : {AssetID : assetid},
    UpdateExpression : updateExpr.slice(0, -2),        // slice to delete ", " at the end of updateExpr
    ExpressionAttributeNames : {'#params' : 'Parameters'}
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
      callback(sendData);
    }
  }
}





function getParamAttributes(req, res) {

  var paramid = req.swagger.params.ParamID.value;

  var paramsParams = {
    TableName: shareUtil.tables.param,
    KeyConditionExpression : "ParamID = :v1",
    ExpressionAttributeValues : { ':v1' : paramid.toString()}
  }

  shareUtil.awsclient.query(paramsParams, onQuery);
  function onQuery(err, data) {
    if (err) {
      var msg =  "Unable to scan the param table.(getParam) Error JSON:" + JSON.stringify(err, null, 2);
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



function getSingleParamInternal(index, params, assetid, parametersToDelete, deleteIndex, paramout, callback) {
  if (index < params.length)
  {
    if (index == 0)
    {
      paramout = [];
    }
    console.log("params.Items[0]: " + params[index]);
    var paramsParams = {
      TableName : shareUtil.tables.param,
      KeyConditionExpression : "ParamID = :v1",
      ExpressionAttributeValues : { ':v1' : params[index]}
    };
    shareUtil.awsclient.query(paramsParams, onQuery);
    function onQuery(err, data) {
      if (!err)
      {
        console.log("no error");
        console.log("data.count = " + data.Count);
        if (data.Count == 1)
        {
          paramout.push(data.Items[0]);
          console.log("paramout: " + paramout);
        }
        else
        {
          parametersToDelete[deleteIndex] = index;
          console.log("parameters[index] -> " + params[index]);
          deleteIndex+= 1;
        }
      }
      getSingleParamInternal(index + 1, params, assetid, parametersToDelete, deleteIndex, paramout, callback);
    }
  }
  else
  {
    callback(paramout, parametersToDelete);
  }
}


function IsParamExist(paramID, callback) {

  var Params = {
     TableName : shareUtil.tables.param,
     KeyConditionExpression : "ParamID = :v1",
     ExpressionAttributeValues : {':v1' : paramID.toString()}
  };
  console.log(paramID.toString());
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
