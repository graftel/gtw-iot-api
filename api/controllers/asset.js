'use strict';
/*
 'use strict' is not required but helpful for turning syntactical errors into true errors in the program flow
 https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Strict_mode
*/

//test commit

/*
 Modules make it possible to import JavaScript files into your application.  Modules are imported
 using 'require' statements that give you a reference to the module.

  It is a good idea to list the modules that your application depends on in the package.json in the project root
 */
var shareUtil = require('./shareUtil.js');
var userManage = require('./userManage.js');
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
  getAsset: getAssetByUserID,
  getSingleAsset: getSingleAsset,
  addAsset: addAsset,
  updateAsset: updateAsset,
  deleteAsset: deleteAsset,
  IsAssetExist: IsAssetExist
};

/*
  Functions in a127 controllers used for operations should take two parameters:

  Param 1: a handle to the request object
  Param 2: a handle to the response object
 */

function getAssetByUserID(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var userid = req.swagger.params.userID.value;
  // first get assets in UserID

  var assetsParams = {
    TableName : shareUtil.tables.users,
    KeyConditionExpression : "UserID = :v1",
    ExpressionAttributeValues : {':v1' : userid
                               },
    ProjectionExpression : "Assets"
  };
  shareUtil.awsclient.query(assetsParams, onScan);
  function onScan(err, data) {
       if (err) {
           var msg = "Error:" + JSON.stringify(err, null, 2);
           shareUtil.SendInternalErr(res,msg);
       } else {
         var sendData = {
           Items: [],
           Count: 0
         };
         if (data.Count == 0)
         {
           shareUtil.SendSuccessWithData(res, sendData);
         }
         else {
           var assets = data.Items[0].Assets;

           if (typeof assets == "undefined")
           {
             shareUtil.SendSuccessWithData(res, sendData);
           }
           else {
             if (assets.length == 0) {
                shareUtil.SendSuccessWithData(res, sendData);
             }
             else{
               getSingleAssetInternal(0, assets, null, function(assetsdata){
                 sendData.Items = assetsdata;
                 sendData.Count = assetsdata.length;
                 shareUtil.SendSuccessWithData(res, sendData);
               });
             }
           }


         }

       }
   }
  // this sends back a JSON response which is a single string

}

function getSingleAssetInternal(index, assets, assetout, callback) {
    if (index < assets.length){
      if (index == 0)
      {
        assetout = [];
      }
      var assetsParams = {
         TableName : shareUtil.tables.assets,
         KeyConditionExpression : "AssetID = :v1",
         ExpressionAttributeValues : {':v1' : assets[index].AssetID}
      };
      shareUtil.awsclient.query(assetsParams, onScan);
      function onScan(err, data) {
           if (!err) {
             if (data.Count == 1)
             {
                assetout.push(data.Items[0]);
             }

           }
           getSingleAssetInternal(index + 1, assets, assetout, callback);
       }
    }
    else {
      callback(assetout);
    }
}


function getSingleAsset(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var assetID = req.swagger.params.AssetID.value;

  var assetsParams = {
     TableName : shareUtil.tables.assets,
     KeyConditionExpression : "AssetID = :v1",
     ExpressionAttributeValues : {':v1' : assetID.toString()}
  };
  shareUtil.awsclient.query(assetsParams, onScan);
  function onScan(err, data) {
       if (err) {
           var msg = "Unable to scan the assets table.(getAssets) Error JSON:" + JSON.stringify(err, null, 2);
          shareUtil.SendInternalErr(res,msg);
       } else {
         if (data.Count == 0)
         {
            shareUtil.SendNotFound(res);
         }
         else {
           shareUtil.SendSuccessWithData(res, data.Items[0]);
         }

       }
   }
  // this sends back a JSON response which is a single string

}

function addAsset(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var assetobj = req.body;
  console.log(assetobj);
  if(assetobj.constructor === Object && Object.keys(assetobj).length === 0) {
    shareUtil.SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
  }
  else {
    if(!assetobj.DisplayName && !assetobj.UserID)
    {
      shareUtil.SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
    }
    else {
      var uuidv1 = require('uuid/v1');
      var crypto = require('crypto');
      var assetID = uuidv1();
      var params = {
        TableName : shareUtil.tables.assets,
        Item : {
          AssetID: assetID,
          AddTimeStamp: Math.floor((new Date).getTime()/1000),
          LatestTimeStamp: 0,
          DeviceCount: 0
        },
        ConditionExpression : "attribute_not_exists(AssetID)"
      };

      params.Item = Object.assign(params.Item, assetobj);
      delete params.Item['UserID'];

      shareUtil.awsclient.put(params, function(err, data) {
        if (err) {
            var msg = "Error:" + JSON.stringify(err, null, 2);
            console.error(msg);
            shareUtil.SendInternalErr(res,msg);
        }else{
            userManage.updateUserAsset(assetobj.UserID, assetID, function(ret1, data){
                if (ret1){
                  shareUtil.SendSuccess(res);
                }
                else{
                  var msg = "Error:" + JSON.stringify(data);
                  shareUtil.SendInternalErr(res,msg);
                }

            });

        }
      });


    }
  }



  // this sends back a JSON response which is a single string

}

function updateAsset(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var assetobj = req.body;
  var isValid = true;
  if(assetobj.constructor === Object && Object.keys(assetobj).length === 0) {
    isValid  = false;
  }
  else {
    if(!assetobj.AssetID)
    {
      isValid  = false;
    }
    else {
      // check if asset exists
      var assetsParams = {
         TableName : shareUtil.tables.assets,
         ProjectionExpression: ["AssetID","AddTimeStamp","DisplayName","LastestTimeStamp","VerificationCode"],
         FilterExpression : "AssetID = :v1",
         ExpressionAttributeValues : {':v1' : assetobj.AssetID.toString()}
      };
      shareUtil.awsclient.scan(assetsParams, onScan);
      function onScan(err, data) {
           if (err) {
               var msg = "Unable to scan the assets table.(getAssets) Error JSON:" + JSON.stringify(err, null, 2);
               console.error(msg);
               var errmsg = {
                 message: msg
               };
               res.status(500).send(errmsg);
           } else {
             if (data.Count == 0)
             {
               var errmsg = {
                 message: "Items not found"
               };
               res.status(404).send(errmsg);
             }
             else {
               var addTimeStamp = data.Items[0].AddTimeStamp;
               var updateItems = "set ";
               var expressvalues = {};

               if (assetobj.CompanyID)
               {
                 updateItems = updateItems + "CompanyID = :v1,";
                 expressvalues[":v1"] = assetobj.CompanyID.toString();
               }
               if (assetobj.DisplayName)
               {
                 updateItems = updateItems + "DisplayName = :v2,";
                 expressvalues[":v2"] = assetobj.DisplayName.toString();

               }

               if (assetobj.LatestTimeStamp)
               {
                 updateItems = updateItems + "LatestTimeStamp = :v3,";
                 expressvalues[":v3"] = assetobj.LatestTimeStamp;
               }

               if (assetobj.VerificationCode)
               {
                 updateItems = updateItems + "VerificationCode = :v4,";
                 expressvalues[":v4"] = assetobj.VerificationCode.toString();
               }

               updateItems = updateItems.slice(0, -1);

               var updateParams = {
                     TableName : shareUtil.tables.assets,
                     Key : {
                       AssetID : assetobj.AssetID,
                       AddTimeStamp : addTimeStamp
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

           }
       }



    }


    if (!isValid)
    {
      var errmsg = {
        message: "Invalid Input"
      };
      console.log(errmsg);
      res.status(400).send(errmsg);
    }
  }


  // this sends back a JSON response which is a single string

}

function deleteAsset(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var assetID = req.swagger.params.assetID.value;

  var assetsParams = {
     TableName : shareUtil.tables.assets,
     ProjectionExpression: ["AssetID","AddTimeStamp","DisplayName","LastestTimeStamp","VerificationCode"],
     FilterExpression : "AssetID = :v1",
     ExpressionAttributeValues : {':v1' : assetID.toString()}
  };
  shareUtil.awsclient.scan(assetsParams, onScan);
  function onScan(err, data) {
       if (err) {
           var msg = "Unable to scan the assets table.(getAssets) Error JSON:" + JSON.stringify(err, null, 2);
           console.error(msg);
           var errmsg = {
             message: msg
           };
           res.status(500).send(errmsg);
       } else {
         if (data.Count == 0)
         {
           var errmsg = {
             message: "Items not found"
           };
           res.status(404).send(errmsg);
         }
         else {
             var Params = {
                   TableName : shareUtil.tables.assets,
                   Key : {
                   AssetID : data.Items[0].AssetID,
                   AddTimeStamp : data.Items[0].AddTimeStamp
                 }
               };
            shareUtil.awsclient.delete(Params, function (err, data) {
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
                  console.log("asset deleted!");
                  res.status(200).send(msg);
                }
             });
         }

       }
   }
}

function IsAssetExist(assetID, callback) {

  var assetsParams = {
     TableName : shareUtil.tables.assets,
     FilterExpression : "AssetID = :v1",
     ExpressionAttributeValues : {':v1' : assetID.toString()}
  };
  shareUtil.awsclient.scan(assetsParams, onScan);
  function onScan(err, data) {
       if (err) {
           var msg = "Unable to scan the assets table.(getAssets) Error JSON:" + JSON.stringify(err, null, 2);
           console.error(msg);
           var errmsg = {
             message: msg
           };
           res.status(500).send(errmsg);
       } else {
         if (data.Count == 0)
         {
           callback(false);
         }
         else {
           callback(true);
         }

       }
   }
}

function updateSingleAssetKey(asset, assetid, key,  callback){
  var updateParams = {
        TableName : shareUtil.tables.assets,
        Key : {
        AssetID : assetid
      },
      UpdateExpression : "SET " + key.toString() + " = :v",
      ExpressionAttributeValues : {
        ":v" : asset[key]
      }
    };

    console.log(updateParams);
    shareUtil.awsclient.update(updateParams, function (err, data) {
    callback(err,data);
  });
}
