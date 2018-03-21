
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
  createUser: createUser,
  updateUser: updateUser,
  authenticate: authenticate,
  getSettings: getSettings,
  updateSettings: updateSettings,
  logIn: logIn,
  activate: activate,
  deleteUser: deleteUser,
  resetUser: resetUser,
  validateResetPasswordLink: validateResetPasswordLink,
  updatePassword: updatePassword,
  updateUserAsset: updateUserAsset
};

function createUser(req, res) {
  var userobj = req.body;
  console.log(userobj);
  if(userobj.constructor === Object && Object.keys(userobj).length === 0) {
    console.log("is valid = false0");
    shareUtil.SendInvalidInput(res);
  }
  else {
    if(!userobj.EmailAddress && !userobj.Password)
    {
      console.log("is valid = false1");
       shareUtil.SendInvalidInput(res);
    }
    else {

      IsEmailExist(userobj.EmailAddress, function(ret1,data){
        if (ret1) { // exists
            console.log("exists");
            shareUtil.SendInvalidInput(res, 'User already exists');
        }
        else {
          // auto generate user id
          var epochtime = Math.floor((new Date).getTime()/1000);
          var uuidv1 = require('uuid/v1');
          var crypto = require('crypto');
          userobj['UserID'] = uuidv1();
          userobj['Created'] = epochtime;
          userobj['VerificationCodeExpire'] = epochtime + 1800; // expire in 30 minutes
          userobj['VerificationCode'] = crypto.randomBytes(20).toString('hex');
          userobj['ApiKey'] = crypto.randomBytes(16).toString('hex');
          userobj['Active'] = 0;
          var params = {
            TableName: shareUtil.tables.users,
            Item : userobj,
            ConditionExpression : "attribute_not_exists(UserID)"
          };
          shareUtil.awsclient.put(params, function(err, data) {
          if (err) {
            var msg = "Error:" + JSON.stringify(err, null, 2);
            console.error(msg);
            shareUtil.SendInternalErr(res,msg);
          }else{
            completeRegistrationEmail(userobj.EmailAddress, userobj.UserID, userobj.VerificationCode);

            shareUtil.SendSuccessWithData(res, userobj);
          }
          });
        }

      });
    }
  }


  // this sends back a JSON response which is a single string
}

function resetUser(req, res) {
  var emailid = req.swagger.params.EmailAddress.value;
  if(emailid)
  {
        IsEmailExist(emailid, function(ret1,data){
          if (ret1) { // exists
            // auto generate user id
            var epochtime = Math.floor((new Date).getTime()/1000);
            var uuidv1 = require('uuid/v1');
            var crypto = require('crypto');
            var updateItems = "set VerificationCodeExpire = :v0, VerificationCode = :v1, Active = :v2";
            var expressvalues = {};
            expressvalues[":v0"] = epochtime + 1800;
            expressvalues[":v1"] = crypto.randomBytes(20).toString('hex');
            expressvalues[":v2"] = 0;

            var updateParams = {
                  TableName : shareUtil.tables.users,
                  Key : {
                    UserID : data.Items[0].UserID,
                },
                UpdateExpression : updateItems,
                ExpressionAttributeValues : expressvalues
              };
            shareUtil.awsclient.update(updateParams, function (err, data1) {
                 if (err) {
                     var msg = "Error:" +  JSON.stringify(err, null, 2);
                     console.error(msg);
                     shareUtil.SendInternalErr(res,'Internal Err');
                 } else {
                   console.log("user reset!");
                   var resetinfo = {
                     VerificationCode: expressvalues[":v1"],
                     UserID: data.Items[0].UserID
                   };
                   sendForgotPasswordEmail(emailid, expressvalues[":v1"]);
                   shareUtil.SendSuccess(res,'Reset Succeed');
                 }
             });

          }
          else {
            console.log("user not exist");
            shareUtil.SendInvalidInput(res, 'User not exists');
          }

        });
  }
  else {
    console.log("is valid = false1");
     shareUtil.SendInvalidInput(res);
  }


  // this sends back a JSON response which is a single string
}

function updateUser(req, res) {
  var userobj = req.body;
  console.log(userobj);
  if(userobj.constructor === Object && Object.keys(userobj).length === 0) {
    console.log("is valid = false0");
    shareUtil.SendInvalidInput(res);
  }
  else {
    if(!userobj.EmailAddress)
    {
      console.log("is valid = false1");
       shareUtil.SendInvalidInput(res);
    }
    else {

      IsEmailExist(userobj.EmailAddress, function(ret1,data){
        if (ret1) { // exists
          var updateItems = "set ";
          var expressvalues = {};

          var i = 0
          for (var key in userobj)
          {
            if (userobj.hasOwnProperty(key))
            {
              if (key != "EmailAddress")
              {
                updateItems = updateItems + key.toString() + " = :v" + i.toString() + ",";
                expressvalues[":v" + i.toString()] = userobj[key];
                i++;
              }
            }
          }

          updateItems = updateItems.slice(0, -1);

          var updateParams = {
                TableName : shareUtil.tables.users,
                Key : {
                  UserID : data.Items[0].UserID,
              },
              UpdateExpression : updateItems,
              ExpressionAttributeValues : expressvalues
            };
          console.log(updateParams);
          shareUtil.awsclient.update(updateParams, function (err, data) {
               if (err) {
                   var msg = "Error:" +  JSON.stringify(err, null, 2);
                   console.error(msg);
                   var errmsg = {
                     message: msg
                   };
                   res.status(500).send(errmsg);
               } else {
                 var msg = {
                   message: "Success"
                 };
                 console.log("user updated!");
                 res.status(200).send(msg);
               }
           });
        }
        else {
          console.log("exists");
          shareUtil.SendInvalidInput(res, 'User Not exists');
        }

      });
    }
  }


  // this sends back a JSON response which is a single string
}


function updateUserAsset(userID, assetID, callback) {
  if(!userID)
  {
    callback(false, null);
  }
  else {
    var updateParams = {
          TableName : shareUtil.tables.users,
          Key : {
            UserID : userID,
                },
        UpdateExpression : 'set #assets = list_append(if_not_exists(#assets, :empty_list), :id)',
        ExpressionAttributeNames: {
            '#assets': 'Assets'
          },
        ExpressionAttributeValues: {
          ':id': [assetID],
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
}


function getSettings(req, res) {
  var userid = req.swagger.params.userID.value;

  if (typeof userid == "undefined")
  {
    shareUtil.SendInvalidInput(res);
  }
  else {

        var params = {
          TableName: shareUtil.tables.users,
          KeyConditionExpression : "UserID = :v1",
          ExpressionAttributeValues : {':v1' : userid.toString()}
        };
        console.log(params)
        shareUtil.awsclient.query(params, function(err, data) {
        if (err) {
          var msg = "Error:" + JSON.stringify(err, null, 2);
          console.error(msg);
          shareUtil.SendInternalErr(res,msg);
        }else{
          console.log(data);
          if (data.Count == 1) {
            if (typeof data.Items[0].Settings.ParameterList == "undefined")
            {
              var msg = "Error: Cannot find data"
              shareUtil.SendInvalidInput(res,shareUtil.constants.NOT_EXIST);
            }
            else {
              shareUtil.SendSuccessWithData(res, data.Items[0].Settings.ParameterList);
            }

          }
          else {
              var msg = "Error: Cannot find data"
             shareUtil.SendInvalidInput(res,shareUtil.constants.NOT_EXIST);
          }

        }
      });
  }

  // this sends back a JSON response which is a single string
}

function singleDeleteUser(userid, callback){
  var params = {
    TableName: shareUtil.tables.users,
      Key : {
        UserID : userid
      }
  };
  console.log(params)
  shareUtil.awsclient.delete(params, function(err, data) {
    callback(err,data);
  });
}

function deleteUser(req, res) {
  var userid = req.swagger.params.userID.value;

  if (typeof userid == "undefined")
  {
    shareUtil.SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
  }
  else {
      singleDeleteUser(userid, function(err,data){
        if(err){
          var msg = "Error:" + JSON.stringify(err, null, 2);
          console.error(msg);
          shareUtil.SendInternalErr(res,msg);
        }
        else{
          shareUtil.SendSuccess(res);
        }

      });
  }

  // this sends back a JSON response which is a single string
}

function updateSettings(req, res) {
  var settingobj = req.body;
  console.log(settingobj);
  if(settingobj.constructor === Object && Object.keys(settingobj).length === 0) {
    console.log("is valid = false0");
    shareUtil.SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
  }
  else {

    var updateItems = "set ";
    var expressvalues = {};

    var i = 0
    for (var key in settingobj.parameters)
    {
      if (settingobj.parameters.hasOwnProperty(key))
      {
        if (key != "DeviceID" && key != "Type")
        {
          updateItems = updateItems + "Settings.ParameterList." + key.toString() + " = :v" + i.toString() + ",";
          expressvalues[":v" + i.toString()] = settingobj.parameters[key];
          i++;
        }
      }
    }

    updateItems = updateItems.slice(0, -1);

    var updateParams = {
          TableName : shareUtil.tables.users,
          Key : {
            UserID : settingobj.UserID,
        },
        UpdateExpression : updateItems,
        ExpressionAttributeValues : expressvalues
      };
    console.log(updateParams);
    shareUtil.awsclient.update(updateParams, function (err, data) {
         if (err) {
             var msg = "Error:" +  JSON.stringify(err, null, 2);
             console.error(msg);
             var errmsg = {
               message: msg
             };
             res.status(500).send(errmsg);
         } else {
           var msg = {
             message: "Success"
           };
           console.log("user setting updated!");
           res.status(200).send(msg);
         }
     });
  }
}
function updatePassword(req, res)
{
  var emailid = req.swagger.params.EmailAddress.value;
  var password = req.swagger.params.Password.value;

  if (emailid && password){
    IsEmailExist(emailid,function(ret1,data){
      if(ret1){

        var updateItems = "set Password = :v0, Active = :v1";
        var expressvalues = {};
        expressvalues[":v0"] = password;
        expressvalues[":v1"] = 1;

        var updateParams = {
              TableName : shareUtil.tables.users,
              Key : {
                UserID : data.Items[0].UserID,
            },
            UpdateExpression : updateItems,
            ExpressionAttributeValues : expressvalues
          };
        shareUtil.awsclient.update(updateParams, function (err, data1) {
             if (err) {
                 var msg = "Error:" +  JSON.stringify(err, null, 2);
                 console.error(msg);
                 shareUtil.SendInternalErr(res,'Internal Err');
             } else {
               console.log("user password updated!");
               shareUtil.SendSuccess(res);
             }
         });
      }
      else{
        shareUtil.SendInvalidInput(res, 'User not exists');
      }
    });
  }
  else {
    shareUtil.SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
  }
}

function validateResetPasswordLink(req, res)
{
  var emailid = req.swagger.params.EmailAddress.value;
  var vercode = req.swagger.params.VerificationCode.value;

  if (emailid && vercode){
    IsEmailExist(emailid,function(ret1,data){
      if(ret1){
        if (vercode === data.Items[0].VerificationCode){
          var epochtime = Math.floor((new Date).getTime()/1000);
          if (epochtime > data.Items[0].VerificationCodeExpire)
          {
            shareUtil.SendInvalidInput(res, 'Verification Code Expired');
          }
          else{
            shareUtil.SendSuccess(res);
          }
        }
        else{
          shareUtil.SendInvalidInput(res, 'Verification Code Not Match');
        }
      }
      else{
        shareUtil.SendInvalidInput(res, 'User not exists');
      }
    });
  }
  else {
    shareUtil.SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
  }
}

function activate(req, res) {
  var userid = req.swagger.params.UserID.value;
  var actkey = req.swagger.params.ActivateKey.value;
  if (typeof userid == "undefined" || typeof actkey == "undefined")
  {
    shareUtil.SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
  }
  else {

        var params = {
          TableName: shareUtil.tables.users,
          KeyConditionExpression : "UserID = :v1",
          ExpressionAttributeValues : {':v1' : userid.toString()}
        };
        shareUtil.awsclient.query(params, function(err, data) {
        if (err) {
          var msg = "Error:" + JSON.stringify(err, null, 2);
          console.error(msg);
          shareUtil.SendInternalErr(res,msg);
        }else{
          if (data.Count == 1) {
            if (typeof data.Items[0].VerificationCode == "undefined")
            {
              var msg = "Error: Cannot find data"
              shareUtil.SendInvalidInput(res,msg);
            }
            else {
              if (actkey === data.Items[0].VerificationCode)
              {
                // check HashExpire
                var epochtime = (new Date).getTime() / 1000;
                if (data.Items[0].VerificationCodeExpire > epochtime){

                  activateUser(userid, function(err){
                    if (err){
                        console.log("cannot activate user");
                    }
                    else{
                      console.log("activate user succeed");
                      shareUtil.SendSuccess(res, 'Activation Succeed');
                    }
                  });


                }
                else{
                  var msg = "Register expired, please register again";
                  // delete user here
                  shareUtil.SendInvalidInput(res,msg);
                }

              }
              else{
                var msg = "Cannot verify VerificationCode";
                shareUtil.SendInvalidInput(res,msg);
              }
            }

          }
          else {
              var msg = "Error: Cannot find data"
             shareUtil.SendInvalidInput(res,msg);
          }

        }
      });
  }

  // this sends back a JSON response which is a single string
}

function authenticate(apikey, callback) {
  var Params = {
   TableName : shareUtil.tables.users,
   FilterExpression : "ApiKey = :v1",
   ExpressionAttributeValues : {':v1' : apikey.toString()}
};
  shareUtil.awsclient.scan(Params, onScan);
  function onScan(err, data) {
     if (err) {
         var err_msg = "Error:" + JSON.stringify(err, null, 2);
         var msg = {
           message: err_msg
         }
         callback(false, msg);
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

function logIn(req, res){
  var userobj = req.body;
  console.log(userobj);
  if(userobj.constructor === Object && Object.keys(userobj).length === 0) {
    console.log("is valid = false0");
    shareUtil.SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
  }
  else {
    if(!userobj.EmailAddress && !userobj.Password)
    {
      console.log("is valid = false1");
       shareUtil.SendInvalidInput(res, shareUtil.constants.INVALID_INPUT);
    }
    else {

      IsEmailExist(userobj.EmailAddress, function(ret1,data){
        if (ret1) { // exists
          // check password
          if (data.Items[0].Password == userobj.Password)
          {
             if (data.Items[0].Active == 0)
             {
               singleDeleteUser(data.Items[0].UserID, function(err,data){
                  shareUtil.SendInvalidInput(res, 'Please register again');
               });
             }
             else {
                shareUtil.SendSuccessWithData(res, data.Items[0]);
             }

          }
          else {
            shareUtil.SendInvalidInput(res, 'Wrong credentials');
          }
        }
        else {
          shareUtil.SendInvalidInput(res, 'Wrong credentials');
        }

      });
    }
  }


  // this sends back a JSON response which is a single string
}

function activateUser(userid, callback)
{
  var updateParams = {
        TableName : shareUtil.tables.users,
        Key : {
          UserID : userid,
      },
      UpdateExpression : "set Active=:v0",
      ExpressionAttributeValues : {":v0": 1}
    };
  console.log(updateParams);
  shareUtil.awsclient.update(updateParams, function (err, data) {
      callback(err);
   });

}

function IsEmailExist(email, callback){
  var Params = {
   TableName : shareUtil.tables.users,
   FilterExpression : "EmailAddress = :v1",
   ExpressionAttributeValues : {':v1' : email.toString()}
};
  shareUtil.awsclient.scan(Params, onScan);
  function onScan(err, data) {
     if (err) {
         var msg = "Error:" + JSON.stringify(err, null, 2);
         shareUtil.SendInternalErr(res,msg);
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



function sendForgotPasswordEmail(emailid, VerificationCode) {
  const sgMail = require('@sendgrid/mail');
  sgMail.setApiKey(require('../../sg.js').SENDGRID_API_KEY);
  var link =  "http://localhost:3000/reset-password?email=" + emailid + "&code=" + VerificationCode;
  const msg = {
    to: emailid,
    from: 'no-reply@graftel.com',
    subject: 'Reset password for IIoT Monitering System',
    html: "Hello,<br><br>You recently requested to reset password for your Heat Exchange Monitering System"
        + " account.<br><br>To reset your password, click the following link or copy and paste the link into your browser: <br><a href='"
        + link
        + "'>Click Here</a>"
        + "<br><br>After resetting your password, in order to access your account, you will need to put in your email address and password "
        + "in the Log In section.<br><br>This password reset link will expire in 30 minutes "
        + "<br><br>If you did not request to have your password reset you can safely ignore this email.<br><br>"
        + "If you need further assistance please contact our support team at http://www.graftel.com/contact/.<br><br>Thank you,<br>Graftel Team.",
  };
  sgMail.send(msg);
}

function completeRegistrationEmail(emailid, userid, verificationCode) {
  const sgMail = require('@sendgrid/mail');
  sgMail.setApiKey(require('../../sg.js').SENDGRID_API_KEY);
  var link =  "http://localhost:3000/activate?id=" + userid + "&code=" + verificationCode;
  const msg = {
    to: emailid,
    from: 'no-reply@graftel.com',
    subject: 'Complete your registration for Heat Exchange Monitering System',
    html: "Hello,<br><br>Thank you for registering with HxMonitor.io"
        + "<br><br>To complete your registration please use below link: <br><br>"
        + '<a href="'+link+'">' + "Click Here</a><br><br>"
        + "If you need further assistance please contact our support team at http://www.graftel.com/contact/.<br><br>Thank you,<br>Graftel Team",
  };
  sgMail.send(msg);
}
