var util = require('util');
var AWS = require("aws-sdk");
const os = require('os');
AWS.config.loadFromPath(os.homedir() + '/.aws/config.json');
var docClient = new AWS.DynamoDB.DocumentClient();

const INVALID_INPUT = "Invalid Input";
const ALREADY_EXIST = "Item Already Exist";
const SUCCESS_MSG = "Success";
const NOT_EXIST = "Item Not Exist";
const INTERNAL_ERR = "Internal Error";

var tables = {
    users: "Hx.Users",
    assets: "Hx.Asset",
    device: "Hx.Device",
    rawData: "Hx.RawData",
    calculatedData: "Hx.CalculatedData",
    variable: "Hx.Variable",
    parameter: "Hx.Parameters"
};


module.exports = {
  tables: tables,
  constants: {
    INVALID_INPUT: INVALID_INPUT,
    ALREADY_EXIST: ALREADY_EXIST,
    SUCCESS_MSG: SUCCESS_MSG,
    NOT_EXIST: NOT_EXIST,
    INTERNAL_ERR: INTERNAL_ERR
  },
  awsclient: docClient,
  SendInvalidInput: SendInvalidInput,
  SendSuccess: SendSuccess,
  SendSuccessWithData: SendSuccessWithData,
  SendInternalErr: SendInternalErr,
  SendNotFound: SendNotFound
};


function SendInvalidInput(res, msg = INVALID_INPUT)
{
  var errmsg = {
    message: msg
  };
  console.log(errmsg);
  res.status(400).send(errmsg);
}

function SendNotFound(res, msg = NOT_EXIST)
{
  var errmsg = {
    message: msg
  };
  console.log(errmsg);
  res.status(404).send(errmsg);
}

function SendSuccess(res, msg = SUCCESS_MSG)
{
  var errmsg = {
    message: msg
  };
  console.log(errmsg);
  res.status(200).send(errmsg);
}
function SendInternalErr(res, msg = INTERNAL_ERR)
{
  var errmsg = {
    message: msg
  };
  console.log(errmsg);
  res.status(500).send(errmsg);
}

function SendSuccessWithData(res, data_out)
{
  res.status(200).send(data_out);
}
