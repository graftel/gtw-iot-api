
var shareUtil = require('./shareUtil.js');
var variableManage = require('./variableManage.js');
var deviceManage = require('./deviceManage.js');
var userManage = require('./userManage.js');

var levelup = require('levelup');
var leveldown = require('leveldown');
var dbCache = shareUtil.dbCache;
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
  getSingleDataByVariableID: getSingleDataByVariableID,
  getMultipleDataByVariableID: getMultipleDataByVariableID,
  addDataByDeviceID: addDataByDeviceID,
  addDataByVariableID: addDataByVariableID,
  addDataByDeviceName: addDataByDeviceName,
  addDataBySerialNumber: addDataBySerialNumber,
  fillBatchGetItem: fillBatchGetItem
};


function fillDataArray(dataArray, timestamp, itemsToAddArray, index, callback) {
  if (index < dataArray.length)
  {
    var variableid = dataArray[index].VariableID;
    var value = dataArray[index].Value;
    if (variableid)
    {
      if(value)
      {
        variableManage.IsVariableExist(variableid, function(ret, data) {
          if (ret)
          {
            if(timestamp)
            {
              var itemToAdd =
              {
                PutRequest : {
                  Item : {
                    "VariableID" : variableid,
                    "Value" : value,
                    "EpochTimeStamp" : timestamp
                  }
                }
              }
            } else // no timestamp provided
            {
              var itemToAdd =
              {
                PutRequest : {
                  Item : {
                    "VariableID" : variableid,
                    "Value" : value,
                    "EpochTimeStamp" : Math.floor((new Date).getTime()/1000)
                  }
                }
              }
            }
            itemsToAddArray.push(itemToAdd);
          } else
          {
            var msg = "VariableID: " + variableid + " does not exist";
            callback(false, msg);
          }
          fillDataArray(dataArray, timestamp, itemsToAddArray, index + 1, callback);
        });
      } else
      {
        var msg = "missing Value for VariableID: " + variableid + " (item number " + index + ")";
        callback(false, msg);
      }
    } else
    {
      var msg = "missing VariableID for item number " + index;
      callback(false, msg);
    }
  } else {
    callback(true, itemsToAddArray);
  }
}

function batchAddData(itemsToAddArray, callback) {
  var dataParams = {
    RequestItems : {
      "Hx.Data" : itemsToAddArray
    }
  }
  //console.log("dataParams = " + JSON.stringify(dataParams, null, 2));
  shareUtil.awsclient.batchWrite(dataParams, onPut);
  function onPut(err, data) {
    if (err) {
      //console.log(JSON.stringify(dataParams, null, 2));
      var msg = "Error:" +  JSON.stringify(err, null, 2);
      //console.error(msg);
      callback(false,msg);
    } else {
      //console.log("write items succeeded !");
      callback(true, null);
    }
  }
}

function addDataByVariableID(req, res) {     // !! Hx.Data hardcoded !!

  var dataobj = req.body;
  var dataArray = dataobj.Data;
  var timestamp = dataobj.Timestamp;
  var itemsToAddArray = [];

  fillDataArray(dataArray, timestamp, itemsToAddArray, 0, function(ret, data){
    if(ret)
    {
      batchAddData(itemsToAddArray, function(ret, data) {
        if (ret) {
          shareUtil.SendSuccess(res);
        } else {
          shareUtil.SendInternalErr(res, data);
        }
      });
    } else
    {
      shareUtil.SendNotFound(res, data);
    }
  });
}

function addDataBySerialNumber(req, res) {
  var serialNumber = req.swagger.params.SerialNumber.value;
  var timestamp = req.body.Timestamp;
  var Data = req.body.Data;

  if (Data.Data.length != 0){
    deviceManage.getDeviceIdBySerialNumber(serialNumber, function(ret, data) {
      var deviceid = data;
      ////console.log("deviceid = " + JSON.stringify(deviceid, null, 2));
      addDataByDeviceIDInternal(deviceid, Data, timestamp, function(ret, data) {
        if (ret) {
          shareUtil.SendSuccess(res);
        } else {
          var msg = "Error: " + JSON.stringify(data, null, 2);
          shareUtil.SendInternalErr(res, msg);
        }
      });
    });
  } else {
    var msg = "No data provided";
    shareUtil.SendInvalidInput(res, msg);
  }
}

function addDataByDeviceName(req, res) {
  var deviceName = req.swagger.params.DeviceName.value;
  var apiKey = req.headers["x-api-key"];
  var Data = req.body.Data;
  var timestamp = req.body.Timestamp;

  if (Data.Data.length != 0) {
    userManage.getUserbyApiKeyQuery(apiKey, function (ret, data) {
      if (ret) {
        var devices = data.Items[0].Devices;
        deviceManage.getDevicesDisplayName(devices, function(ret1, data1) {
          if (ret1) {
            var devIDtoNameMap = data1.Responses[shareUtil.tables.device];
            var devObj = {};
            convertDevIDtoDevNameArrayIntoObj(devIDtoNameMap, devObj, 0, function(ret2, data2) {
              if (ret2) {
                var deviceid = data2[deviceName];
                addDataByDeviceIDInternal(deviceid, Data, timestamp, function(ret, data) {
                  if (ret) {
                    shareUtil.SendSuccess(res);
                  } else {
                    var msg = "Error: " + JSON.stringify(data, null, 2);
                    shareUtil.SendInternalErr(res, msg);
                  }
                });
              } else {
                shareUtil.SendInvalidInput(res);
              }
            });
          } else {
            shareUtil.SendInvalidInput(res, data);
          }
        });
      } else {
        shareUtil.SendInvalidInput(res, data);
      }
    });
  } else {
    var msg = "No data provided";
    shareUtil.SendInvalidInput(res, msg);
  }
}

function convertDevIDtoDevNameArrayIntoObj(devIDtoNameMap, devObj, index, callback) {
  if (index < devIDtoNameMap.length) {
    devid = devIDtoNameMap[index].DeviceID;
    devName = devIDtoNameMap[index].DisplayName;
    devObj[devName] = devid
    convertDevIDtoDevNameArrayIntoObj(devIDtoNameMap, devObj, index + 1, callback);
  } else {
    callback(true, devObj);
  }
}

function addDataByDeviceIDInternal(deviceid, data, timestamp, callback) {
  if (!timestamp) {
    timestamp = Math.floor((new Date).getTime()/1000);
    //console.log("timestamp = " + timestamp);
  }
  if(deviceid){
    deviceManage.getVariablesFromDevice(deviceid, function(ret1, data1){
      if (ret1) {
        var variableidList = data1.Variables;
        var getItems = [];
        ////console.log("variableidList = " + variableidList);
        if(variableidList){
          batchGetItem(variableidList, getItems, function(ret2, data2){
            if(ret2){
              var varIDtoNameMap = data2.Responses["Hx.Variable"];
              console.log("data2 = " + JSON.stringify(data2, null, 2));
              var dataObj = {};
              convertDataArrToObj(data.Data, dataObj, 0, function(ret3, data3) {
                if (ret3) {
                  var varObj = {};
                  convertVarIDtoVarNameArrayIntoObj(varIDtoNameMap, varObj, 0, function(ret7, data7) {
                    if (ret7) {
                      var valueToVarIDMap = [];
                      mapValueToVarID(data3, data7, valueToVarIDMap, 0, deviceid, function(ret4, data4) {
                        if (ret4) {
                          ////console.log("data4 = " + JSON.stringify(data4, null, 2));
                          var itemsToAddArray = [];
                          fillDataArray(data4, timestamp, itemsToAddArray, 0, function(ret5, data5) {
                            if (ret5) {
                              ////console.log("data5 = " + JSON.stringify(data5, null, 2));
                              batchAddData(data5, function(ret6, data6) {
                                if (ret6) {
                                  callback(true);
                                } else {
                                  callback(false, data6);
                                }
                              });
                            } else {
                              callback(false);
                            }
                          });
                        } else {
                          callback(false, data4);
                        }
                      });
                    } else {
                      callback(false);
                    }
                  })
                } else {
                  callback(false);
                }
              });
            } else {
              callback(false, data2);
            }
          });
        } else {
          // Case whre there is no Variable inside the Device
          var varIDtoNameMap = [];
          ////console.log("varIDtoNameMap = " + JSON.stringify(varIDtoNameMap, null, 2));
          var dataObj = {};
          convertDataArrToObj(data.Data, dataObj, 0, function(ret3, data3) {
            if (ret3) {
              var varObj = {};
              convertVarIDtoVarNameArrayIntoObj(varIDtoNameMap, varObj, 0, function(ret7, data7) {
                if (ret7) {
                  var valueToVarIDMap = [];
                  mapValueToVarID(data3, data7, valueToVarIDMap, 0, deviceid, function(ret4, data4) {
                    if (ret4) {
                      ////console.log("data4 = " + JSON.stringify(data4, null, 2));
                      var itemsToAddArray = [];
                      fillDataArray(data4, timestamp, itemsToAddArray, 0, function(ret5, data5) {
                        if (ret5) {
                          ////console.log("data5 = " + JSON.stringify(data5, null, 2));
                          batchAddData(data5, function(ret6, data6) {
                            if (ret6) {
                              callback(true);
                            } else {
                              callback(false, data6);
                            }
                          });
                        } else {
                          callback(false);
                        }
                      });
                    } else {
                      callback(false, data4);
                    }
                  });
                } else {
                  callback(false);
                }
              })
            } else {
              callback(false);
            }
          });
        }
      } else { // no Variables found in Device
        callback(false);
      }
    });
  } else {
      var msg = "DeviceID missing";
      callback(false, msg);
  }
}

function addDataByDeviceIDInternal2(deviceid, data, timestamp, callback) {
  if (!timestamp) {
    timestamp = Math.floor((new Date).getTime()/1000);
    //console.log("timestamp = " + timestamp);
  }
  if(deviceid){
    dbCache.get(deviceid, function(err, value) {
      if (err) {
        console.log('get error', err);
        deviceManage.getVariablesFromDevice(deviceid, function(ret1, data1){
          if (ret1) {
            var variableidList = data1.Variables;
            var getItems = [];
            if(variableidList){
              batchGetItem(variableidList, getItems, function(ret2, data2){
                if(ret2){
                  var varIDtoNameMap = data2.Responses["Hx.Variable"];
                  console.log("data2 = " + JSON.stringify(data2, null, 2));
                  var dataObj = {};
                  convertDataArrToObj(data.Data, dataObj, 0, function(ret3, data3) {
                    if (ret3) {
                      var varObj = {};
                      convertVarIDtoVarNameArrayIntoObj(varIDtoNameMap, varObj, 0, function(ret7, data7) {
                        if (ret7) {
                          var devCache = {};
                          converVarNametoVarIDtArrayIntoObj(varIDtoNameMap, devCache, 0, function(ret8, data8) {
                            if (ret8) {
                              console.log("data8 = " + JSON.stringify(data8, null, 2));
                              //var dataCache = JSON.stringify(data8, null, 2);
                              //console.log("dataCache = " + dataCache)
                              /*dbCache.put(deviceid, dataCache, function(err) {
                                if(err) {
                                  return console.log('put error', err);
                                } /*else {
                                  dbCache.get(deviceid, function(err, value) {
                                    if (err) {
                                      return console.log('get error', err);
                                    } else {
                                      console.log('value = ' + value);
                                      var obj = JSON.parse(value);
                                      console.log("obj = " + JSON.stringify(obj, null, 2));
                                      console.log("obj.testvar2 = " + obj.testvar2);
                                    }
                                  });
                                }
                              });*/
                              var valueToVarIDMap = [];
                              mapValueToVarID2(data3, data8, valueToVarIDMap, 0, deviceid, function(ret4, data4) {
                                if (ret4) {
                                  ////console.log("data4 = " + JSON.stringify(data4, null, 2));
                                  var itemsToAddArray = [];
                                  fillDataArray(data4, timestamp, itemsToAddArray, 0, function(ret5, data5) {
                                    if (ret5) {
                                      ////console.log("data5 = " + JSON.stringify(data5, null, 2));
                                      batchAddData(data5, function(ret6, data6) {
                                        if (ret6) {
                                          callback(true);
                                        } else {
                                          callback(false, data6);
                                        }
                                      });
                                    } else {
                                      callback(false);
                                    }
                                  });
                                } else {
                                  callback(false, data4);
                                }
                              });
                            } else {
                              callback(false);
                            }
                          });
                        } else {
                          callback(false);
                        }
                      });
                    } else {
                      callback(false);
                    }
                  });
                } else {
                  callback(false, data2);
                }
              });
            } else {
              // Case whre there is no Variable inside the Device
              var varIDtoNameMap = [];
              ////console.log("varIDtoNameMap = " + JSON.stringify(varIDtoNameMap, null, 2));
              var dataObj = {};
              convertDataArrToObj(data.Data, dataObj, 0, function(ret3, data3) {
                if (ret3) {
                  var varObj = {};
                  convertVarIDtoVarNameArrayIntoObj(varIDtoNameMap, varObj, 0, function(ret7, data7) {
                    if (ret7) {
                      var valueToVarIDMap = [];
                      mapValueToVarID2(data3, data7, valueToVarIDMap, 0, deviceid, function(ret4, data4) {
                        if (ret4) {
                          ////console.log("data4 = " + JSON.stringify(data4, null, 2));
                          var itemsToAddArray = [];
                          fillDataArray(data4, timestamp, itemsToAddArray, 0, function(ret5, data5) {
                            if (ret5) {
                              ////console.log("data5 = " + JSON.stringify(data5, null, 2));
                              batchAddData(data5, function(ret6, data6) {
                                if (ret6) {
                                  callback(true);
                                } else {
                                  callback(false, data6);
                                }
                              });
                            } else {
                              callback(false);
                            }
                          });
                        } else {
                          callback(false, data4);
                        }
                      });
                    } else {
                      callback(false);
                    }
                  })
                } else {
                  callback(false);
                }
              });
            }
          } else { // no Variables found in Device
            callback(false);
          }
        });
      } else {    // cache for device exist
        console.log('value = ' + value);
        var obj = JSON.parse(value);
        console.log("obj = " + JSON.stringify(obj, null, 2));
        console.log("obj.testvar2 = " + obj.testvar2);
        var valueToVarIDMap = [];
        var dataObj = {};
        var variablesNotInCache = {};
        convertDataArrToObj(data.Data, dataObj, 0, function(ret3, data3) {
          if (ret3) {
            mapValueToVarID2(data3, obj, valueToVarIDMap, 0, deviceid, function(ret4, data4) {
              if (ret4) {
                var itemsToAddArray = [];
                fillDataArray(data4, timestamp, itemsToAddArray, 0, function(ret5, data5) {
                  if (ret5) {
                    batchAddData(data5, function(ret6, data6) {
                      if (ret6) {
                        callback(true);
                      } else {
                        callback(false, data6);
                      }
                    });
                  } else {
                    callback(false);
                  }
                });
              } else {
                callback(false, data4);
              }
            });
          } else {
            callback(false);
          }
        });
      //  callback(true);
      }
    });
  } else {
      var msg = "DeviceID missing";
      callback(false, msg);
  }
}

function addDataByDeviceID(req, res) {
  var deviceid = req.swagger.params.DeviceID.value;
  var dataobj = req.body;
  var data = dataobj.Data;
  var timestamp = dataobj.Timestamp;

  if(data.Data.length != 0) {
    addDataByDeviceIDInternal2(deviceid, data, timestamp, function(ret, data) {
      if (ret) {
        shareUtil.SendSuccess(res);
      } else {
        var msg = "Error: " + JSON.stringify(data, null, 2);
        shareUtil.SendInternalErr(res, msg);
      }
    });
  } else {
    var msg = "No data provided";
    shareUtil.SendInvalidInput(res, msg);
  }
}

function mapValueToVarID(varNameToValueMap, varIDtoNameMap, valueToVarIDMap, index, deviceid, callback) {
    //console.log("varNameToValueMap = " + JSON.stringify(varNameToValueMap, null, 2));
    //console.log(" length = " + Object.keys(varNameToValueMap).length);
  if (index < Object.keys(varNameToValueMap).length) {
    var varName = Object.keys(varNameToValueMap)[index];
    var varValue = varNameToValueMap[varName];
    var item = {};
    var indexOfName = Object.values(varIDtoNameMap).indexOf(varName);
    if (indexOfName > -1) {
      item.VariableID = Object.keys(varIDtoNameMap)[indexOfName];
      //console.log(item.VariableID);
      item.Value = varValue;
      valueToVarIDMap.push(item);
      mapValueToVarID(varNameToValueMap, varIDtoNameMap, valueToVarIDMap, index+1, deviceid, callback);
    } else {
      //create a new varid
      var uuidv1 = require('uuid/v1');
      var variableID = uuidv1();
      createNewVariableFromName(varName, variableID, deviceid, function(ret, data){
        item.VariableID = variableID;
        //console.log(item.VariableID);
        item.Value = varValue;
        valueToVarIDMap.push(item);
        mapValueToVarID(varNameToValueMap, varIDtoNameMap, valueToVarIDMap, index+1, deviceid, callback);
      });
    }
  } else {
    callback(true, valueToVarIDMap);
    //console.log("valueToVarIDMap = " + JSON.stringify(valueToVarIDMap));
  }
}

function mapValueToVarID2(varNameToValueMap, varNametoIDMap, valueToVarIDMap, index, deviceid, callback) {

  if (index < Object.keys(varNameToValueMap).length) {
    var varName = Object.keys(varNameToValueMap)[index];
    var varValue = varNameToValueMap[varName];
    var item = {};
    if (varNametoIDMap[varName]) {
      item.VariableID = varNametoIDMap[varName];
      item.Value = varValue;
      valueToVarIDMap.push(item);
      mapValueToVarID2(varNameToValueMap, varNametoIDMap, valueToVarIDMap, index+1, deviceid, callback);
    } else {
      //create a new varid
      var uuidv1 = require('uuid/v1');
      var variableID = uuidv1();
      createNewVariableFromName(varName, variableID, deviceid, function(ret, data){
        item.VariableID = variableID;
        item.Value = varValue;
        valueToVarIDMap.push(item);
        mapValueToVarID2(varNameToValueMap, varNametoIDMap, valueToVarIDMap, index+1, deviceid, callback);
      });
    }
  } else {
    callback(true, valueToVarIDMap);
    //console.log("valueToVarIDMap = " + JSON.stringify(valueToVarIDMap));
  }
}

function mapValueToVarID3(varNameToValueMap, varNametoIDMap, valueToVarIDMap, index, deviceid, variablesNotInCache, callback) {

  if (index < Object.keys(varNameToValueMap).length) {
    var varName = Object.keys(varNameToValueMap)[index];
    var varValue = varNameToValueMap[varName];
    var item = {};
    if (varNametoIDMap[varName]) {
      item.VariableID = varNametoIDMap[varName];
      item.Value = varValue;
      valueToVarIDMap.push(item);
      mapValueToVarID3(varNameToValueMap, varNametoIDMap, valueToVarIDMap, index+1, deviceid, variablesNotInCache, callback);
    } else {
      //create a new varid
      variablesNotInCache.varName = varValue;
      mapValueToVarID3(varNameToValueMap, varNametoIDMap, valueToVarIDMap, index+1, deviceid, variablesNotInCache, callback);
    }
  } else {
    if (Object.keys(variablesNotInCache).length == 0) {
      callback(true, valueToVarIDMap);
      console.log("valueToVarIDMap = " + JSON.stringify(valueToVarIDMap, null, 2));
    } else {
      handleVariablesNotInCache(deviceid, variablesNotInCache, function(ret, data) {
        if (ret) {

        } else {

        }
      });
    }
  }
}

function handleVariablesNotInCache(deviceid, variablesNotInCache, callback) {
  getVariableNameFromDevice(deviceid, function(ret, data) {
    if (ret) {
      if (data != null) {
        var variablesToAddToCache = {};
        var variablesToCreate = {};
        checkIfVariableExistInDevice(deviceid, variablesNotInCache, data, variablesToAddToCache, variablesToCreate, 0, function(ret, data) {
          if (ret) {
            if (Object.keys(variablesToAddToCache).length > 0) {
              addMultipleVarToCache(variablesToAddToCache, function(ret1, data1) {

              });
            }
            if (Object.keys(variablesToCreate).length > 0) {
              createNewVariablesFromName(variablesToCreate, deviceid, valueToVarIDMap, 0, function(ret2, data2) {

              });
            }
          }
        });
      } else {    //No variable in Device
        createNewVariablesFromName(variablesNotInCache, deviceid, valueToVarIDMap, 0, function(ret1, data) {
          if (ret1) {
            callback(true, valueToVarIDMap);
          } else {    // problem in creating new devices
            callback(false);
          }
        });
      }
    } else {    // did not get variables names
      callback(false);
    }
  });
}

function checkIfVariablesExistInDevice(deviceid, variablesNotInCache, varIDtoNameMap, variablesToAddToCache, variablesToCreate, index, callback) {
  if (index < Object.keys(variablesNotInCache).length) {
    var varName = Object.keys(variablesNotInCache)[index];
    var variableValue = variablesNotInCache[varName];
    if (varIDtoNameMap.indexOf(varName) > -1) {   // variable exist already in Device but wasn't in the cache
      variablesToAddToCache.varName = variableValue;
      checkIfVariablesExistInDevice(deviceid, variablesNotInCache, varIDtoNameMap, variablesToAddToCache, variablesToCreate, index+1, callback);
    } else {
      variablesToCreate.varName = variableValue;
      checkIfVariablesExistInDevice(deviceid, variablesNotInCache, varIDtoNameMap, variablesToAddToCache, variablesToCreate, index+1, callback);
    }
  } else {
    callback(true, variablesToAddToCache, variablesToCreate);
  }
}

function getVariableNameFromDevice(deviceid, callback) {
  deviceManage.getVariablesFromDevice(deviceid, function(ret, data){
    if (ret) {
      var variableidList = data.Variables;
      var getItems = [];
      if (variableidList || variableidList.length == 0) {
        batchGetItem(variableidList, getItems, function(ret1, data1){
          if (ret1) {
            var varIDtoNameMap = data1.Responses["Hx.Variable"];
            console.log("data1 = " + JSON.stringify(data1, null, 2));
            var dataObj = {};
            convertDataArrToObj(data.Data, dataObj, 0, function(ret2, data2) {
              if (ret2) {
                callback(true, dataObj);
              } else {
                callback(false);
              }
            });
          } else {
            callback(false);
          }
        });
      } else {
        console.log("variableidList empty");
        callback(true, null);
      }
    } else {
      callback(false);
    }
  });
}

function createNewVariablesFromName(variablesToCreate, deviceid, valueToVarIDMap, index, callback) {
  if (index < Object.keys(variablesToCreate).length) {
    var varName = Object.keys(variablesToCreate)[index];
    var uuidv1 = require('uuid/v1');
    var variableID = uuidv1();
    createNewVariableFromName(varName, variableID, deviceid, function(ret, data){
      item.VariableID = variableID;
      //console.log(item.VariableID);
      item.Value = varValue;
      valueToVarIDMap.push(item);
      createNewVariablesFromName(variablesToCreate, deviceid, valueToVarIDMap, index+1, callback);
  } else {
    callback(true, valueToVarIDMap);
  }
}

function createNewVariableFromName(varName, varID, deviceid, callback) {

  var params = {
    TableName : shareUtil.tables.variable,
    Item : {
      VariableID: varID,
      AddTimeStamp: Math.floor((new Date).getTime()/1000),
      VariableName: varName
    },
    ConditionExpression : "attribute_not_exists(VariableID)"
  };
  shareUtil.awsclient.put(params, function(err, data) {
    if (err) {
      var msg = "Error:" + JSON.stringify(err, null, 2);
      callback(false, msg);
    } else {
        updateVariableIDInDevice(varID, deviceid, function(ret1, data1){
        if (ret1) {
          console.log("var " + varID + " created !");
          callback(true, null);
        } else {
          var msg = "Error:" + JSON.stringify(data1) + "update failed";
          callback(false, msg);
        }
      });
    }
  });
}

function convertVarIDtoVarNameArrayIntoObj(varIDtoNameMap, varObj, index, callback) {
  if( index < varIDtoNameMap.length) {
    varid = varIDtoNameMap[index].VariableID;
    varName = varIDtoNameMap[index].VariableName;
    varObj[varid] = varName;
    convertVarIDtoVarNameArrayIntoObj(varIDtoNameMap, varObj, index + 1, callback);
  } else {
    callback(true, varObj);
  }
}

function converVarNametoVarIDtArrayIntoObj(varIDtoNameMap, varObj, index, callback) {
  if( index < varIDtoNameMap.length) {
    varid = varIDtoNameMap[index].VariableID;
    varName = varIDtoNameMap[index].VariableName;
    varObj[varName] = varid;
    converVarNametoVarIDtArrayIntoObj(varIDtoNameMap, varObj, index + 1, callback);
  } else {
    callback(true, varObj);
  }
}

function convertDataArrToObj(dataArray, dataObj, index, callback) {

  if (index < dataArray.length) {
    //var dataSorted = {};
    var key = Object.keys(dataArray[index]);
    var value2 = Object.values(dataArray[index]);
    dataObj[key] = value2[0];
    ////console.log("dataSorted = " + JSON.stringify(dataObj, null, 2));
    convertDataArrToObj(dataArray, dataObj, index + 1, callback);
  } else {
    callback(true, dataObj);
  //  //console.log(" dataObj = " +  JSON.stringify(dataObj, null, 2));
  }
}

function batchGetItem(variableidList, getItems, callback) {
  fillBatchGetItem(variableidList, getItems, 0, function(ret, data) {
    if (ret) {
      var dataParams = {
        RequestItems : {
          "Hx.Variable" : {
            Keys : data,
            ProjectionExpression : "VariableID, VariableName"
          }
        }
      }
      shareUtil.awsclient.batchGet(dataParams, onGet);
      function onGet(err, data1) {
        if (err) {
          var msg = "Error:" +  JSON.stringify(err, null, 2);
          callback(false, msg);
        } else {
          callback(true, data1);
        }
      }
    } else {
      callback(false, data);
    }
  });
}

function fillBatchGetItem(variableidList, getItems, index, callback) {
  if (index < variableidList.length) {
    var getItem = {
      "VariableID" : variableidList[index]
    }
    getItems.push(getItem);
    fillBatchGetItem(variableidList, getItems, index+1, callback);
  } else {
    callback(true, getItems);
  }
}

function addSingleData(deviceid, dataobj, index, callback) {

  if (index < dataobj.Data.length)
  {
    var variableName = dataobj.Data[index].VariableName;
    ////console.log("variableName = " + variableName);
    var nameParams = {
      TableName: shareUtil.tables.variable,
      FilterExpression : "VariableName = :v1",
      ExpressionAttributeValues : {':v1' : variableName},
      ProjectionExpression : "VariableID"
    }
    shareUtil.awsclient.scan(nameParams, onScan);
    function onScan(err, data) {

      if (err)
      {
        var msg = "Error:" + JSON.stringify(err, null, 2);
        shareUtil.SendInternalErr(res, msg);
      } else
      {
        if (data.Count == 0)
        {
          var msg = "data.Count == 0"
          //shareUtil.SendNotFound(res, msg);
          // have to create a new var name
          createNewVariable(deviceid, dataobj, index, function(){
            addSingleData(deviceid, dataobj, index+1, callback);
          });
        } else
        {
          // varName found so add Value to RawData
          var variableid = data.Items[0].VariableID;
          ////console.log("variableid = " + variableid);
          var timestamp = dataobj.Timestamp;
          var value = dataobj.Data[index].Value;

          var dataParams = {
            TableName : shareUtil.tables.data,
            Item : {
              VariableID : variableid,
              EpochTimeStamp : timestamp,
              Value : value
            }
          }
          ////console.log("dataParams = "  + JSON.stringify(dataParams, null, 2));

          shareUtil.awsclient.put(dataParams, onPut);
          function onPut(err, data)
          {
            if (err)
            {
              var msg = "Error:" + JSON.stringify(err, null, 2);
              shareUtil.SendInternalErr(res, msg);
            } else
            {
              //shareUtil.SendSuccess(res);
              //update current value in Variable table
              var updateVarParams = {
                TableName : shareUtil.tables.variable,
                Key : {
                  VariableID: variableid
                },
                UpdateExpression : "set CurrentValue = :v1",
                ExpressionAttributeValues : {':v1' : value}
              }

              shareUtil.awsclient.update(updateVarParams, function(err, data) {
                if (err)
                {
                  var msg = "Unable to update the settings table.( POST /settings) Error JSON:" +  JSON.stringify(err, null, 2);
                  console.error(msg);
                } else
                {
                   //console.log("device updated!");
                   addSingleData(deviceid, dataobj, index+1, callback);
                }
              });
            }
          //  addSingleData(deviceid, dataobj, index+1, callback);
          }
        }
      }
    }
  } else
  {
    return callback();
  }
}

function createNewVariable(deviceid, dataobj, index, callback) {

  var uuidv1 = require('uuid/v1');
  var crypto = require('crypto');
  var variableid = uuidv1();
  var variableName = dataobj.Data[index].VariableName;
  var timestamp = dataobj.Timestamp;
  var variableValue = dataobj.Data[index].Value;

  var params = {
    TableName : shareUtil.tables.variable,
    Item : {
      VariableID: variableid,
      AddTimeStamp: timestamp,
      VariableName: variableName,
      CurrentValue: variableValue
    },
    ConditionExpression :  "attribute_not_exists(VariableID)"
  };

  shareUtil.awsclient.put(params, onPut);
  function onPut(err, data) {
    if (err)
    {
      var msg = "Error:" + JSON.stringify(err, null, 2);
      shareUtil.SendInternalErr(res,msg);
    } else
    {
      updateVariableIDInDevice(variableid, deviceid, function(ret1, data){
        if (ret1)
        {
          //shareUtil.SendSuccess(res);
          //console.log("timestamp in update = " + timestamp);
          addRawData(variableid, timestamp, variableValue, function() {
            addSingleData(deviceid, dataobj, index, callback);
          });
        }
        else{
          var msg = "Error:" + JSON.stringify(data);
          shareUtil.SendInternalErr(res,msg);
        }
       });
    }
  }
}

function updateVariableIDInDevice(variableID, deviceID, callback) {
  if(!deviceID)
  {
    return callback(false, null);
  }
  else {
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
      } else
      {
        return callback(true,null);
      }
    });
  }
}

function addRawData(variableid, timestamp, value, callback) {

  var dataParams = {
    TableName : shareUtil.tables.data,
    Item : {
      VariableID : variableid,
      EpochTimeStamp : timestamp,
      Value : value
    }
  }
  ////console.log("dataParams = "  + JSON.stringify(dataParams, null, 2));
  ////console.log("timestamp = " + timestamp);

  shareUtil.awsclient.put(dataParams, onPut);
  function onPut(err, data)
  {
    if (err)
    {
      var msg = "Error:" + JSON.stringify(err, null, 2);
      //console.log(msg);
      //shareUtil.SendInternalErr(res, msg);
    } else
    {
      //shareUtil.SendSuccess(res);
      return callback();
    }
  }
}

function getSingleDataByVariableID(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var variableID = req.swagger.params.VariableID.value;
  var dataTimeStamp = req.swagger.params.TimeStamp.value;

   var params = {
     TableName: shareUtil.tables.data,
     KeyConditionExpression : "VariableID = :v1 and EpochTimeStamp = :v2",
     ExpressionAttributeValues : {':v1' : variableID.toString(),
                                  ':v2' : dataTimeStamp}
   };
   ////console.log(params)
   shareUtil.awsclient.query(params, function(err, data) {
   if (err) {
     var msg = "Error:" + JSON.stringify(err, null, 2);
     console.error(msg);
     shareUtil.SendInternalErr(res,msg);
   }else{
     ////console.log(data);
     if (data.Count == 0)
     {
       var msg = "Error: Cannot find data"
        shareUtil.SendInvalidInput(res, msg);
     }
     else if (data.Count == 1)
     {
        var out_data = {'Value' : data.Items[0]["Value"]};
        ////console.log(out_data);
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
  ////console.log(params)
  docClient.query(params, function(err, data) {
  if (err)
  {
    var msg = "Error:" + JSON.stringify(err, null, 2);
    console.error(msg);
    shareUtil.SendInternalErr(res,msg);
  } else
  {
    //console.log(data);
    if (data.Count == 0)
    {
      var msg = "Error: Cannot find data"
      shareUtil.SendInvalidInput(res,NOT_EXIST);
    }
    else if (data.Count == 1)
    {
      shareUtil.SendSuccessWithData(res, data.Items[0]);
    }
    else
    {
      var msg = "Error: data count is not 1"
      shareUtil.SendInternalErr(res,msg);
     }
    }
  });
}

function getMultipleDataByVariableID(req, res) {
  // variables defined in the Swagger document can be referenced using req.swagger.params.{parameter_name}
  var variableID = req.swagger.params.VariableID.value;
  var dataTimeStampFrom = req.swagger.params.StartTimeStamp.value;
  var dataTimeStampTo = req.swagger.params.EndTimeStamp.value;

   var params = {
     TableName: shareUtil.tables.data,
     KeyConditionExpression : "VariableID = :v1 and EpochTimeStamp between :v2 and :v3",
     ExpressionAttributeValues : {':v1' : variableID.toString(),
                                  ':v2' : dataTimeStampFrom,
                                  ':v3' : dataTimeStampTo}
   };
   //console.log(params)
   shareUtil.awsclient.query(params, function(err, data) {
   if (err) {
     var msg = "Error:" + JSON.stringify(err, null, 2);
     console.error(msg);
     shareUtil.SendInternalErr(res,msg);
   }else{
     ////console.log(data);
     if (data.Count == 0)
     {
      var msg = "Error: Cannot find data"
      shareUtil.SendInvalidInput(res, msg);
     }
     else {
        delete data["ScannedCount"];
        shareUtil.SendSuccessWithData(res, data);
     }
   }
 });
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
     ////console.log(data);
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
   ////console.log(params);
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
        ////console.log(data);
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
