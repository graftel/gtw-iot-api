'use strict';
aaa
var SwaggerExpress = require('swagger-express-mw');
var app = require('express')();
var userManage = require('./api/controllers/userManage.js')
module.exports = app; // for testing

var config = {
  appRoot: __dirname, // required config
  swaggerSecurityHandlers: {
    UserSecurity: function(req, authOrSecDef, scopesOrApiKey, callback) {
        var apikey = req.headers['x-api-key'];
        if (typeof(apikey) != "undefined")
        {
          userManage.authenticate(apikey,function(ret1, data){
            if (ret1)
            {
              return callback(null);
            }
            else {
              return callback(new Error('access denied'));
            }
          });

        }
        else {
          return callback(new Error('access denied'));
        }


    }
}
};

SwaggerExpress.create(config, function(err, swaggerExpress) {
  if (err) { throw err; }

  // install middleware
  swaggerExpress.register(app);

  var port = process.env.PORT || 10010;
  app.listen(port);

  if (swaggerExpress.runner.swagger.paths['/hello']) {
    console.log('try this:\ncurl http://127.0.0.1:' + port + '/hello?name=Scott');
  }
});
