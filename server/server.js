'use strict';

var loopback = require('loopback');
var boot = require('loopback-boot');
var explorer = require('loopback-component-explorer');

var app = module.exports = loopback();

app.start = function() {
  // start the web server
  return app.listen(function() {
    app.emit('started');
    var baseUrl = app.get('url').replace(/\/$/, '');
    console.log('Web server listening at: %s', baseUrl);
    /* doesn't work, don't know why 
    if (app.get('loopback-component-explorer')) {
      var explorerPath = app.get('loopback-component-explorer').mountPath;
      console.log('Browse your REST API at %s%s', baseUrl, explorerPath);
    }
    */
    let options = {};
    app.use('/explorer', explorer.routes(app, options));
    //var explorerPath = app.get('loopback-component-explorer').mountPath;
    console.log('Browse your REST API at %s%s', baseUrl, '/explorer');

    app.use(function(req, res, next) {
      console.log('request', baseUrl + req.url);
      next();
    });
  });
};

// Bootstrap the application, configure models, datasources and middleware.
// Sub-apps like REST API are mounted via boot scripts.
boot(app, __dirname, function(err) {
  if (err) throw err;

  // start the server if `$ node server.js`
  if (require.main === module)
    app.start();
});
