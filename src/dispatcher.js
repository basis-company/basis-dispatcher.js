var request = require('request');

class Dispatcher
{
  constructor(config) {
    this.config = config;
    this.reset();
  }

  reset() {
    this.localHandlers = {};
    this.invalidJobs = {};
    this.jobService = {};
  }

  dispatch(job, params = {}, headers = {}) {

    var handler = this.getLocalHandler(job);
    if(handler) {
      return new Promise((resolve, reject) => {
        try {
          var result = handler.bind(this)(params, headers) || {};
          resolve(result);
        } catch(e) {
          reject(e);
        }
      });
    }

    var form = {
      rpc: JSON.stringify({job, params})
    };

    return this.getRemoteHandler({job})
      .then(url => {
        return new Promise(function(resolve, reject) {
          var formData = {url, headers, form};
          request.post(formData, (e, r, b) => {
            if(e) {
              reject(e);

            } else {
              try {
                resolve(JSON.parse(b));

              } catch(error) {
                reject(b);
              }
            }
          });
        });
      });
  }

  getLocalHandler(job) {

    if(this.localHandlers[job]) {
      return this.localHandlers[job];
    }

    this.localHandlers[job] = false;

    var split = job.split('.');
    var ns = split[0];
    var method = split[1];

    [this, this.config].forEach((api) => {
      if(api[ns] && api[ns][method]) {
        this.localHandlers[job] = api[ns][method];
      }
    });
    return this.localHandlers[job];
  }

  getRemoteHandler(params) {
    if(this.invalidJobs[params.job]) {
      return Promise.reject('no job ' + params.job);
    }
    if(this.jobService[params.job]) {
      return Promise.resolve('http://' + this.jobService[params.job] + '/api');
    }
    return new Promise((resolve, reject) => {
      if(params.job.indexOf('.') === -1) {
        this.invalidJobs[params.job] = true;
        return reject('no job ' + params.job);
      }

      var service = params.job.split('.')[0];
      this.jobService[params.job] = service;
      resolve(this.getRemoteHandler(params));
    });
  }

  api(req, res) {
    try {
      if(!req.body.rpc) {
        throw 'no rpc';
      }

      var request = JSON.parse(req.body.rpc);
      if(!request) {
        throw 'invalid rpc';
      }

      if(!request.job) {
        throw 'no rpc.job';
      }

      if(!request.params) {
        throw 'no rpc.params';
      }

      return this.dispatch(request.job, request.params, {
        'x-real-ip': req.headers['x-real-ip'],
        'x-session': req.headers['x-session']
      })
        .then((data) => {
          res.send(JSON.stringify({data, success: true}));
        })
        .catch((error) => {
          var message = error.message || error;
          res.send(JSON.stringify({message, success: false}));
        });

    } catch (error) {
      var message = error.message || error;
      res.send(JSON.stringify({message, success: false}));
    }
  }
}

module.exports = Dispatcher;
