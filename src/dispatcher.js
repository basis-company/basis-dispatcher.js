var etcdjs = require('etcdjs')
var request = require('request');

class Internal
{
  bootstrap(config) {

    this.store.mkdir('/jobs');

    Object.keys(config)
      .filter(ns => typeof(config[ns]) === 'object')
      .forEach(ns => {
        Object.keys(config[ns])
        .filter(action => typeof(config[ns][action]) == 'function')
        .forEach(action => {
          var job = ns + '.' + action;
          this.store.get('/jobs/' + job, (e, r) => {
            if(e || !r || !r.node) {
              this.store.set('/jobs/' + job + '/service', config.service);
            }
          });
        })
      });
  }
  reset() {
    this.localHandlers = {};
    this.invalidJobs = {};
    this.jobService = {};
    return {success: true};
  }
}

class Dispatcher
{
  constructor(config) {

    this.config = config;
    this.store  = new etcdjs(config.host);
    this.internal  = new Internal()

    this.internal.reset.bind(this)()

    if(config.service) {
      this.dispatch('internal.bootstrap', config);
    }
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
          request.post({url, headers, form}, (e, r, b) => {
            if(e) {
              reject(e);

            } else {
              try {
                resolve(JSON.parse(b));

              } catch(e) {
                reject(b)
              }
            }
          });
        });
      })
  }

  getLocalHandler(job) {

    if(this.localHandlers.hasOwnProperty(job)) {
      return this.localHandlers[job];
    }

    this.localHandlers[job] = false;

    var split = job.split('.');
    var ns = split[0];
    var method = split[1];

    [this, this.config].forEach(api => {
      if(api[ns] && api[ns][method]) {
        this.localHandlers[job] = api[ns][method];
      }
    })
    return this.localHandlers[job];
  }

  getRemoteHandler(params) {
    if(this.invalidJobs.hasOwnProperty(params.job)) {
      return Promise.reject('no job ' + params.job);
    }
    if(this.jobService.hasOwnProperty(params.job)) {
      return Promise.resolve('http://' + this.jobService[params.job] + '/api');
    }
    return new Promise((resolve, reject) => {
      this.store.get('jobs/' + params.job + '/service', (error, result) => {
        if(error || !result) {
          this.invalidJobs[params.job] = true;
          return reject('no job ' + params.job);
        } else {
          var service = result.node.value;
          this.jobService[params.job] = service;
          resolve(this.getRemoteHandler(params));
        }
      });
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
        throw 'no rpc.job'
      }

      if(!request.params) {
        throw 'no rpc.params'
      }

      return this.dispatch(request.job, request.params, {
        'x-real-ip': req.headers['x-real-ip'],
        'x-session': req.headers['x-session']
      })
        .then(data => {
          res.send(JSON.stringify({data, success: true}));
        })
        .catch(error => {
          var message = error.message || error;
          res.send(JSON.stringify({message, success: false}))
        });

    } catch (error) {
      var message = error.message || error;
      res.send(JSON.stringify({message, success: false}))
    }
  }
}

module.exports = Dispatcher
