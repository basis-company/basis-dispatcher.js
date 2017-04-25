var etcdjs = require('etcdjs')
var request = require('request');

class Internal
{
  bootstrap(config) {
    Object.keys(config).forEach(ns => {
      if(typeof(config[ns]) === 'object') {
        Object.keys(config[ns]).forEach(action => {
          if(typeof(config[ns][action]) == 'function') {
            var job = ns + '.' + action;
            this.store.mkdir('/jobs');
            this.store.get('/jobs/' + job, (e, r) => {
              if(e || !r || !r.node) {
                this.store.set('/jobs/' + job, config.name);
              }
            });
          }
        })
      }
    })
  }
  reset() {
    this.remoteServices = {};
    this.remoteHandlers = {};
    this.localHandlers = {};
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

    if(config.name) {
      this.dispatch('internal.bootstrap', config);

      this.store.mkdir('/services');
      this.store.get('/services/' + config.name, (e, r) => {
        if(e || !r || !r.node) {
          this.store.set('/services/' + config.name)
        }
      });
    }
  }

  dispatch(job, params = {}, headers = {}) {

    var handler = this.getLocalHandler(job);
    if(handler) {
      return Promise.resolve(handler.bind(this)(params));
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
    if(this.remoteHandlers.hasOwnProperty(params.job)) {
      var hostname = process.env[this.remoteHandlers[params.job]] || this.remoteServices[params.job];
      return Promise.resolve('http://' + hostname + '/api');
    }
    return new Promise((resolve, reject) => {
      this.store.get('/jobs/' + params.job, (error, result) => {
        if(error || !result) {
          return reject('no job ' + params.job);
        } else {
          var service = result.node.value;
          this.remoteServices[params.job] = service;
          this.remoteHandlers[params.job] = service.toUpperCase() + "_SERVICE_HOST";
          return resolve(this.getRemoteHandler(params));
        }
      });
    });
  }
}

module.exports = Dispatcher
