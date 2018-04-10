var pull = require('pull-stream')
var Next = require('pull-next')
var nested = require('libnested')

module.exports = function (createStream, opts, property, range) {

  range = range || (opts.reverse ? 'lt' : 'gt')
  property = property || 'timestamp'
  opts = nested.clone(opts)
  var last = null, count = -1
  return Next(function () {
    if(last) {
      if(count === 0) return
      var value = nested.set(opts, range, nested.get(last, property))
      if(value == null) return
      last = null
    }

    return pull(
      createStream(nested.clone(opts)),
      pull.through(function (msg) {
        count ++
        if(!msg.sync) {
          last = msg
        }
      }, function (err) {
        //retry on errors...
        if(err) return count = -1
        //end stream if there were no results
        if(last == null) last = {}
      })
    )
  })
}






