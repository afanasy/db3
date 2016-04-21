var
  _ = require('underscore'),
  queryString = require('db3-query-string')

module.exports = function () {
  var app = function (query, value, done) {
    return app.query(query, value, done)
  }
  _.extend(app, {
    queryString: queryString,
    end: function (done) {
      this.query('dbEnd', done)
      return this
    },
    createTable: function (table, field, done) {
      if (_.isFunction(table)) {
        field = table
        table = 'table' + +new Date
      }
      if (_.isFunction(field)) {
        done = field
        field = undefined
      }
      return this.query({name: 'createTable', table: table, field: field}, done)
    },
    dropTable: function (table, done) {
      return this.query({name: 'dropTable', table: table}, done)
    },
    truncateTable: function (table, done) {
      return this.query({name: 'truncateTable', table: table}, done)
    },
    renameTable: function (from, to, done) {
      if (_.isFunction(to)) {
        done = to
        to = undefined
      }
      return this.query({name: 'renameTable', table: from, to: to}, done)
    },
    tableExists: function (table, done) {
      return this.query({name: 'select', table: table, limit: 1}, function (err, data) {
        if (!err)
          return done(null, true)
        done(null, false)
      })
    },
    copyTable: function (from, to, done) {
      if (_.isFunction(to)) {
        done = to
        to = undefined
      }
      var self = this
      return self.query({name: 'createTable', table: to, like: from}, function (err, data) {
        if (err)
          return done(err, null)
        self.query({name: 'insert', table: to, select: from}, done)
      })
    },
    insert: function (table, d, done) {
      if (_.isFunction(d)) {
        done = d
        d = undefined
      }
      return this.query({name: 'insert', table: table, set: d}, done)
    },
    update: function (table, cond, d, done)  {
      if (_.isString(cond) || _.isNumber(cond) || _.isArray(cond))
        cond = {id: cond}
      return this.query({name: 'update', table: table, set: d, where: cond}, done)
    },
    delete: function (table, cond, done) {
      if (_.isString(cond) || _.isNumber(cond) || _.isArray(cond))
        cond = {id: cond}
      return this.query({name: 'delete', table: table, where: cond}, done)
    },
    save: function (table, d, field, done) {
      if (_.isFunction(d)) {
        done = field
        field = d
        d = undefined
      }
      if (_.isFunction(field)) {
        done = field
        field = undefined
      }
      if (!_.size(d))
        d = {id: null}
      if (_.isString(field))
        field = [field]
      return this.query({name: 'insert', table: table, set: d, update: (field && _.pick(d, field)) || d}, done)
    },
    duplicate: function (table, where, update, done) {
      if (_.isFunction(update)) {
        done = update
        update = undefined
      }
      if (_.isString(where) || _.isNumber(where) || _.isArray(where))
        where = {id: where}
      var self = this
      return self.query({name: 'select', table: table, where: where}, function (err, data) {
        _.each(data, function (value) {
          delete value.id
          _.extend(value, update)
        })
        self.insert(table, data, done)
      })
    },
    select: function (d, cond, field, done) {
      if (_.isFunction(cond)) {
        done = cond
        field = undefined
        cond = undefined
      }
      if (_.isFunction(field)) {
        done = field
        field = undefined
      }
      if (_.isNumber(d) || _.isString(d)) {
        d = {
          table: d,
          where: cond,
          field: field
        }
      }
      var unpackRow = _.isNumber(d.where) || _.isString(d.where)
      if (_.isNumber(d.where) || _.isString(d.where) || _.isArray(d.where))
        d.where = {id: d.where}
      var unpackField = (_.isNumber(d.field) || _.isString(d.field)) && d.field
      function unpack (err, data) {
        if (err)
          return done(err)
        if (unpackField)
          data = _.pluck(data, unpackField)
        if (unpackRow)
          data = data[0]
        done(err, data)
      } 
      var query = _.extend({name: 'select'}, _.pick(d, ['field', 'table', 'where', 'orderBy', 'limit']))
      if (!done)
        return this.query(query)
      return this.query(query, unpack)
    },
    groupBy: function (func, table, cond, field, done) {
      if (_.isFunction(cond) || _.isArray(cond)) {
        done = field
        field = cond
        cond = undefined
      }
      if (_.isFunction(field)) {
        done = field
        field = undefined
      }
      field = field || 'id'
      if (!_.isArray(field))
        field = [field]
      var query = {name: 'groupBy', func: func, table: table, where: cond, field: field}
      this.query(query, function (err, data) {
        if (err)
          return done(err)
        if (field.length <= 1) {
          var value = data && data[0] && _.values(data[0])[0]
          if (value && !_.contains(['min', 'max'], func))
            value = +value
          return done(err, value)
        }
        done(err, data)
      })
    },
    query: function (query, value, done) {
      if (_.isFunction(value)) {
        done = value
        value = undefined
      }
      return this.pump({
        i: 0,
        db: this,
        query: query,
        value: value,
        done: done
      })
    },
    pump: function (ctx) {
      var next = function (err) {
        var fn = this.pipeline[ctx.i++]
        if (err || !fn)
          return ctx.done(err, ctx.data)
        return fn(ctx, next)
      }.bind(this)
      return next()
    },
    use: function (fn) {
      this.pipeline.push(fn)
      return this
    },
    reset: function () {
      this.pipeline = []
      this.use(this.stringify())
      return this
    },
    stringify: function () {
      return function (ctx, next) {
        ctx.queryString = queryString.stringify(ctx.query, ctx.value)
        return next()
      }
    }
  })
  _.each(['count', 'min', 'max', 'avg', 'sum'], function (func) {
    app[func] = function (table, cond, field, done) {
      return app.groupBy(func, table, cond, field, done)
    }
  })
  app.reset()
  return app
}

