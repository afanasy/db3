var
  _ = require('underscore'),
  stream = require('stream'),
  queryString = require('db3-query-string'),
  format = require('sqlstring').format,
  escapeId = require('sqlstring').escapeId


module.exports = function () {
  return new Db3()
}

function Db3 () {
  var self = this
  _.each(['count', 'min', 'max', 'avg', 'sum'], function (func) {
    self[func] = function (table, cond, field, done) {
      return self.groupBy(func, table, cond, field, done)
    }
  })
  this.reset()
  return this
}

_.extend(Db3.prototype, {
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
    this.query({name: 'createTable', table: table, field: field}, function (err, data) {
      data = data || {}
      data.table = table
      done(err, data)
    })
  },
  dropTable: function (table, done) {
    this.query({name: 'dropTable', table: table}, done)
  },
  truncateTable: function (table, done) {
    this.query({name: 'truncateTable', table: table}, done)
  },
  renameTable: function (from, to, done) {
    if (_.isFunction(to)) {
      done = to
      to = undefined
    }
    if (!to)
      to = from + +new Date
    this.query({name: 'renameTable', table: from, to: to}, function (err, data) {
      if (err)
        return done(err, null)
      data = data || {}
      data.table = to
      done(err, data)
    })
  },
  tableExists: function (table, done) {
    this.query({name: 'select', table: table, limit: 1}, function (err, data) {
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
    if (!to)
      to = from + +new Date
    var self = this
    self.query({name: 'createTable', table: to, like: from}, function (err, data) {
      if (err)
        return done(err, null)
      self.query({name: 'insert', table: to, select: from}, function (err, data) {
        if (err)
          return done(err, null)
        data = data || {}
        data.table = to
        done(err, data)
      })
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
    if (_.isString(cond) || _.isNumber(cond))
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
  duplicate: function (table, id, d, done) {
    if (_.isFunction(d)) {
      done = d
      d = undefined
    }
    var self = this
    var temporaryTable = 'duplicate' + +new Date
    self.query({name: 'createTable', table: temporaryTable, like: table}, function () {
      self.query({name: 'insert', table: temporaryTable, select: {table: table, where: {id: id}}}, function () {
        self.update(temporaryTable, id, d, function () {
          self.query({name: 'alterTable', table: temporaryTable, drop: 'id'}, function () {
            self.query('insert ?? select null, ??.* from ??', [table, temporaryTable, temporaryTable], function (err, data) {
              self.dropTable(temporaryTable, function () {done(err, data)})
            })
          })
        })
      })
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
    var query = _.extend(
      {
        name: 'select',
        unpack: function (data) {
          if (unpackField)
            data = _.pluck(data, unpackField)
          if (unpackRow)
            data = data[0]
          return data
        }
      },
      _.pick(d, ['field', 'table', 'where', 'orderBy', 'limit'])
    )
    return this.query(query, done)
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
    cond = queryString.where.query(cond)
    field = field || 'id'
    if (_.isString(field))
      field = [field]
    var lastField = escapeId(field.pop())
    field = _.map(field, function (d, i) {return escapeId(field)}).join(', ')
    var query = 'select ' + field
    if (field.length)
      query += ', '
    query += func + '(' + lastField + ') as ' + func + ' from ' + escapeId(table)
    if (cond)
      query += ' where ' + cond
    if (field.length)
      query += ' group by ' + field
    this.query(query, function (err, data) {
      if (!field.length) {
        var value = data && data[0] && _.values(data[0])[0]
        if (value && !_.contains(['min', 'max'], func))
          value = +value
        return done(err, value, field)
      }
      done(err, data)
    })
  },
  query: function (sql, values, done) {
    if (_.isFunction(values)) {
      done = values
      values = undefined
    }
    return this.pump({
      i: 0,
      db: this,
      sql: sql,
      values: values,
      done: done
    })
  },
  pump: function (ctx) {
    var self = this
    function next (err) {
      var fn = self.pipeline[ctx.i++]
      if (err || !fn)
        return ctx.done(err, ctx.data)
      return fn(ctx, next)
    }
    return next()
  },
  use: function (fn) {
    if (_.isString(fn) && _.isFunction(this[fn]))
      fn = this[fn]()
    if (_.isFunction(fn))
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
      ctx.queryString = queryString.stringify(ctx.sql, ctx.values)
      return next()
    }
  },
  unpack: function () {
    return function (ctx, next) {
      if (!ctx.sql || !ctx.sql.unpack)
        return next()
      ctx.data = ctx.sql.unpack(ctx.data)
      return next()
    }
  }
})
