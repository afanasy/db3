var
  _ = require('underscore'),
  async = require('async'),
  stream = require('stream'),
  mysql = require('mysql'),
  where = require('js-where')

exports.connect = function (d) {
  return new Db3(mysql.createPool(d))
}

var Db3 = function (d) {
  this.db = d
  var self = this
  _.each(['count', 'min', 'max', 'avg', 'sum'], function (func) {
    self[func] = function (table, cond, field, done) {
      return self.groupBy(func, table, cond, field, done)
    }
  })
  return this
}

_.extend(Db3.prototype, {
  format: mysql.format,
  end: function (done) {
    return this.db.end(done)
  },
  createTable: function (table, field, done) {
    if (_.isFunction(table)) {
      field = table
      table = 'table' + +(new Date)
    }
    if (_.isFunction(field)) {
      done = field
      field = undefined
    }
    if (!_.size(field))
      field = ['id', 'name']
    field = _.map(field, function (field) {
      var type = 'text'
      if (field == 'id')
        type = 'bigint primary key auto_increment'
      if (field.match(/Id$/))
        type = 'bigint'
      return mysql.escapeId(field) + ' ' + type
    }).join(', ')
    this.query('create table ' + mysql.escapeId(table) + ' (' + field + ')', function (err, data) {
      data = data || {}
      data.table = table
      done(err, data)
    })
  },
  dropTable: function (table, done) {
    this.query(mysql.format('drop table ??', table), done)
  },
  tableExists: function (table, done) {
    this.query(mysql.format('select 1 from ?? limit 1', table), function (err, data) {
      if (!err)
        return done(null, true)
      done(null, false)
    })
  },
  truncateTable: function (table, done) {
    this.query(mysql.format('truncate table ??', table), done)
  },
  copyTable: function (from, to, done) {
    if (_.isFunction(to)) {
      done = to
      to = undefined
    }
    if (!to)
      to = from + +(new Date)
    var self = this
    self.query(mysql.format('create table ?? like ??', [to, from]), function (err, data) {
      if (err)
        return done(err, null)
      self.query(mysql.format('insert ?? select * from ??', [to, from]), function (err, data) {
        if (err)
          return done(err, null)
        data = data || {}
        data.table = to
        done(err, data)
      })
    })
  },
  renameTable: function (from, to, done) {
    if (_.isFunction(to)) {
      done = to
      to = undefined
    }
    if (!to)
      to = from + +(new Date)
    this.query(mysql.format('rename table ?? to ??', [from, to]), function (err, data) {
      if (err)
        return done(err, null)
      data = data || {}
      data.table = to
      done(err, data)
    })
  },
  insert: function (table, d, done) {
    if (_.isFunction(d)) {
      done = d
      d = undefined
    }
    if (!d || !_.size(d))
      d = {id: null}
    var query = 'insert ' + mysql.escapeId(table) + ' set ' + where.query(d, true)
    if (!done) {
      var self = this
      var s = new stream.Writable({objectMode: true})
      s._write = function (d, encoding, done) {self.insert(table, d, function () {done()})}
      return s
    }
    this.query(query, done)
  },
  update: function (table, cond, d, done)  {
    if (_.isString(cond) || _.isNumber(cond))
      cond = {id: cond}
    this.query(mysql.format('update ?? set ', table) + where.query(d, true) + ' where ' + where.query(cond), done)
  },
  delete: function (table, cond, done) {
    if (_.isString(cond) || _.isNumber(cond))
      cond = {id: cond}
    if (!done) {
      var self = this
      var s = new stream.Writable({objectMode: true})
      s._write = function (d, encoding, done) {self.delete(table, d, function () {done()})}
      return s
    }
    this.query('delete from ' + mysql.escapeId(table) + ' where ' + where.query(cond), done)
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
    var self = this
    var insert = []
    var update = []
    _.each(d, function (value, key) {
      var pair
      if ((key == 'id') && (value === false))
        pair = '`' + key + '` = last_insert_id(' + key + ')'
      else
        pair = where.pair(key, value, null, true, true, true)
      insert.push(pair)
      if (!field || _.contains(field, key))
        update.push(pair)
    })
    if (!done) {
      var self = this
      var s = new stream.Writable({objectMode: true})
      s._write = function (d, encoding, done) {self.save(table, d, field, function () {done()})}
      return s
    }
    this.query('insert ' + mysql.escapeId(table) + ' set ' + insert.join(', ') + ' on duplicate key update ' + update.join(', '), done)
  },
  select: function (table, cond, field, done) {
    if (_.isFunction(cond)) {
      done = field
      field = cond
      cond = undefined
    }
    if (_.isFunction(field)) {
      done = field
      field = undefined
    }
    var unpackRow = _.isNumber(cond) || _.isString(cond)
    if (_.isNumber(cond) || _.isString(cond) || _.isArray(cond))
      cond = {id: cond}
    cond = where.query(cond)
    var unpackField = (_.isNumber(field) || _.isString(field)) && (field != '*') && field
    field = field || '*'
    if (_.isString(field))
      field = [field]
    field = _.map(field, function (d) {
      if (d != '*')
        return mysql.escapeId(field)
      return d
    }).join(', ')
    var query = 'select ' + field + ' from ' + mysql.escapeId(table)
    if (cond)
      query += ' where ' + cond
    var unpack = function (data) {
      if (unpackField)
        data = _.pluck(data, unpackField)
      if (unpackRow)
        data = data[0]
      return data
    }
    if (!done)
      return this.db.query(query).stream()
    this.query(query, function (err, data, fields) {
      done(err, unpack(data), fields)
    })
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
    cond = where.query(cond)
    field = field || 'id'
    if (_.isString(field))
      field = [field]
    var lastField = mysql.escapeId(field.pop())
    field = _.map(field, function (d, i) {return mysql.escapeId(field)}).join(', ')
    var query = 'select ' + field
    if (field.length)
      query += ', '
    query += func + '(' + lastField + ') as ' + func + ' from ' + mysql.escapeId(table)
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
    if (!done)
      return this.db.query(sql, values).stream()
    return this.db.query(sql, values, done)
  }
})
