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
    self[func] = function (table, cond, field, cb) {
      return self.groupBy(func, table, cond, field, cb)
    }
  })
  return this
}

_.extend(Db3.prototype, {
  format: mysql.format,
  end: function (cb) {
    return this.db.end(cb)
  },
  createTable: function (table, field, cb) {
    if (_.isFunction(table)) {
      field = table
      table = 'table' + +(new Date)
    }
    if (_.isFunction(field)) {
      cb = field
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
    this.query('create table ' + mysql.escapeId(table) + ' (' + field + ')', function (data, err) {
      data = data || {}
      data.table = table
      cb(data, err)
    })
  },
  dropTable: function (table, cb) {
    this.query(mysql.format('drop table ??', table), cb)
  },
  tableExists: function (table, cb) {
    this.query(mysql.format('select 1 from ?? limit 1', table), function (data, err) {
      if (!err)
        return cb(true)
      cb(false)
    })
  },
  truncateTable: function (table, cb) {
    this.query(mysql.format('truncate table ??', table), cb)
  },
  copyTable: function (from, to, cb) {
    if (_.isFunction(to)) {
      cb = to
      to = undefined
    }
    if (!to)
      to = from + +(new Date)
    var self = this
    self.query(mysql.format('create table ?? like ??', [to, from]), function () {
      self.query(mysql.format('insert ?? select * from ??', [to, from]), function (data, err) {
        data = data || {}
        data.table = to
        cb(data, err)
      })
    })
  },
  renameTable: function (from, to, cb) {
    if (_.isFunction(to)) {
      cb = to
      to = undefined
    }
    if (!to)
      to = from + +(new Date)
    this.query(mysql.format('rename table ?? to ??', [from, to]), function (data, err) {
      data = data || {}
      data.table = to
      cb(data, err)
    })
  },
  insert: function (table, d, cb) {
    if (_.isFunction(d)) {
      cb = d
      d = undefined
    }
    if (!d || !_.size(d))
      d = {id: null}
    var query = 'insert ' + mysql.escapeId(table) + ' set ' + where.query(d, true)
    if (!cb) {
      var self = this
      var s = new stream.Writable({objectMode: true})
      s._write = function (d, encoding, done) {self.insert(table, d, function () {done()})}
      return s
    }
    this.query(query, cb)
  },
  update: function (table, cond, d, cb)  {
    if (_.isString(cond) || _.isNumber(cond))
      cond = {id: cond}
    this.query(mysql.format('update ?? set ', table) + where.query(d, true) + ' where ' + where.query(cond), cb)
  },
  delete: function (table, cond, cb) {
    if (_.isString(cond) || _.isNumber(cond))
      cond = {id: cond}
    if (!cb) {
      var self = this
      var s = new stream.Writable({objectMode: true})
      s._write = function (d, encoding, done) {self.delete(table, d, function () {done()})}
      return s
    }
    this.query('delete from ' + mysql.escapeId(table) + ' where ' + where.query(cond), cb)
  },
  save: function (table, d, field, cb) {
    if (_.isFunction(d)) {
      cb = field
      field = d
      d = undefined
    }
    if (_.isFunction(field)) {
      cb = field
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
    if (!cb) {
      var self = this
      var s = new stream.Writable({objectMode: true})
      s._write = function (d, encoding, done) {self.save(table, d, field, function () {done()})}
      return s
    }
    this.query('insert ' + mysql.escapeId(table) + ' set ' + insert.join(', ') + ' on duplicate key update ' + update.join(', '), cb)
  },
  select: function (table, cond, field, cb) {
    if (_.isFunction(cond)) {
      cb = field
      field = cond
      cond = undefined
    }
    if (_.isFunction(field)) {
      cb = field
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
    if (!cb)
      return this.db.query(query).stream()
    this.query(query, function (data, err, fields) {
      cb(unpack(data), err, fields)
    })
  },
  groupBy: function (func, table, cond, field, cb) {
    if (_.isFunction(cond) || _.isArray(cond)) {
      cb = field
      field = cond
      cond = undefined
    }
    if (_.isFunction(field)) {
      cb = field
      field = undefined
    }
    cond = where.query(cond)
    var defaultField = 'id'
    if (func == 'count')
      defaultField = '*'
    field = field || defaultField
    if (_.isString(field))
      field = [field]
    var lastField = field.pop()
    if (func != 'count')
      lastField = mysql.escapeId(lastField)
    field = _.map(field, function (d, i) {return mysql.escapeId(field)}).join(', ')
    var query = 'select ' + field
    if (field.length)
      query += ', '
    query += func + '(' + lastField + ') as ' + func + ' from ' + mysql.escapeId(table)
    if (cond)
      query += ' where ' + cond
    if (field.length)
      query += ' group by ' + field
    this.query(query, function (data, err) {
      if (!field.length) {
        var value = data && data[0] && _.values(data[0])[0]
        if (func == 'count')
          value = value || 0
        if (value && !_.contains(['min', 'max'], func))
          value = +value
        return cb(value, err, field)
      }
      cb(data, err)
    })
  },
  query: function (sql, values, cb) {
    if (_.isFunction(values)) {
      cb = values
      values = undefined
    }
    if (!cb)
      return this.db.query(sql, values).stream()
    return this.db.query(sql, values, function (err, results, fields) {return cb(results, err, fields)})
  }
})
