var
  stream = require('stream'),
  mysql = require('mysql'),
  _ = require('underscore'),
  async = require('async')

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
  escape: function (value, set) {
    var self = this
    if (_.isNaN(value) || _.isNull(value) || _.isUndefined(value))
      return 'null'
    if (_.isNumber(value))
      return value
    if (_.isBoolean(value))
      return +value
    if (!set) {
      if (_.isArray(value))
        return '(' + _.map(value, function (d) {return self.escape(d, set)}).join(', ') + ')'
      if (_.isObject(value)) {
        if (!_.isUndefined(value.from) && !_.isUndefined(value.to))
          return this.escape(value.from, set) + ' and ' + this.escape(value.to, set)
        if (!_.isUndefined(value.from))
          return this.escape(value.from, set)
        if (!_.isUndefined(value.to))
          return this.escape(value.to, set)
      }
    }
    return mysql.escape(String(value))
  },
  cond: function (d, set) {
    var delimiter = ' and '
    if (set)
      delimiter = ', '
    if (_.isNumber(d) || _.isString(d))
      d = {id: +d}
    var self = this
    return _.map(d, function (value, key) {return self.pair(key, value, null, true, true, set)}).join(delimiter)
  },
  pair: function (key, value, operator, escapeKey, escapeValue, set) {
    operator = operator || '='
    if (!set) {
      if ((_.isNaN(value) || _.isNull(value) || _.isUndefined(value)) && (operator == '='))
        operator = 'is'
      if (_.isArray(value))
        operator = 'in'
      if (_.isObject(value)) {
        if (!_.isUndefined(value.from) && !_.isUndefined(value.to))
          operator = 'between'
        else {
          if (!_.isUndefined(value.from))
            operator = '>='
          if (!_.isUndefined(value.to))
            operator = '<='
        }
      }
    }
    if (escapeKey !== false)
      key = mysql.escapeId(String(key))
    if (escapeValue !== false)
      value = this.escape(value, set)
    return key + ' ' + operator + ' ' + value
  },
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
    this.q('createTable', 'create table ' + mysql.escapeId(table) + ' (' + field + ')', function (data, err) {
      data = data || {}
      data.table = table
      cb(data, err)
    })
  },
  dropTable: function (table, cb) {
    this.q('dropTable', mysql.format('drop table ??', table), cb)
  },
  tableExists: function (table, cb) {
    this.q('tableExists', mysql.format('select 1 from ?? limit 1', table), function (data, err) {
      if (!err)
        return cb(true)
      cb(false)
    })
  },
  truncateTable: function (table, cb) {
    this.q('truncateTable', mysql.format('truncate table ??', table), cb)
  },
  copyTable: function (from, to, cb) {
    if (_.isFunction(to)) {
      cb = to
      to = undefined
    }
    if (!to)
      to = from + +(new Date)
    var self = this
    self.q('copyTable', mysql.format('create table ?? like ??', [to, from]), function () {
      self.q('copyTable', mysql.format('insert ?? select * from ??', [to, from]), function (data, err) {
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
    this.q('renameTable', mysql.format('rename table ?? to ??', [from, to]), function (data, err) {
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
    var query = 'insert ' + mysql.escapeId(table) + ' set ' + this.cond(d, true)
    if (!cb) {
      var self = this
      var s = new stream.Writable({objectMode: true})
      s._write = function (d, encoding, done) {self.insert(table, d, function () {done()})}
      return s
    }
    this.q('insert', query, cb)
  },
  update: function (table, cond, d, cb)  {
    if (_.isString(cond) || _.isNumber(cond))
      cond = {id: cond}
    this.q('update', mysql.format('update ?? set ', table) + this.cond(d, true) + ' where ' + this.cond(cond), cb)
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
    this.q('delete', 'delete from ' + mysql.escapeId(table) + ' where ' + this.cond(cond), cb)
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
        pair = self.pair(key, value, null, true, true, true)
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
    this.q('save', 'insert ' + mysql.escapeId(table) + ' set ' + insert.join(', ') + ' on duplicate key update ' + update.join(', '), cb)
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
    cond = this.cond(cond)
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
    this.q('select', query, function (data, err, fields) {
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
    cond = this.cond(cond)
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
    this.q(func, query, function (data, err) {
      if (!field.length) {
        var value = data && data[0] && _.values(data[0])[0]
        if (func == 'count')
          value = value || 0
        return cb(value, err, field)
      }
      cb(data, err)
    })
  },
  q: function (name, query, cb) {
    if ((this.logQuery === true) || (this.logQuery === name) || _.contains(this.logQuery, name))
      console.log(query)
    this.query(query, cb)
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
