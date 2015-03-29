var
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
    if (_.isNaN(value) || _.isNull(value) || _.isUndefined(value))
      return 'null'
    if (_.isNumber(value))
      return value
    if (_.isBoolean(value))
      return +value
    if (!set) {
      if (_.isArray(value))
        return '(' + _.map(value, function (d) {return this.escape(d)}).join(', ') + ')'
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
    if (_.isArray(d)) {
      var self = this
      return async.eachSeries(d, function (data, done) {self.insert(table, data, function () {done()})}, function () {cb()})
    }
    this.q('insert', mysql.format('insert ?? set ', table) + this.cond(d, true), cb)
  },
  update: function (table, cond, d, cb)  {
    if (_.isString(cond) || _.isNumber(cond))
      cond = {id: cond}
    this.q('update', mysql.format('update ?? set ', table) + this.cond(d, true) + ' where ' + this.cond(cond), cb)
  },
  delete: function (table, cond, cb) {
    if (_.isString(cond) || _.isNumber(cond))
      cond = {id: cond}
    this.q('delete', 'delete from ' + mysql.escapeId(table) + ' where ' + this.cond(cond), cb)
  },
  save: function (table, d, cb) {
    if (_.isFunction(d)) {
      cb = d
      d = undefined
    }
    if (!d || !_.size(d))
      d = {id: null}
    var self = this
    var values = _.map(d, function (value, key) {
      if ((key == 'id') && (value === false))
        return '`' + key + '` = last_insert_id(' + key + ')'
      else
        return self.pair(key, value, null, true, true, true)
    })
    values = values.join(', ')
    this.q('save', 'insert ' + mysql.escapeId(table) + ' set ' + values + ' on duplicate key update ' + values, cb)
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
    if (_.isNumber(cond) || _.isString(cond))
      cond = {id: cond}
    cond = this.cond(cond)
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
    this.q('select', query, cb)
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
    return this.db.query(sql, values, function (err, results, fields) {return cb(results, err, fields)})
  }
})
