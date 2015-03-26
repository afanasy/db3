var
  mysql = require('mysql'),
  _ = require('underscore')

exports.connect = function (d) {
  return new Db3(mysql.createPool(d))
}

var Db3 = function (d) {
  this.db = d
  return this
}

_.extend(Db3.prototype, {
  cond: function (d) {
    if (_.isNumber(d) || _.isString(d))
      d = {id: +d}
    return _.map(d, function (value, key) {return mysql.format('?', _.pick(d, key))}).join(' and ')
  },
  pair: function (key, value, operator, escapeKey, escapeValue) {
    operator = operator || ' = '
    if (escapeKey !== false)
      key = mysql.escapeId(key)
    if (escapeValue !== false)
      value = mysql.escape(value)
    return key + ' ' + operator + ' ' + value
  },
  end: function (cb) {
    return this.db.end(cb)
  },
  createTable: function (table, field, cb) {
    if (_.isFunction(field)) {
      cb = field
      field = undefined
    }
    if (!_.size(field))
      field = ['id']
    field = _.map(field, function (field) {
      var type = 'text'
      if (field == 'id')
        type = 'bigint primary key auto_increment'
      if (field.match(/Id$/))
        type = 'bigint'
      return mysql.escapeId(field) + ' ' + type
    }).join(', ')
    this.q('createTable', 'create table ' + mysql.escapeId(table) + ' (' + field + ')', cb)
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
  moveTable: function (from, to, cb) {
    if (_.isFunction(to)) {
      cb = to
      to = undefined
    }
    if (!to)
      to = from + +(new Date)
    this.q('moveTable', mysql.format('rename table ?? to ??', [from, to]), function (data, err) {
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
    this.q('insert', mysql.format('insert ?? set ?', [table, d]), cb)
  },
  update: function (table, cond, d, cb)  {
    if (_.isString(cond) || _.isNumber(cond))
      cond = {id: cond}
    this.q('update', mysql.format('update ?? set ?', [table, d]) + ' where ' + this.cond(cond), cb)
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
    var pair = this.pair
    var values = _.map(d, function (value, key) {
      if ((key == 'id') && (value === false))
        return '`' + key + '` = last_insert_id(' + key + ')'
      else
        return pair(key, value)
    })
    values = values.join(', ')
    this.q('save', 'insert ' + mysql.escapeId(table) + ' set ' + values + ' on duplicate key update ' + values, cb)
  },
  select: function (table, cond, field, cb) {
    if (_.isFunction(cond)) {
      field = cond
      cond = undefined
    }
    if (_.isFunction(field)) {
      cb = field
      field = undefined
    }
    cond = this.cond(cond)
    if (_.isString(field))
      field = [field]
    if (_.isArray(field))
      field = _.map(field, function (d) {
        if (d != '*')
          return mysql.escapeId(field)
        return d
      }).join(', ')
    var query = 'select ' + (field || '*') + ' from ' + mysql.escapeId(table)
    if (cond)
      query += ' where ' + cond
    this.q('select', query, cb)
  },
  count: function (table, cond, cb) {
    if (_.isFunction(cond)) {
      cb = cond
      cond = undefined
    }
    cond = this.cond(cond)
    var query = 'select count(*) as count from ' + mysql.escapeId(table)
    if (cond)
      query += ' where ' + cond
    this.q('count', query, function (d) {
      cb && cb((d && d[0] && d[0].count) || 0)
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
