var
  mysql = require('mysql'),
  _ = require('underscore')

var db3 = function (d) {
  this.db = mysql.createPool(d)
  return this
}

_.extend(db3.prototype, {
  end: function () {
    return this.db.end()
  },
  cond: function (d) {
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
  query: function (name, query, cb) {
    if ((this.logQuery === true) || (this.logQuery === name) || _.contains(this.logQuery, name))
      console.log(query)
    this.db.query(query, function (err, data) {return cb && cb(data, err)})
  },
  insert: function (table, d, cb) {
    if (_.isFunction(d)) {
      cb = d
      d = undefined
    }
    if (!d || !_.size(d))
      d = {id: null}
    this.query('insert', mysql.format('insert ?? set ?', [table, d]), cb)
  },
  update: function (table, cond, d, cb)  {
    if (_.isString(cond) || _.isNumber(cond))
      cond = {id: cond}
    this.query('update', mysql.format('update ?? set ?', [table, d]) + ' where ' + this.cond(cond), cb)
  },
  delete: function (table, cond, cb) {
    if (_.isString(cond) || _.isNumber(cond))
      cond = {id: cond}
    this.query('delete', 'delete from ' + mysql.escapeId(table) + ' where ' + this.cond(cond), cb)
  },
  save: function (table, d, cb) {
    var values = _.map(d, function (value, key) {
      if ((key == 'id') && (value === false))
        return '`' + key + '` = last_insert_id(' + key + ')'
      else
        return this.pair(key, value)
    })
    values = values.join(', ')
    this.query('save', 'insert ' + mysql.escapeId(table) + ' set ' + values + ' on duplicate key update ' + values, cb)
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
    if (_.isString(field))
      field = [field]
    if (_.isArray(field))
      field = _.map(field, function (d) {
        if (d != '*')
          return mysql.escapeId(field)
        return d
      }).join(', ')
    cond = this.cond(cond)
    var query = 'select ' + (field || '*') + ' from ' + mysql.escapeId(table)
    if (cond)
      query += ' where ' + cond
    this.query('select', query, cb)
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
    this.query('count', query, function (d) {
      cb && cb((d && d[0] && d[0].count) || 0)
    })
  }
})

module.exports = db3
