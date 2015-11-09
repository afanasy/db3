var
  _ = require('underscore'),
  stream = require('stream'),
  mysql = require('mysql'),
  shortid = require('shortid'),
  set = require('db3-set'),
  where = require('db3-where'),
  orderBy = require('db3-order-by')

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
  where: where,
  orderBy: orderBy,
  end: function (done) {
    return this.db.end(done)
  },
  createTable: function (table, field, done) {
    if (_.isFunction(table)) {
      field = table
      table = 'table' + shortid.generate()
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
      to = from + shortid.generate()
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
      to = from + shortid.generate()
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
    var query = 'insert ' + mysql.escapeId(table) + ' set ' + set.query(d)
    if (!done) {
      var self = this
      var s = new stream.Writable({objectMode: true})
      s._write = function (d, encoding, done) {self.insert(table, d, function () {done()})}
      return s
    }
    this.query(query, done)
  },
  update: function (table, cond, d, done)  {
    if (_.isString(cond) || _.isNumber(cond) || _.isArray(cond))
      cond = {id: cond}
    this.query(mysql.format('update ?? set ', table) + set.query(d) + ' where ' + where.query(cond), done)
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
    if (!done) {
      var self = this
      var s = new stream.Writable({objectMode: true})
      s._write = function (d, encoding, done) {self.save(table, d, field, function () {done()})}
      return s
    }
    this.query('insert ' + mysql.escapeId(table) + ' set ' + set.query(d) + ' on duplicate key update ' + set.query((field && _.pick(d, field)) || d), done)
  },
  duplicate: function (table, id, d, done) {
    if (_.isFunction(d)) {
      done = d
      d = undefined
    }
    var self = this
    var temporaryTable = 'duplicate' + shortid.generate()
    self.query('create table ?? like ??', [temporaryTable, table], function () {
      self.query('insert ?? select * from ?? where ?', [temporaryTable, table, {id: id}], function () {
        self.update(temporaryTable, id, d, function () {
          self.query('alter table ?? drop id', [temporaryTable], function () {
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
    if (_.isString(d)) {
      d = {
        table: d,
        where: cond,
        field: field
      }
    }
    var unpackRow = _.isNumber(d.where) || _.isString(d.where)
    if (_.isNumber(d.where) || _.isString(d.where) || _.isArray(d.where))
      d.where = {id: d.where}
    d.field = d.field || '*'
    var unpackField = (_.isNumber(d.field) || _.isString(d.field)) && (d.field != '*') && d.field
    if (_.isString(d.field))
      d.field = [d.field]
    d.field = _.map(d.field, function (d) {
      if (d != '*')
        return mysql.escapeId(d)
      return d
    }).join(', ')
    var query = 'select ' + d.field + ' from ' + mysql.escapeId(d.table)
    d.where = where.query(d.where)
    if (d.where)
      query += ' where ' + d.where
    d.orderBy = orderBy.query(d.orderBy)
    if (d.orderBy)
      query += ' order by ' + d.orderBy
    if (d.limit)
      query += ' limit ' + +d.limit
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
