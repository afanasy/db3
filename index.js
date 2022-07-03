var stream = require('stream')
var mysql = require('mysql')
var shortid = require('shortid')
var queryString = require('db3-query-string')

function tagTester (name) {
  var tag = '[object ' + name + ']'
  return function (obj) {
    return toString.call(obj) === tag
  }
}

var isArray = Array.isArray
var isFunction = tagTester('Function')
var isNumber = tagTester('Number')
var isString = tagTester('String')

exports.connect = function (d) {
  return new Db3(mysql.createPool(d))
}

var Db3 = function (d) {
  this.db = d
  ;['count', 'min', 'max', 'avg', 'sum'].forEach(func =>
    this[func] = (table, cond, field, done) => this.groupBy(func, table, cond, field, done)
  )
  return this
}

Object.assign(Db3.prototype, {
  format: mysql.format,
  queryString: queryString,
  end: function (done) {
    return this.db.end(done)
  },
  createTable: function (table, field, done) {
    if (isFunction(table)) {
      field = table
      table = 'table' + shortid.generate()
    }
    if (isFunction(field)) {
      done = field
      field = undefined
    }
    this.query({name: 'createTable', table: table, field: field}, (err, data) => {
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
    if (isFunction(to)) {
      done = to
      to = undefined
    }
    if (!to)
      to = from + shortid.generate()
    this.query({name: 'renameTable', table: from, to: to}, (err, data) => {
      if (err)
        return done(err, null)
      data = data || {}
      data.table = to
      done(err, data)
    })
  },
  tableExists: function (table, done) {
    this.query({name: 'select', table: table, limit: 1}, err => {
      if (!err)
        return done(null, true)
      done(null, false)
    })
  },
  copyTable: function (from, to, done) {
    if (isFunction(to)) {
      done = to
      to = undefined
    }
    if (!to)
      to = from + shortid.generate()
    var self = this
    self.query({name: 'createTable', table: to, like: from}, err => {
      if (err)
        return done(err, null)
      self.query({name: 'insert', table: to, select: from}, (err, data) => {
        if (err)
          return done(err, null)
        data = data || {}
        data.table = to
        done(err, data)
      })
    })
  },
  insert: function (table, d, done) {
    if (isFunction(d)) {
      done = d
      d = undefined
    }
    if (!d || !Object.keys(d).length)
      d = {id: null}
    if (!done) {
      var s = new stream.Writable({objectMode: true})
      s._write = (d, encoding, done) => this.insert(table, d, () => done())
      return s
    }
    this.query({name: 'insert', table: table, set: d}, done)
  },
  update: function (table, cond, d, done)  {
    if (isString(cond) || isNumber(cond) || isArray(cond))
      cond = {id: cond}
    this.query({name: 'update', table: table, set: d, where: cond}, done)
  },
  delete: function (table, cond, done) {
    if (isString(cond) || isNumber(cond))
      cond = {id: cond}
    if (!done) {
      var s = new stream.Writable({objectMode: true})
      s._write = (d, encoding, done) => this.delete(table, d, () => done())
      return s
    }
    this.query({name: 'delete', table: table, where: cond}, done)
  },
  save: function (table, d, field, done) {
    if (isFunction(d)) {
      done = field
      field = d
      d = undefined
    }
    if (isFunction(field)) {
      done = field
      field = undefined
    }
    if (!d || !Object.keys(d).length)
      d = {id: null}
    if (isString(field))
      field = [field]
    if (!done) {
      var s = new stream.Writable({objectMode: true})
      s._write = (d, encoding, done) => this.save(table, d, field, () => done())
      return s
    }
    var update = d
    if (field) {
      update = {}
      for (var i = 0; i < field.length; i++)
        update[field[i]] = d[field[i]]
    }
    this.query({name: 'insert', table: table, set: d, update: update}, done)
  },
  duplicate: function (table, id, d, done) {
    if (isFunction(d)) {
      done = d
      d = undefined
    }
    var temporaryTable = 'duplicate' + shortid.generate()
    this.query({name: 'createTable', table: temporaryTable, like: table}, () => {
      this.query({name: 'insert', table: temporaryTable, select: {table: table, where: {id: id}}}, () => {
        this.update(temporaryTable, id, d, () => {
          this.query({name: 'alterTable', table: temporaryTable, drop: 'id'}, () => {
            this.query('insert ?? select null, ??.* from ??', [table, temporaryTable, temporaryTable], (err, data) => {
              this.dropTable(temporaryTable, () =>
                done(err, data)
              )
            })
          })
        })
      })
    })
  },
  select: function (d, cond, field, done) {
    if (isFunction(cond)) {
      done = cond
      field = undefined
      cond = undefined
    }
    if (isFunction(field)) {
      done = field
      field = undefined
    }
    if (isString(d)) {
      d = {
        table: d,
        where: cond,
        field: field
      }
    }
    var unpackRow = isNumber(d.where) || isString(d.where)
    if (isNumber(d.where) || isString(d.where) || isArray(d.where))
      d.where = {id: d.where}
    var unpackField = (isNumber(d.field) || isString(d.field)) && d.field
    var query = queryString.stringify({
      name: 'select',
      field: d.field,
      table: d.table,
      where: d.where,
      orderBy: d.orderBy,
      limit: d.limit
    })
    function unpack (data) {
      if (data) {
        if (unpackField)
          data = data.map(d => d[unpackField])
        if (unpackRow)
          data = data[0]
      }
      return data
    }
    if (!done)
      return this.db.query(query).stream()
    this.query(query, (err, data, fields) =>{
      done(err, unpack(data), fields)
    })
  },
  groupBy: function (func, table, cond, field, done) {
    if ((isFunction(cond)) || isArray(cond)) {
      done = field
      field = cond
      cond = undefined
    }
    if (isFunction(field)) {
      done = field
      field = undefined
    }
    cond = queryString.where.query(cond)
    field = field || 'id'
    if (isString(field))
      field = [field]
    var lastField = mysql.escapeId(field.pop())
    field = mysql.escapeId(field)
    var query = 'select ' + field
    if (field.length)
      query += ', '
    query += func + '(' + lastField + ') as ' + func + ' from ' + mysql.escapeId(table)
    if (cond)
      query += ' where ' + cond
    if (field.length)
      query += ' group by ' + field
    this.query(query, (err, data) => {
      if (!field.length) {
        var value = data && data[0] && Object.values(data[0])[0]
        if (value && (func != 'min') && (func != 'max'))
          value = +value
        return done(err, value, field)
      }
      done(err, data)
    })
  },
  query: function (sql, values, done) {
    if (isFunction(values)) {
      done = values
      values = undefined
    }
    var query = queryString.stringify(sql, values)
    if (!done)
      return this.db.query(query).stream()
    return this.db.query(query, done)
  }
})
