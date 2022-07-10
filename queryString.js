var sqlstring = require('mysql/lib/protocol/SqlString')
var format = sqlstring.format
var escape = sqlstring.escape
var escapeId = sqlstring.escapeId
var is = require('./is')
var isArray = is.array
var isFunction = is.function
var isString = is.string
var isUndefined = is.undefined

var app = module.exports = {
  set: require('db3-set'),
  where: require('db3-where'),
  orderBy: require('db3-order-by'),
  stringify: (d, value) => {
    if (isString(d)) {
      if (!isUndefined(value))
        return format(d, value)
      return d
    }
    if (!isUndefined(d.table))
      d.table = String(d.table)
    if (isFunction(app.stringify[d.name]))
      return app.stringify[d.name](d)
    return String(d)
  }
}

Object.assign(app.stringify, {
  createTable: d => {
    if (d.like)
      return format('create table ?? like ??', [d.table, d.like])
    var field = d.field
    if (!field || !field.length)
      field = ['id', 'name']
    field = field.map(field => {
      var type = 'text'
      if (field == 'id')
        type = 'bigint primary key auto_increment'
      if (field.match(/Id$/))
        type = 'bigint'
      return escapeId(field) + ' ' + type
    }).join(', ')
    return 'create table ' + escapeId(d.table) + ' (' + field + ')'
  },
  dropTable: d => format('drop table ??', d.table),
  truncateTable: d => format('truncate table ??', d.table),
  renameTable: d => format('rename table ?? to ??', [d.table, d.to]),
  alterTable: d => format('alter table ?? drop ??', [d.table, d.drop]),
  insert: d => {
    var query = 'insert ' + escapeId(d.table) + ' '
    if (d.select)
      return query + app.stringify.select(d.select)
    var set = d.set
    if (isArray(set)) {
      if (set.length)
        query += '('  + Object.keys(set[0]).map(key => escapeId(key)).join(', ') + ') values ' +
          set.map(value => '(' + value.map(d => escape(d)).join(', ') + ')').join(', ')
    }
    else {
      set = set || {id: null}
      query += 'set ' + app.set.query(set)
    }
    if (d.update)
      query += ' on duplicate key update ' + app.set.query(d.update)
    return query
  },
  update: d => format('update ?? set ', d.table) + app.set.query(d.set) + ' where ' + app.where.query(d.where),
  delete: d => 'delete from ' + escapeId(d.table) + ' where ' + app.where.query(d.where),
  select: d => {
    if (isString(d))
      d = {table: d}
    var field = d.field
    if (isUndefined(field))
     field = '*'
    if (!isArray(field))
      field = [field]
    field = field.map(d => {
      if (d != '*')
        return escapeId(String(d))
      return d
    }).join(', ')
    var query = 'select ' + field + ' from ' + escapeId(d.table)
    var where = app.where.query(d.where)
    if (where)
      query += ' where ' + where
    var orderBy = app.orderBy.query(d.orderBy)
    if (orderBy)
      query += ' order by ' + orderBy
    if (d.limit)
      query += ' limit ' + +d.limit
    return query
  },
  groupBy: d => {
    var where = app.where.query(d.where)
    var field = field || 'id'
    if (!isArray(field))
      field = [field]
    else
      field = field.slice()
    var lastField = escapeId(field.pop())
    field = escapeId(field)
    var query = 'select ' + field
    if (field.length)
      query += ', '
    query += d.func + '(' + lastField + ') as ' + d.func + ' from ' + escapeId(d.table)
    if (where)
      query += ' where ' + where
    if (field.length)
      query += ' group by ' + field
    return query
  }
})
