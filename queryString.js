var sqlstring = require('mysql/lib/protocol/SqlString')
var format = sqlstring.format
var escape = sqlstring.escape
var escapeId = sqlstring.escapeId
var is = require('./is')
var isArray = is.array
var isBoolean = is.boolean
var isFunction = is.function
var isNaN = is.nan
var isNull = is.null
var isNumber = is.number
var isObject = is.object
var isString = is.string
var isUndefined = is.undefined

var app = module.exports = {
  set: {
    //https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Assignment_Operators
    operator: ['+', '-', '*', '/', '%', '<<', '>>', '>>>', '&', '^', '|'],
    set: (operator, y) => {
      if (operator == '+')
        return x => x + y
      if (operator == '-')
        return x => x - y
      if (operator == '*')
        return x => x * y
      if (operator == '/')
        return x => x / y
      if (operator == '%')
        return x => x % y
      if (operator == '<<')
        return x => x << y
      if (operator == '>>')
        return x => x >> y
      if (operator == '>>>')
        return x => x >>> y
      if (operator == '&')
        return x => x & y
      if (operator == '^')
        return x => x ^ y
      if (operator == '|')
        return x => x | y
      return d => d
    },
    query: d => {
      if (isNumber(d) || isString(d))
        d = {id: +d}
      return Object.keys(d || {}).map(key => {
        var value = d[key]
        if (isObject(value)) {
          var valueKey = Object.keys(value)
          if (valueKey.length) {
            var func = valueKey[0]
            var args = value[func]
            if (func == 'now')
              return format('?? = now()', key)
            if (app.set.operator.indexOf(func) >= 0)
              return format('?? = ? ' + func + ' ?', [key, args[0], args[1]])
            if ((func[1] == '=') && (app.set.operator.indexOf(func[0]) >= 0))
              return format('?? = ?? ' + func + ' ?', [key, key, args])
          }
        }
        if ((key == 'id') && (value === false))
          return format('?? = last_insert_id(??)', [key, key])
        var pair = {}
        pair[key] = value
        return format('?', pair)
      }).join(', ')
    },
    transform: d => {
      if (isNumber(d) || isString(d))
        d = {id: +d}
      return a => {
        Object.keys(d || {}).map(key => {
          var value = d[key]
          if (isObject(value)) {
            var valueKey = Object.keys(value)
            if (valueKey.length) {
              var func = valueKey[0]
              var args = value[func]
              if (func == 'now')
                return a[key] = (new Date(Date.now() - (new Date).getTimezoneOffset() * 60000)).toISOString().substring(0, 19).replace('T', ' ')
              if (app.set.operator.indexOf(func) >= 0)
                return a[key] = app.set.set(func, args[1])(args[0])
              if ((func[1] == '=') && (app.set.operator.indexOf(func[0]) >= 0))
                return a[key] = app.set.set(func, args)(a[key])
            }
          }
          a[key] = value
        })
        return a
      };
    }
  },
  where: {
    query: d => {
      if (isNumber(d) || isString(d))
        d = {id: +d}
      return app.where.and(d).map(d => {
        if (isArray(d.value) && !d.value.length) {
          if (d.operator == '=')
            return 0
          if (d.operator == '!=')
            return 1
        }
        return format('?? ', d.key) + app.where.queryOperator(d.operator, d.value) + ' ' + app.where.queryValue(d.value)
      }).join(' and ')
    },
    queryOperator: (operator, value) => {
      if (operator == '=') {
        if (isNull(value))
          return 'is'
        if (isArray(value))
          return 'in'
      }
      if (operator == '!=') {
        if (isNull(value))
          return 'is not'
        if (isArray(value))
          return 'not in'
      }
      return operator
    },
    queryValue: value => {
      if (isNaN(value) || isNull(value) || isUndefined(value))
        return 'null'
      if (isNumber(value))
        return value
      if (isBoolean(value))
        return +value
      if (isArray(value))
        return format('(?)', [value])
      return format('?', [value])
    },
    and: d => {
      var and = []
      Object.keys(d || {}).forEach(key => {
        var value = d[key]
        if (isNaN(value) || isNull(value) || isUndefined(value) || isNumber(value) || isBoolean(value) || isString(value) || isArray(value))
          return and.push({key: key, value: value, operator: '='})
        if (isObject(value)) {
          if (!isUndefined(value.from))
            and.push({key: key, value: value.from, operator: '>='})
          if (!isUndefined(value.to))
            and.push({key: key, value: value.to, operator: '<='})
          ;['=', '!=', '>', '<', '>=', '<='].forEach(operator => {
            if (!isUndefined(value[operator]))
              and.push({key: key, value: value[operator], operator: operator})
          })
        }
      })
      return and
    },
    filter: d => {
      return a => !app.where.and(d).find(d => {
        if (d.operator == '=') {
          if (!isArray(d.value))
            return a[d.key] !== d.value
          else
            return d.value.indexOf(a[d.key]) < 0
        }
        if (d.operator == '!=') {
          if (!isArray(d.value))
            return a[d.key] === d.value
          else
            return d.value.indexOf(a[d.key]) >= 0
        }
        if (d.operator == '>')
          return a[d.key] <= d.value
        if (d.operator == '<')
          return a[d.key] >= d.value
        if (d.operator == '>=')
          return a[d.key] < d.value
        if (d.operator == '<=')
          return a[d.key] > d.value
      })
    }
  },
  orderBy: {
    query: d => {
      if (isString(d))
        return escapeId(d)
      if (isArray(d))
        return d.map(app.orderBy.query).join(', ')
      if (isObject(d))
        return Object.keys(d).map(key => escapeId(key) + ' ' + d[key]).join(', ')
    },
    sort: d => {
      if (!isArray(d))
        d = [d]
      return (a, b) => {
        var r = 0
        d.find(rule => {
          var key
          var order = 'asc'
          if (isString(rule))
            key = rule
          else {
            key = Object.keys(rule)[0]
            order = rule[key]
          }
          if (a[key] > b[key])
            r = (order == 'asc')? 1: -1
          if (a[key] < b[key])
            r = (order == 'asc')? -1: 1
          return !!r
        })
        return r
      }
    }
  },
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
