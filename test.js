var
  _ = require('underscore'),
  async = require('async'),
  csv = require('fast-csv'),
  db3Mysql = require('db3-mysql'),
  db3 = require('./')

var db = db3().
  use(db3Mysql({user: 'root', database : 'test'})).
  use('unpack')

var person = [
  {name: 'God', gender: 'god'},
  {name: 'Adam', gender: 'male'},
  {name: 'Eve', gender: 'female'},
  {name: 'Cain', gender: 'male'},
  {name: 'Able', gender: 'male'},
  {name: 'Seth', gender: 'male'}
]

describe('Db3', function () {
  before(function (done) {
    async.series([
      function (done) {db.dropTable('test', function () {done()})},
      db.createTable.bind(db, 'test'),
      function (done) {db.dropTable('person', function () {done()})},
      db.createTable.bind(db, 'person', ['id', 'name', 'gender']),
      async.eachSeries.bind(async, person, db.insert.bind(db, 'person'))
    ], done)
  })
  describe('#connect()', function () {
    it('connects to the db', function (done) {
      db3().
        use(db3Mysql({user: 'root', database : 'test'})).
        query('select 1', function (err, data) {
          done(data.length !== 1)
        })
    })
  })
  describe('#end()', function () {
    it('disconnects from db', function (done) {
      db3().use(db3Mysql({user: 'root', database : 'test'})).end(done)
    })
  })
  describe('#createTable()', function () {
    it('creates table', function (done) {
      var table = 'createTable' + +(new Date)
      db.createTable(table, function () {
        db.tableExists(table, function (err, exists) {
          done(exists !== true)
        })
      })
    })
  })
  describe('#dropTable()', function () {
    it('drops table', function (done) {
      var table = 'dropTable' + +(new Date)
      db.createTable(table, function () {
        db.dropTable(table, function () {
          db.tableExists(table, function (err, exists) {
            done(exists == true)
          })
        })
      })
    })
  })
  describe('#tableExists()', function () {
    it('checks if table exists', function (done) {
      db.tableExists('tableExists' + +(new Date), function (err, exists) {
        done(exists == true)
      })
    })
  })
  describe('#truncateTable()', function () {
    it('truncates table', function (done) {
      var table = 'truncateTable' + +(new Date)
      db.createTable(table, function () {
        db.insert(table, function () {
          db.truncateTable(table, function () {
            db.count(table, function (err, count) {
              done(count != 0)
            })
          })
        })
      })
    })
  })
  describe('#copyTable()', function () {
    it('copies table and all its data to other table', function (done) {
      var table = 'copyTable' + +(new Date)
      db.createTable(table, function () {
        db.insert(table, function () {
          db.copyTable(table, function (err, data) {
            db.count(data.table, function (err, count) {
              done(count <= 0)
            })
          })
        })
      })
    })
  })
  describe('#renameTable()', function () {
    it('renames table', function (done) {
      var table = 'renameTable' + +(new Date)
      db.createTable(table, function () {
        db.insert(table, function () {
          db.renameTable(table, function (err, data) {
            db.count(data.table, function (err, count) {
              done(count <= 0)
            })
          })
        })
      })
    })
  })
  describe('#insert()', function () {
    it('inserts empty item', function (done) {
      db.insert('test', function (err, data) {
        done(data.insertId <= 0)
      })
    })
    it('inserts item', function (done) {
      db.insert('test', {name: 'test'}, function (err, data) {
        db.select('test', data.insertId, function (err, data) {
          done(data.name != 'test')
        })
      })
    })
    it('creates writeable insert stream', function (done) {
      var table = 'person' + +(new Date)
      db.createTable(table, ['id', 'name', 'gender'], function () {
        var stream = db.insert(table)
        stream.on('finish', function () {
          db.count(table, function (err, count) {
            done(count <= 0)
          })
        })
        stream.write({})
        stream.end()
      })
    })
  })
  describe('#update()', function () {
    it('updates row', function (done) {
      db.insert('test', function (err, data) {
        var id = data.insertId
        db.update('test', id, {name: 'test'}, function (err, data) {
          db.select('test', id, function (err, data) {
            done(data.name != 'test')
          })
        })
      })
    })
    it('updates rows with array condition', function (done) {
      db.insert('test', function (err, data) {
        var id = [data.insertId]
        db.insert('test', function (err, data) {
          id.push(data.insertId)
          db.update('test', id, {name: 'test'}, function (err, data) {
            db.select('test', id, function (err, data) {
              done((data.length != id.length) || (data[0].name != 'test') || (data[1].name != 'test'))
            })
          })
        })
      })
    })
    it('creates writeable update stream', function (done) {
      var table = 'person' + +(new Date)
      db.copyTable('person', table, function () {
        var update = {name: 'test'}
        var stream = db.update(table, 1)
        stream.on('finish', function () {
          db.count(table, update, function (err, count) {
            done(count <= 0)
          })
        })
        stream.write(update)
        stream.end()
      })
    })
  })
  describe('#delete()', function () {
    it('deletes row', function (done) {
      db.insert('test', function (err, data) {
        var id = data.insertId
        db.delete('test', id, function (err, data) {
          db.select('test', id, function (err, data) {
            done(!_.isUndefined(data))
          })
        })
      })
    })
    it('creates writeable delete stream', function (done) {
      var table = 'person' + +(new Date)
      db.copyTable('person', table, function () {
        db.select(table).pipe(db.delete(table)).on('finish', function () {
          db.count(table, function (err, count) {
            done(count != 0)
          })
        })
      })
    })
  })
  describe('#save()', function () {
    it('inserts a new row', function (done) {
      db.save('test', function (err, data) {
        db.select('test', data.insertId, function (err, data) {
          done(!data.id)
        })
      })
    })
    it('updates an existing row', function (done) {
      db.save('test', function (err, data) {
        var item = {id: data.insertId, name: 'test'}
        db.save('test', item, function (err, data) {
          db.select('test', item.id, function (err, data) {
            done(data.name != item.name)
          })
        })
      })
    })
    it('updates an existing row using specified fields', function (done) {
      var item = {name: 'test'}
      db.save('test', item, function (err, data) {
        item.id = data.insertId
        item.name = 'tset'
        db.save('test', item, 'id', function (err, data) {
          db.select('test', item.id, function (err, data) {
            done(!data.name || (data.name == item.name))
          })
        })
      })
    })
    it('creates writeable save stream', function (done) {
      var table = 'person' + +(new Date)
      db.createTable(table, ['id', 'name', 'gender'], function () {
        db.select('person').pipe(db.save(table)).on('finish', function () {
          db.count(table, function (err, count) {
            done(count != person.length)
          })
        })
      })
    })
  })
  describe('#duplicate()', function () {
    it('duplicates row', function (done) {
      db.insert('test', function (err, data) {
        var id = data.insertId
        db.duplicate('test', id, function (err, data) {
          if (data.insertId == id)
            return done(true)
          db.count('test', data.insertId, function (err, data) {
            done(data != 1)
          })
        })
      })
    })
  })
  describe('#select()', function () {
    it('selects an item from table', function (done) {
      db.select('person', {name: 'God'}, function (err, data) {
        done(data[0].name != 'God')
      })
    })
    it('selects an item from table using "in ()"', function (done) {
      db.select('person', {id: [1, 2]}, function (err, data) {
        done(data.length != 2)
      })
    })
    it('selects an item from table using "between"', function (done) {
      db.select('person', {id: {from: 2, to: 4}}, function (err, data) {
        done(data.length != 3)
      })
    })
    it('selects an item from table using ">="', function (done) {
      db.select('person', {id: {from: 2}}, function (err, data) {
        done(data.length != 5)
      })
    })
    it('selects an item from table using "<="', function (done) {
      db.select('person', {id: {to: 2}}, function (err, data) {
        done(data.length != 2)
      })
    })
    it('selects an item from table using shorthand id syntax', function (done) {
      db.select('person', 3, function (err, data) {
        done(data.name != 'Eve')
      })
    })
    it('selects an item field from table using shorthand id/name syntax', function (done) {
      db.select('person', 3, 'name', function (err, data) {
        done(data != 'Eve')
      })
    })
    it('creates readable select stream', function (done) {
      var count = 0
      db.select('person').
        on('data', function (data) {data && data.id && count++}).
        on('end', function () {
          done(count <= 0)
        })
    })
    it('selects from object with orderBy', function (done) {
      db.select({table: 'person', field: 'name', orderBy: {name: 'desc'}}, function (err, data) {
        done(data[0] != 'Seth')
      })
    })
    it('selects from object with limit', function (done) {
      db.select({table: 'person', limit: 10}, function (err, data) {
        done(data.length != person.length)
      })
    })
    it('selects with empty condition', function (done) {
      db.select('person', {}, function (err, data) {
        done(data.length != person.length)
      })
    })
    it('returns err on invalid query', function (done) {
      db.select(+new Date, function (err) {
        done(!err)
      })
    })
  })
  describe('#groupBy()', function () {
    var groupBy = {
      count: {
        field: '*',
        fullTable: person.length,
        filtered: 1,
        grouped: {gender: 'female', count: 1},
        filteredAndGrouped: {gender: 'male', count: 4}
      },
      min: {
        fullTable: 1,
        filtered: 2,
        grouped: {gender: 'female', min: 3},
        filteredAndGrouped: {gender: 'male', min: 4}
      },
      max: {
        fullTable: person.length,
        filtered: 2,
        grouped: {gender: 'female', max: 3},
        filteredGrouped: {gender: 'male', max: 4}
      },
      avg: {
        fullTable: 3.5,
        filtered: 2,
        grouped: {gender: 'female', avg: 3},
        filteredGrouped: {gender: 'male', avg: 4}
      },
      sum: {
        fullTable: 21,
        filtered: 2,
        grouped: {gender: 'female', sum: 3.0},
        filteredGrouped: {gender: 'male', sum: 4}
      }
    }
    _.each(['count', 'min', 'max', 'avg', 'sum'], function (func) {
      var name = func + '(' + (groupBy[func].field || 'id') + ')'
      it('selects ' + name  + ' from table', function (done) {
        db[func]('person', function (err, data) {
          done(data != groupBy[func].fullTable)
        })
      })
      it('selects ' + name + ' from table filtered by condition', function (done) {
        db[func]('person', {name: 'Adam'}, function (err, data) {
          done(data != groupBy[func].filtered)
        })
      })
      it('selects ' + name + ' from table grouped by field', function (done) {
        db[func]('person', ['gender', 'id'], function (err, data) {
          done(!_.findWhere(data, groupBy[func].grouped))
        })
      })
      it('selects ' + name + ' from table filtered by condition and grouped by field', function (done) {
        db[func]('person', {name: 'Cain'}, ['gender', 'id'], function (err, data) {
          done(!_.findWhere(data, groupBy[func].filteredGrouped))
        })
      })
    })
  })
  describe('#query()', function () {
    it('returns summary data grouped by name', function (done) {
      db.query('select ??, count(*) from ?? group by ??', ['name', 'test', 'name'], function (err, data) {
        done(data.length <= 0)
      })
    })
    it('returns readable stream if no callback provided', function (done) {
      var count = 0
      db.query('select * from ??', 'person').
        on('data', function (err, data) {count++}).
        on('end', function () {
          done(count != person.length)
        })
    })
  })
  describe('#csv()', function () {
    it('streams select to csv', function (done) {
      var stream = csv.format({headers: true})
      var count = 0
      stream.on('data', function (err, data) {count++})
      stream.on('finish', function () {
        done(count <= 0)
      })
      db.select('person').pipe(stream)
    })
    it('streams csv to insert', function (done) {
      var table = 'csv' + +(new Date)
      db.createTable(table, function () {
        var stream = db.insert(table)
        stream.on('finish', function () {
          db.count(table, function (err, count) {
            done(count <= 0)
          })
        })
        csv.fromString("name\ncsv1\ncsv2\n", {headers: true}).pipe(stream)
      })
    })
  })
  describe('#use', function () {
    beforeEach(function (done) {
      db.end(function () {
        db.reset().
          use(db3Mysql({user: 'root', database : 'test'})).
          use('unpack')
        done()
      })
    })
    it('uses plugin', function (done) {
      db.use(function (ctx, next) {
        ctx.data.push(true)
        next()
      })
      db.select('person', function (err, data) {
        done(_.last(data) !== true)
      })
    })
    it('uses plugin for select', function (done) {
      db.use(function (ctx, next) {
        if (ctx.sql.name == 'select')
          ctx.data.push(true)
        next()
      })
      db.createTable(+new Date, function () {
        db.select('person', function (err, data) {
          done(_.last(data) !== true)
        })
      })
    })
  })
})
