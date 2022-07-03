var db3 = require('./')
var db = db3.connect({user: 'root', database : 'test'})

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
    db.dropTable('test', function () {
      db.dropTable('person', function () {
        db.createTable('test', () => {
          db.createTable('person', ['id', 'name', 'gender'], () => {
            function insert (i) {
              if (!person[i])
                return done()
              db.insert('person', person[i], () => {
                insert(i + 1)
              })
            }
            insert(0)
          })
        })
      })
    })
  })
  describe('#connect()', function () {
    it('connects to the db', function (done) {
      var db = db3.connect({user: 'root', database : 'test'})
      db.query('select 1', function (err, data) {
        done(data.length != 1)
      })
    })
  })
  describe('#end()', function () {
    it('disconnects from db', function (done) {
      db3.connect({user: 'root', database : 'test'}).end(done)
    })
  })
  describe('#createTable()', function () {
    it('creates table', function (done) {
      var table = 'createTable' + +(new Date)
      db.createTable(table, function () {
        db.tableExists(table, function (err, exists) {
          done(exists != true)
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
        db.update('test', id, {name: 'test'}, function () {
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
          db.update('test', id, {name: 'test'}, function () {
            db.select('test', id, function (err, data) {
              done((data.length != id.length) || (data[0].name != 'test') || (data[1].name != 'test'))
            })
          })
        })
      })
    })
  })
  describe('#delete()', function () {
    it('deletes row', function (done) {
      db.insert('test', function (err, data) {
        var id = data.insertId
        db.delete('test', id, function () {
          db.select('test', id, function (err, data) {
            done(data)
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
          done(!+data.id)
        })
      })
    })
    it('updates an existing row', function (done) {
      db.save('test', function (err, data) {
        var item = {id: data.insertId, name: 'test'}
        db.save('test', item, function () {
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
        db.save('test', item, 'id', function () {
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
        on('data', () => count++).
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
    it('does not fail with select error', function (done) {
      db.select('', 1, () => {
        done()
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
        filteredGrouped: {gender: 'male', count: 1}
      },
      min: {
        fullTable: 1,
        filtered: 2,
        grouped: {gender: 'female', min: 3},
        filteredGrouped: {gender: 'male', min: 4}
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
    ;['count', 'min', 'max', 'avg', 'sum'].forEach(func => {
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
          // done(!_.findWhere(data, groupBy[func].grouped))
          done(!data.find(d => !Object.keys(groupBy[func].grouped).find(key => groupBy[func].grouped[key] != d[key])))
        })
      })
      it('selects ' + name + ' from table filtered by condition and grouped by field', function (done) {
        db[func]('person', {name: 'Cain'}, ['gender', 'id'], function (err, data) {
          // done(!_.findWhere(data, groupBy[func].filteredGrouped))
          done(!data.find(d => !Object.keys(groupBy[func].filteredGrouped).find(key => groupBy[func].filteredGrouped[key] != d[key])))
        })
      })
    })
  })
  describe('#query()', function () {
    it('returns summary data grouped by name', function (done) {
      db.query('select ??, count(*) from ?? group by ??', ['name', 'test', 'name'], function (err, data) {
        done(!data.length)
      })
    })
    it('returns readable stream if no callback provided', function (done) {
      var count = 0
      db.query('select * from ??', 'person').
        on('data', () => count++).
        on('end', function () {
          done(count != person.length)
        })
    })
  })
})
