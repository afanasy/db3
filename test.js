var
  expect = require('chai').expect,
  _ = require('underscore'),
  async = require('async'),
  csv = require('fast-csv'),
  db3 = require('./'),
  db = db3.connect({user: 'root', database : 'test'})

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
      db.dropTable.bind(db, 'test'),
      db.createTable.bind(db, 'test'),
      db.dropTable.bind(db, 'person'),
      db.createTable.bind(db, 'person', ['id', 'name', 'gender']),
      async.eachSeries.bind(async, person, db.insert.bind(db, 'person'))
    ], done)
  })
  describe('#connect()', function () {
    it('should connect to the db', function (done) {
      var db = db3.connect({user: 'root', database : 'test'})
      db.query('select 1', function (err, data) {
        expect(data).to.have.length(1)
        done()
      })
    })
  })
  describe('#end()', function () {
    it('should disconnect from db', function (done) {
      db3.connect({user: 'root', database : 'test'}).end(done)
    })
  })
  describe('#createTable()', function () {
    it('should create table', function (done) {
      var table = 'createTable' + +(new Date)
      db.createTable(table, function () {
        db.tableExists(table, function (err, exists) {
          expect(exists).to.be.true
          done()
        })
      })
    })
  })
  describe('#dropTable()', function () {
    it('should drop table', function (done) {
      var table = 'dropTable' + +(new Date)
      db.createTable(table, function () {
        db.dropTable(table, function () {
          db.tableExists(table, function (err, exists) {
            expect(exists).not.to.be.true
            done()
          })
        })
      })
    })
  })
  describe('#tableExists()', function () {
    it('should check if table exists', function (done) {
      db.tableExists('tableExists' + +(new Date), function (err, exists) {
        expect(exists).not.to.be.true
        done()
      })
    })
  })

  describe('#truncateTable()', function () {
    it('should truncate table', function (done) {
      var table = 'truncateTable' + +(new Date)
      db.createTable(table, function () {
        db.insert(table, function () {
          db.truncateTable(table, function () {
            db.count(table, function (err, count) {
              expect(count).to.equal(0)
              done()
            })
          })
        })
      })
    })
  })
  describe('#copyTable()', function () {
    it('should copy table and all its data to other table', function (done) {
      var table = 'copyTable' + +(new Date)
      db.createTable(table, function () {
        db.insert(table, function () {
          db.copyTable(table, function (err, data) {
            db.count(data.table, function (err, count) {
              expect(count).to.be.above(0)
              done()
            })
          })
        })
      })
    })
  })
  describe('#renameTable()', function () {
    it('should rename table', function (done) {
      var table = 'renameTable' + +(new Date)
      db.createTable(table, function () {
        db.insert(table, function () {
          db.renameTable(table, function (err, data) {
            db.count(data.table, function (err, count) {
              expect(count).to.be.above(0)
              done()
            })
          })
        })
      })
    })
  })
  describe('#insert()', function () {
    it('should insert empty item', function (done) {
      db.insert('test', function (err, data) {
        expect(data.insertId).to.be.above(0)
        done()
      })
    })
    it('should insert item', function (done) {
      db.insert('test', {name: 'test'}, function (err, data) {
        db.select('test', data.insertId, function (err, data) {
          expect(data.name).to.be.equal('test')
          done()
        })
      })
    })
    it('should create writeable insert stream', function (done) {
      var table = 'person' + +(new Date)
      db.createTable(table, ['id', 'name', 'gender'], function () {
        db.select('person')
        var stream = db.insert(table)
        stream.on('finish', function () {
          db.count(table, function (err, count) {
            expect(count).to.be.above(0)
            done()
          })
        })
        stream.write({})
        stream.end()
      })
    })
  })
  describe('#update()', function () {
    it('should update row', function (done) {
      db.insert('test', function (err, data) {
        var id = data.insertId
        db.update('test', id, {name: 'test'}, function (err, data) {
          db.select('test', id, function (err, data) {
            expect(data.name).to.equal('test')
            done()
          })
        })
      })
    })
  })
  describe('#delete()', function () {
    it('should delete row', function (done) {
      db.insert('test', function (err, data) {
        var id = data.insertId
        db.delete('test', id, function (err, data) {
          db.select('test', id, function (err, data) {
            expect(data).to.be.undefined
            done()
          })
        })
      })
    })
    it('should create writeable delete stream', function (done) {
      var table = 'person' + +(new Date)
      db.copyTable('person', table, function () {
        db.select(table).pipe(db.delete(table)).on('finish', function () {
          db.count(table, function (err, count) {
            expect(count).to.equal(0)
            done()
          })
        })
      })
    })
  })
  describe('#save()', function () {
    it('should insert a new row', function (done) {
      db.save('test', function (err, data) {
        db.select('test', data.insertId, function (err, data) {
          expect(data).to.have.property('id')
          done()
        })
      })
    })
    it('should update an existing row', function (done) {
      db.save('test', function (err, data) {
        var item = {id: data.insertId, name: 'test'}
        db.save('test', item, function (err, data) {
          db.select('test', item.id, function (err, data) {
            expect(data).to.have.property('name', item.name)
            done()
          })
        })
      })
    })
    it('should update an existing row using specified fields', function (done) {
      var item = {name: 'test'}
      db.save('test', item, function (err, data) {
        item.id = data.insertId
        item.name = 'tset'
        db.save('test', item, 'id', function (err, data) {
          db.select('test', item.id, function (err, data) {
            expect(data).to.have.property('name')
            expect(data.name).not.to.equal(item.name)
            done()
          })
        })
      })
    })
    it('should create writeable save stream', function (done) {
      var table = 'person' + +(new Date)
      db.createTable(table, ['id', 'name', 'gender'], function () {
        db.select('person').pipe(db.save(table)).on('finish', function () {
          db.count(table, function (err, count) {
            expect(count).to.equal(person.length)
            done()
          })
        })
      })
    })
  })
  describe('#select()', function () {
    it('should select an item from table', function (done) {
      db.select('person', {name: 'God'}, function (err, data) {
        expect(data[0].name).to.equal('God')
        done()
      })
    })
    it('should select an item from table using "in ()"', function (done) {
      db.select('person', {id: [1, 2]}, function (err, data) {
        expect(data).to.have.length(2)
        done()
      })
    })
    it('should select an item from table using "between"', function (done) {
      db.select('person', {id: {from: 2, to: 4}}, function (err, data) {
        expect(data).to.have.length(3)
        done()
      })
    })
    it('should select an item from table using ">="', function (done) {
      db.select('person', {id: {from: 2}}, function (err, data) {
        expect(data).to.have.length(5)
        done()
      })
    })
    it('should select an item from table using "<="', function (done) {
      db.select('person', {id: {to: 2}}, function (err, data) {
        expect(data).to.have.length(2)
        done()
      })
    })
    it('should select an item from table using shorthand id syntax', function (done) {
      db.select('person', 3, function (err, data) {
        expect(data.name).to.equal('Eve')
        done()
      })
    })
    it('should select an item field from table using shorthand id/name syntax', function (done) {
      db.select('person', 3, 'name', function (err, data) {
        expect(data).to.equal('Eve')
        done()
      })
    })
    it('should create readable select stream', function (done) {
      var count = 0
      db.select('person').
        on('data', function (err, data) {count++}).
        on('end', function () {
          expect(count).to.be.above(0)
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
      it('should select ' + name  + ' from table', function (done) {
        db[func]('person', function (err, data) {
          expect(data).to.equal(groupBy[func].fullTable)
          done()
        })
      })
      it('should select ' + name + ' from table filtered by condition', function (done) {
        db[func]('person', {name: 'Adam'}, function (err, data) {
          expect(data).to.equal(groupBy[func].filtered)
          done()
        })
      })
      it('should select ' + name + ' from table grouped by field', function (done) {
        db[func]('person', ['gender', 'id'], function (err, data) {
          expect(_.findWhere(data, groupBy[func].grouped)).to.be.ok
          done()
        })
      })
      it('should select ' + name + ' from table filtered by condition and grouped by field', function (done) {
        db[func]('person', {name: 'Cain'}, ['gender', 'id'], function (err, data) {
          expect(_.findWhere(data, groupBy[func].filteredGrouped)).to.be.ok
          done()
        })
      })
    })
  })
  describe('#query()', function () {
    it('should return summary data grouped by name', function (done) {
      db.query('select ??, count(*) from ?? group by ??', ['name', 'test', 'name'], function (err, data) {
        expect(data).to.have.length.above(0)
        done()
      })
    })
    it('should return readable stream if no callback provided', function (done) {
      var count = 0
      db.query('select * from ??', 'person').
        on('data', function (err, data) {count++}).
        on('end', function () {
          expect(count).to.equal(person.length)
          done()
        })
    })
  })
  describe('#csv()', function () {
    it('should stream select to csv', function (done) {
      var stream = csv.format({headers: true})
      var count = 0
      stream.on('data', function (err, data) {count++})
      stream.on('finish', function () {
        expect(count).to.be.above(0)
        done()
      })
      db.select('person').pipe(stream)
    })
    it('should stream csv to insert', function (done) {
      var table = 'csv' + +(new Date)
      db.createTable(table, function () {
        var stream = db.insert(table)
        stream.on('finish', function () {
          db.count(table, function (err, count) {
            expect(count).to.be.above(0)
            done()
          })
        })
        csv.fromString("name\ncsv1\ncsv2\n", {headers: true}).pipe(stream)
      })
    })
  })
})
