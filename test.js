var
  should = require('chai').expect(),
  db3 = require('./db3.js'),
  db = db3.connect({user: 'root', database : 'test'})

describe('Db3', function () {
  before(function (done) {
    db.dropTable('test', function () {
      db.createTable('test', ['id', 'name'], function () {
        done()
      })
    })
  })
  describe('#connect()', function () {
    it('should connect to the test db', function (done) {
      var db = db3.connect({user: 'root', database : 'test'})
      db.query('select 1', function (data) {
        if (!data || !data[0])
          return done(new Error('test db is not working'))
        done()
      })
    })
  })
  describe('#end()', function () {
    it('should connect to the test db and then disconnect', function (done) {
      var db = db3.connect({user: 'root', database : 'test'})
      db.end(function () {done()})
    })
  })
  describe('#createTable()', function () {
    it('should create table', function (done) {
      var table = 'test' + +(new Date)
      db.createTable(table, function () {
        db.tableExists(table, function (exists) {
          if (!exists)
            return done(new Error('"' + table + '" table does not exist'))
          return done()
        })
      })
    })
  })
  describe('#dropTable()', function () {
    it('should create and drop table', function (done) {
      var table = 'test' + +(new Date)
      db.createTable(table, function () {
        db.dropTable(table, function () {
          db.tableExists(table, function (exists) {
            if (exists)
              return done(new Error('"' + table + '" table was not dropped'))
            return done()
          })
        })
      })
    })
  })
  describe('#tableExists()', function () {
    it('should check if random table exists', function (done) {
      db.tableExists('test' + +(new Date), function (exists) {
        if (exists)
          return done(new Error('"' + table + '" table exists'))
        return done()
      })
    })
  })
  describe('#insert()', function () {
    it('should insert empty item', function (done) {
      db.insert('test', function (data) {
        if (!data.insertId)
          return done(new Error('no insert id'))
        done()
      })
    })
    it('should insert item with name "test"', function (done) {
      db.insert('test', {name: 'test'}, function (data) {
        if (!data.insertId)
          return done(new Error('no insert id'))
        db.select('test', data.insertId, function (data) {
          if (!data || !data[0] || (data[0].name != 'test'))
            return done(new Error('inserted item has wrong name field'))
          done()
        })
      })
    })
  })
  describe('#update()', function () {
    it('should update empty item name to "test"', function (done) {
      db.insert('test', function (data) {
        if (!data.insertId)
          return done(new Error('no insert id'))
        var id = data.insertId
        db.update('test', id, {name: 'test'}, function (data) {
          if (!data || !data.changedRows)
            return done(new Error('update failed'))
          db.select('test', id, function (data) {
            if (!data || !data[0] || (data[0].name != 'test'))
              return done(new Error('inserted item has wrong name field'))
            done()
          })
        })
      })
    })
  })
  describe('#delete()', function () {
    it('should delete item', function (done) {
      db.insert('test', function (data) {
        if (!data.insertId)
          return done(new Error('no insert id'))
        var id = data.insertId
        db.delete('test', id, function (data) {
          if (!data || !data.affectedRows)
            return done(new Error('update failed'))
          db.select('test', id, function (data) {
            if (data && data[0])
              return done(new Error('item was not deleted'))
            done()
          })
        })
      })
    })
  })
  describe('#save()', function () {
    it('should insert a new item', function (done) {
      db.save('test', function (data) {
        if (!data.insertId)
          return done(new Error('no insert id'))
        db.select('test', data.insertId, function (data) {
          if (!data || !data[0])
            return done(new Error('item was not added'))
          done()
        })
      })
    })
    it('should update an existing item', function (done) {
      db.save('test', function (data) {
        if (!data.insertId)
          return done(new Error('no insert id'))
        var item = {id: data.insertId, name: 'test'}
        db.save('test', item, function (data) {
          db.select('test', item.id, function (data) {
            if (!data || !data[0] || (data[0].name != item.name))
              return done(new Error('item was not updated'))
            done()
          })
        })
      })
    })
  })
  describe('#select()', function () {
    it('should add and select an item', function (done) {
      var item = {name: 'test'}
      db.insert('test', item, function (data) {
        if (!data.insertId)
          return done(new Error('no insert id'))
        db.select('test', data.insertId, function (data) {
          if (!data || !data[0] || (data[0].name != item.name))
            return done(new Error('item was not found'))
          done()
        })
      })
    })
  })
  describe('#count()', function () {
    it('should count items with name "test" in a table', function (done) {
      db.count('test', {name: "test"}, function (data) {
        if (!data)
          return done(new Error('no items found'))
        done()
      })
    })
  })
  describe('#query()', function () {
    it('should return summary data grouped by name', function (done) {
      db.query('select ??, count(*) from ?? group by ??', ['name', 'test', 'name'], function (data) {
        if (!data || !data.length)
          return done(new Error('no items found'))
        done()
      })
    })
  })
})
