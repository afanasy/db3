var
  should = require('chai').expect(),
  Db3 = require('./db3.js'),
  db = new Db3({user: 'root', database : 'passio3_db', connectionLimit: 100, dateStrings: true, supportBigNumbers: true, bigNumberStrings: true})

describe('Db3', function (){
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
    it('should save item, and insert a new item', function (done) {
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
    it('should save item, and update an existing item', function (done) {
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
})
