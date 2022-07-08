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

var mocha = {
  test: [],
  depth: 0,
  describe: (title, done) => {
    mocha.test.push({type: 'describe', title, depth: mocha.depth})
    mocha.depth++
    done()
    mocha.depth--
  },
  it: (title, run) => {
    mocha.test.push({type: 'test', title, depth: mocha.depth, run}) 
  },
  run: () => {
    !function next (i) {
      var test = mocha.test[i]
      if (!i)
        start = Date.now()
      if (!test) {
        console.log(pass + ' passing, ' + fail + ' failed (' + (Date.now() - start) + 'ms)')
        process.exit(fail? 1: 0)
        return
      }
      var indent = '  '.repeat(test.depth)
      if (test.type == 'describe') {
        console.log(indent, test.title)
        return next(i + 1)
      }
      test.run(err => {
        if (err)
          fail++
        else
          pass++
        console.log(indent, (err? '✖': '✓') + ' ' + test.title)
        next(i + 1)
      })
    }(0)
    var start
    var pass = 0
    var fail = 0
  }
}

var describe = mocha.describe
var it = mocha.it

db.dropTable('test', () => {
  db.dropTable('person', () => {
    db.createTable('test', () => {
      db.createTable('person', ['id', 'name', 'gender'], () => {
        function insert (i) {
          if (!person[i])
            return mocha.run()
          db.insert('person', person[i], () => {
            insert(i + 1)
          })
        }
        insert(0)
      })
    })
  })
})

describe('Db3', () => {
  describe('#connect()', () => {
    it('connected to the db', done => {
      db.query('select 1', (err, data) => {
        done(data.length != 1)
      })
    })
  })
  describe('#end()', () => {
    it('disconnects from db', done => {
      db3.connect({user: 'root', database : 'test'}).end(done)
    })
  })
  describe('#createTable()', () => {
    it('creates table', done => {
      db.createTable((err, data) => {
        db.tableExists(data.table, (err, exists) => {
          done(!exists)
        })
      })
    })
    it('creates table with table name', done => {
      var table = 'createTable' + Date.now()
      db.createTable(table, () => {
        db.tableExists(table, (err, exists) => {
          done(!exists)
        })
      })
    })
  })
  describe('#dropTable()', () => {
    it('drops table', done => {
      var table = 'dropTable' + +(new Date)
      db.createTable(table, () => {
        db.dropTable(table, () => {
          db.tableExists(table, (err, exists) => {
            done(exists)
          })
        })
      })
    })
  })
  describe('#tableExists()', () => {
    it('checks if table exists', done => {
      db.tableExists('tableExists' + +(new Date), (err, exists) => {
        done(exists)
      })
    })
  })
  describe('#truncateTable()', () => {
    it('truncates table', done => {
      var table = 'truncateTable' + +(new Date)
      db.createTable(table, () => {
        db.insert(table, () => {
          db.truncateTable(table, () => {
            db.count(table, (err, count) => {
              done(count)
            })
          })
        })
      })
    })
  })
  describe('#copyTable()', () => {
    it('copies table and all its data to other table', done => {
      db.createTable((err, data) => {
        db.insert(data.table, () => {
          db.copyTable(data.table, (err, data) => {
            db.count(data.table, (err, count) => {
              done(!count)
            })
          })
        })
      })
    })
  })
  describe('#renameTable()', () => {
    it('renames table', done => {
      db.createTable((err, data) => {
        db.insert(data.table, () => {
          db.renameTable(data.table, (err, data) => {
            db.count(data.table, (err, count) => {
              done(!count)
            })
          })
        })
      })
    })
    it('renames table with table name', done => {
      var table = 'renameTable' + Date.now()
      db.createTable(table, () => {
        db.insert(table, () => {
          db.renameTable(table, (err, data) => {
            db.count(data.table, (err, count) => {
              done(!count)
            })
          })
        })
      })
    })
  })
  describe('#insert()', () => {
    it('inserts empty item', done => {
      db.insert('test', (err, data) => {
        done(!data.insertId)
      })
    })
    it('inserts item', done => {
      db.insert('test', {name: 'test'}, (err, data) => {
        db.select('test', data.insertId, (err, data) => {
          done(data.name != 'test')
        })
      })
    })
    it('creates writeable insert stream', done => {
      var table = 'person' + +(new Date)
      db.createTable(table, ['id', 'name', 'gender'], () => {
        var stream = db.insert(table)
        stream.on('finish', () => {
          db.count(table, (err, count) => {
            done(!count)
          })
        })
        stream.write({})
        stream.end()
      })
    })
  })
  describe('#update()', () => {
    it('updates row', done => {
      db.insert('test', (err, data) => {
        var id = data.insertId
        db.update('test', id, {name: 'test'}, () => {
          db.select('test', id, (err, data) => {
            done(data.name != 'test')
          })
        })
      })
    })
    it('updates rows with array condition', done => {
      db.insert('test', (err, data) => {
        var id = [data.insertId]
        db.insert('test', (err, data) => {
          id.push(data.insertId)
          db.update('test', id, {name: 'test'}, () => {
            db.select('test', id, (err, data) => {
              done((data.length != id.length) || (data[0].name != 'test') || (data[1].name != 'test'))
            })
          })
        })
      })
    })
  })
  describe('#delete()', () => {
    it('deletes row', done => {
      db.insert('test', (err, data) => {
        var id = data.insertId
        db.delete('test', id, () => {
          db.select('test', id, (err, data) => {
            done(data)
          })
        })
      })
    })
    it('creates writeable delete stream', done => {
      var table = 'person' + +(new Date)
      db.copyTable('person', table, () => {
        db.select(table).pipe(db.delete(table)).on('finish', () => {
          db.count(table, (err, count) => {
            done(count)
          })
        })
      })
    })
  })
  describe('#save()', () => {
    it('inserts a new row', done => {
      db.save('test', (err, data) => {
        db.select('test', data.insertId, (err, data) => {
          done(!+data.id)
        })
      })
    })
    it('updates an existing row', done => {
      db.save('test', (err, data) => {
        var item = {id: data.insertId, name: 'test'}
        db.save('test', item, () => {
          db.select('test', item.id, (err, data) => {
            done(data.name != item.name)
          })
        })
      })
    })
    it('updates an existing row using specified fields', done => {
      var item = {name: 'test'}
      db.save('test', item, (err, data) => {
        item.id = data.insertId
        item.name = 'tset'
        db.save('test', item, 'id', () => {
          db.select('test', item.id, (err, data) => {
            done(!data.name || (data.name == item.name))
          })
        })
      })
    })
    it('creates writeable save stream', done => {
      var table = 'person' + +(new Date)
      db.createTable(table, ['id', 'name', 'gender'], () => {
        db.select('person').pipe(db.save(table)).on('finish', () => {
          db.count(table, (err, count) => {
            done(count != person.length)
          })
        })
      })
    })
  })
  describe('#duplicate()', () => {
    it('duplicates row', done => {
      db.insert('test', (err, data) => {
        var id = data.insertId
        db.duplicate('test', id, (err, data) => {
          if (data.insertId == id)
            return done(true)
          db.count('test', data.insertId, (err, data) => {
            done(data != 1)
          })
        })
      })
    })
  })
  describe('#select()', () => {
    it('selects an item from table', done => {
      db.select('person', {name: 'God'}, (err, data) => {
        done(data[0].name != 'God')
      })
    })
    it('selects an item from table using "in ()"', done => {
      db.select('person', {id: [1, 2]}, (err, data) => {
        done(data.length != 2)
      })
    })
    it('selects an item from table using "between"', done => {
      db.select('person', {id: {from: 2, to: 4}}, (err, data) => {
        done(data.length != 3)
      })
    })
    it('selects an item from table using ">="', done => {
      db.select('person', {id: {from: 2}}, (err, data) => {
        done(data.length != 5)
      })
    })
    it('selects an item from table using "<="', done => {
      db.select('person', {id: {to: 2}}, (err, data) => {
        done(data.length != 2)
      })
    })
    it('selects an item from table using shorthand id syntax', done => {
      db.select('person', 3, (err, data) => {
        done(data.name != 'Eve')
      })
    })
    it('selects an item field from table using shorthand id/name syntax', done => {
      db.select('person', 3, 'name', (err, data) => {
        done(data != 'Eve')
      })
    })
    it('creates readable select stream', done => {
      var count = 0
      db.select('person').
        on('data', () => count++).
        on('end', () => {
          done(!count)
        })
    })
    it('selects from object with orderBy', done => {
      db.select({table: 'person', field: 'name', orderBy: {name: 'desc'}}, (err, data) => {
        done(data[0] != 'Seth')
      })
    })
    it('selects from object with limit', done => {
      db.select({table: 'person', limit: 10}, (err, data) => {
        done(data.length != person.length)
      })
    })
    it('selects with empty condition', done => {
      db.select('person', {}, (err, data) => {
        done(data.length != person.length)
      })
    })
    it('does not fail with select error', done => {
      db.select('', 1, () => {
        done()
      })
    })
  })
  describe('#groupBy()', () => {
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
      it('selects ' + name  + ' from table', done => {
        db[func]('person', (err, data) => {
          done(data != groupBy[func].fullTable)
        })
      })
      it('selects ' + name + ' from table filtered by condition', done => {
        db[func]('person', {name: 'Adam'}, (err, data) => {
          done(data != groupBy[func].filtered)
        })
      })
      it('selects ' + name + ' from table grouped by field', done => {
        db[func]('person', ['gender', 'id'], (err, data) => {
          done(!data.find(d => !Object.keys(groupBy[func].grouped).find(key => groupBy[func].grouped[key] != d[key])))
        })
      })
      it('selects ' + name + ' from table filtered by condition and grouped by field', done => {
        db[func]('person', {name: 'Cain'}, ['gender', 'id'], (err, data) => {
          done(!data.find(d => !Object.keys(groupBy[func].filteredGrouped).find(key => groupBy[func].filteredGrouped[key] != d[key])))
        })
      })
    })
  })
  describe('#query()', () => {
    it('returns summary data grouped by name', done => {
      db.query('select ??, count(*) from ?? group by ??', ['name', 'test', 'name'], (err, data) => {
        done(!data.length)
      })
    })
    it('returns readable stream if no callback provided', done => {
      var count = 0
      db.query('select * from ??', 'person').
        on('data', () => count++).
        on('end', () => {
          done(count != person.length)
        })
    })
  })
})
