# db3
neat db interface for node-mysql

# Introduction
Db3 will replace SQL queries in your code with simple, clean and readable syntax. It's aim to provide shorthand methods for basic and most used queries, rather than trying to cover the whole SQL specification, so some operations are not supported (you can write them in SQL yourself). Db3 is based on excellent [node-mysql](https://github.com/felixge/node-mysql) lib.

## Installation
```
npm install db3
```

## Connecting
```
var db = new require('db3')({host: 'example.org', user: 'bob', password: 'secret'})
```
connection options object passed directly to [mysql.createPool](https://github.com/felixge/node-mysql#establishing-connections)

## Adding (insert ...)
```
//db.insert(table, data, callback)
db.insert('person', {name: 'Bob'}, function (data) {
  console.log('inserted row with id: ' + data.insertId)
})
```
## Updating (update ...)
```
//db.update(table, condition, data, callback)
db.update('person', {name: 'Bob'}, {name: 'Alice'}, function (data) {
  console.log('updated ' + data.changedRows + ' rows')
})
```
## Deleting (delete from ...)
```
//db.delete(table, condition, callback)
db.delete('person', {name: 'Alice'}, function (data) {
  console.log('deleted ' + data.affectedRows + ' rows')
})
```
## Saving (insert ... on duplicate key update ...)
```
//db.save(table, data, callback)
db.save('person', {id: 1, name: 'Bob'}, function (data) {
  console.log('saved row with id: ' + data.insertId)
})
```
## Counting (select count(*) from table)
//db.count(table, condition, callback)
```
db.count('person', {name: 'Bob'}, function (count) {
  console.log('there are ' + count + ' persons named "Bob"')  
})
```
