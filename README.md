# db3
neat db interface for node-mysql

## Introduction
Db3 replaces SQL queries in your code with simple, clean and readable calls. Its aim is to provide shorthand methods for basic and most used query patterns, rather than trying to cover the whole SQL specification - so some operations are not supported (you can write them in SQL directly). Db3 is based on excellent [node-mysql](https://github.com/felixge/node-mysql) lib.

## Installation
```
npm install db3
```

## Connecting
```
var Db3 = require('db3')
var db = new Db3({host: 'example.org', user: 'bob', password: 'secret'})
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
## Selecting
//db.select(table, condition, field, callback)
```
db.select('persons', {name: 'Bob'}, ['name', 'gender'], function (data) {
  console.log(data)
})
```
## Counting (select count(*) from table)
//db.count(table, condition, callback)
```
db.count('person', {name: 'Bob'}, function (count) {
  console.log('there are ' + count + ' persons named "Bob"')  
})
```
