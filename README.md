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
var db3 = require('db3')
var db = db3.connect({host: 'example.org', user: 'bob', password: 'secret', database : 'test'})
```
connection options object passed directly to [mysql.createPool](https://github.com/felixge/node-mysql#establishing-connections)

##Disconnecting
```
//db.end(cb)
db.end(function (err) {
  console.log('all connections closed gracefully')
})
```
## Creating table (create table ...)
```
//db.createTable(table, fields, callback)
/*
create table `person` (
  id bigint primary key auto_increment,
  name text,
  gender text
);
*/
db.createTable('person', ['id', 'name', 'gender'], function (data) {
  console.log('created table `person` with field `id`, `name`, `gender`')
})
```
all fields will be of `text` type, except `id` (will be "bigint primary key auto_increment") and fields matching /Id$/, like userId (will become bigint)

## Droping table (drop table ...)
```
//db.dropTable(table, callback)
//drop table `person`;
db.dropTable('person', function () {
  console.log('table `person` dropped')
})
```

## Checking if table exists
```
//db.tableExists(table, callback)
db.tableExists('person', function (exists) {
  if (exists)
    console.log('table `person` exists')
  else
    console.log('table `person` does not exist')
})
```

## Adding (insert ...)
```
//db.insert(table, data, callback)
//insert `person` set `name` = "Bob";
db.insert('person', {name: 'Bob'}, function (data) {
  console.log('inserted row into table `person` with id ' + data.insertId + ' and `name` set to "Bob"')
})
```

## Updating (update ...)
```
//db.update(table, condition, data, callback)
//update `person` set `name` = "Bob" where `name` = "Alice";
db.update('person', {name: 'Bob'}, {name: 'Alice'}, function (data) {
  console.log('updated table `person`: ' + data.changedRows + ' rows named "Bob" changed name to "Alice"')
})
```

## Deleting (delete from ...)
```
//db.delete(table, condition, callback)
//delete from `person` where `name` = "Alice";
db.delete('person', {name: 'Alice'}, function (data) {
  console.log('deleted ' + data.affectedRows + ' rows named "Alice" from table `person`')
})
```

## Saving (insert ... on duplicate key update ...)
```
//db.save(table, data, callback)
//insert `person` set `id` = 1, `name` = "Bob" on duplicate key update `id` = 1, `name` = "Bob";
db.save('person', {id: 1, name: 'Bob'}, function (data) {
  console.log('saved row with id ' + data.insertId + ' and name set to "Bob" into table `person`')
})
```

## Selecting
```
//db.select(table, condition, field, callback)
//select `name`, `gender` from `person` where `name` = "Bob";
db.select('person', {name: 'Bob'}, ['name', 'gender'], function (data) {
  console.log('selected name, gender fields from table `person`, where `name` = "Bob"')
  console.log(data)
})
```

## Counting (select count(*) from table)
```
//db.count(table, condition, callback)
//select count(*) from `person` where `name` = "Bob";
db.count('person', {name: 'Bob'}, function (count) {
  console.log('there are ' + count + ' persons named "Bob"')  
})
```

## SQL query
Proxied to the underlying node-mysql lib, but with swapped 'err' and 'data' arguments (more info [here](https://github.com/felixge/node-mysql#performing-queries))
```
db.query('select ??, count(*) from ?? group by ??', ['gender', 'person', 'gender'], function (data) {
  console.log(data)
})
```
