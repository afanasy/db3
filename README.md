[![NPM Downloads][downloads-image]][downloads-url]
[![Node.js Version][node-version-image]][node-version-url]
[![Linux Build][travis-image]][travis-url]

## Introduction
Db3 replaces SQL queries in your code with simple, clean and readable calls. Its aim is to provide shorthand methods for basic and most used SQL patterns, rather than trying to cover the whole SQL specification. It may be useful for those who doesn't know or doesn't want to use SQL, but still interested in using mysql as backend db. Db3 is based on excellent [node-mysql](https://github.com/felixge/node-mysql) lib. For PHP alternative check out the [Medoo](http://medoo.in/) project.

## Installation
```
npm install db3
```

## Connecting
```javascript
var db3 = require('db3')
var db = db3.connect({host: 'example.org', user: 'bob', password: 'secret', database : 'test'})
```
connection options object passed directly to [mysql.createPool](https://github.com/felixge/node-mysql#establishing-connections)

## Disconnecting
```javascript
//db.end(cb)
db.end(function (err) {
  console.log('all connections closed gracefully')
})
```
## Creating table (create table ...)
```javascript
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
```javascript
//db.dropTable(table, callback)
//drop table `person`;
db.dropTable('person', function () {
  console.log('table `person` dropped')
})
```

## Truncating table (truncate table ...)
```javascript
//db.truncateTable(table, callback)
//truncate table `person`;
db.truncateTable('person', function () {
  console.log('table `person` truncated')
})
```

## Copying table (create table ... like ... insert)
```javascript
//db.copyTable(from, to, callback)
//create table `personCopy` like `person`; insert `personCopy` select * from `person`;
db.copyTable('person', 'personCopy', function () {
  console.log('copied table `person` and all its data to table `personCopy`')
})
```

## Moving table (rename table ...)
```javascript
//db.moveTable(from, to, callback)
//rename table `person` to `nosrep`;
db.moveTable('person', 'nosrep', function () {
  console.log('moved table `person` and all its data to table `nosrep`')
})
```


## Checking if table exists
```javascript
//db.tableExists(table, callback)
db.tableExists('person', function (exists) {
  if (exists)
    console.log('table `person` exists')
  else
    console.log('table `person` does not exist')
})
```

## Adding (insert ...)
```javascript
//db.insert(table, data, callback)
//insert `person` set `name` = "Bob";
db.insert('person', {name: 'Bob'}, function (data) {
  console.log('inserted row into table `person` with id ' + data.insertId + ' and `name` set to "Bob"')
})
```

## Updating (update ...)
```javascript
//db.update(table, condition, data, callback)
//update `person` set `name` = "Bob" where `name` = "Alice";
db.update('person', {name: 'Bob'}, {name: 'Alice'}, function (data) {
  console.log('updated table `person`: ' + data.changedRows + ' rows named "Bob" changed name to "Alice"')
})
```

## Deleting (delete from ...)
```javascript
//db.delete(table, condition, callback)
//delete from `person` where `name` = "Alice";
db.delete('person', {name: 'Alice'}, function (data) {
  console.log('deleted ' + data.affectedRows + ' rows named "Alice" from table `person`')
})
```

## Saving (insert ... on duplicate key update ...)
```javascript
//db.save(table, data, callback)
//insert `person` set `id` = 1, `name` = "Bob" on duplicate key update `id` = 1, `name` = "Bob";
db.save('person', {id: 1, name: 'Bob'}, function (data) {
  console.log('saved row with id ' + data.insertId + ' and name set to "Bob" into table `person`')
})
```

## Selecting
```javascript
//db.select(table, condition, field, callback)
//select `name`, `gender` from `person` where `name` = "Bob";
db.select('person', {name: 'Bob'}, ['name', 'gender'], function (data) {
  console.log('selected name, gender fields from table `person`, where `name` = "Bob"')
  console.log(data)
  //[{name: 'Bob', gender: 'male'}, {name: 'Bob', gender: 'male'}, {name: 'Bob', gender: 'female'}, ...]
})
```

## Group by functions (count, min, max, avg, sum)
```javascript
//db[functionName](table, condition, field, callback)
//select count(*) from `person` where `name` = "Bob";
db.count('person', {name: 'Bob'}, function (count) {
  console.log('there are ' + count + ' persons named "Bob"')  
})
//select min(id) from `person` where `name` = "Bob";
db.min('person', {name: 'Bob'}, function (min) {
  console.log('first "Bob" has id ' + min)  
})
//select name, avg(age) from `person` where `name` = "Bob";
db.min('person', {name: 'Bob'}, ['age'], function (avg) {
  console.log('Bob average age is ' + avg)  
})
//select name, sum(income) from `person` where `city` = "Hong Kong" group by name;
db.min('person', {city: 'Hong Kong', year: '2015'}, ['gender', 'income'], function (data) {
  console.log('total income of HK citizens by gender for 2015')  
  console.log(data)
  //[{name: 'Yun', sum: someNumber}, {name: 'Tony', sum: someNumber}, {name: 'Donnie', sum: someNumber}, ...]
})
```

## SQL query
Proxied to the underlying node-mysql lib, but with swapped 'err' and 'data' arguments (more info [here](https://github.com/felixge/node-mysql#performing-queries))
```javascript
db.query('select ??, count(*) as count from ?? group by ?? order by id limit 10', ['gender', 'person', 'gender'], function (data) {
  console.log(data)
  //[{gender: 'male', count: someNumber}, {gender: 'female', count: someNumber}, ...]
})
```

[downloads-image]: https://img.shields.io/npm/dm/db3.svg
[downloads-url]: https://npmjs.org/package/db3
[node-version-image]: http://img.shields.io/node/v/db3.svg
[node-version-url]: http://nodejs.org/download/
[travis-image]: https://img.shields.io/travis/afanasy/db3/master.svg
[travis-url]: https://travis-ci.org/afanasy/db3
