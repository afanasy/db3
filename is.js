function tagTester (name) {
  var tag = '[object ' + name + ']'
  return obj => toString.call(obj) === tag
}

module.exports = {
  array: Array.isArray,
  function: tagTester('Function'),
  number: tagTester('Number'),
  string: tagTester('String'),
  undefined: d => d === undefined
}
