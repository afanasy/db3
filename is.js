function tagTester (name) {
  var tag = '[object ' + name + ']'
  return obj => toString.call(obj) === tag
}

module.exports = {
  array: Array.isArray,
  function: tagTester('Function'),
  number: tagTester('Number'),
  object: d => {
    var type = typeof d
    return type === 'function' || type === 'object' && !!d
  },
  string: tagTester('String'),
  undefined: d => d === undefined
}
