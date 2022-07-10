function tagTester (name) {
  var tag = '[object ' + name + ']'
  return obj => toString.call(obj) === tag
}

module.exports = {
  array: Array.isArray,
  boolean: d => d === true || d === false || toString.call(d) === '[object Boolean]',
  function: tagTester('Function'),
  nan: d => module.exports.number(d) && isNaN(d),
  null: d => d === null,
  number: tagTester('Number'),
  object: d => {
    var type = typeof d
    return type === 'function' || type === 'object' && !!d
  },
  string: tagTester('String'),
  undefined: d => d === void 0
}
