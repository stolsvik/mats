/// Small helper to remove the null values from a map.
/// This will create a new map, that only consist of the values from the src
/// map that are non-null. This is usefull to produce compact json, where we
/// don't want to include null keys.
Map<String, dynamic> removeNullValues(Map<String, dynamic> src) {
  var res = <String, dynamic>{};
  src.forEach((key, value) {
    if (value != null) {
      res[key] = value;
    }
  });
  return res;
}