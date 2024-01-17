import os
import json
import traceback

LOG_FILE_QUERY_PREFIX = "/var/mitmproxy/query/"
LOG_FILE_QUERY_PARSED = os.path.join(LOG_FILE_QUERY_PREFIX, "query.txt")

class Result:
  def __init__(self, sql, can_parse = True, skipping_comments = []):
    self.sql = sql
    self.can_parse = can_parse
    self.skipping_comments = skipping_comments

  def add_skipping_comment(self, comment):
    self.skipping_comments.append(comment)

  def __str__(self):
    return "(sql: {sql}, skipping_comments: {skipping_comments}, can_parse: {can_parse})".format(
      sql=self.sql, skipping_comments=self.skipping_comments, can_parse=self.can_parse)
  
def createResultOr(results):
    sql = '(' + " OR ".join([r.sql for r in results]) + ')'
    can_parse = all([r.can_parse for r in results])
    skipping_comments = [comment for result in results for comment in result.skipping_comments]
    return Result(sql, can_parse, skipping_comments)

def createResultAnd(results):
    sql = '(' + " AND ".join([r.sql for r in results]) + ')' if len(results) > 1 else results[0].sql
    can_parse = all([r.can_parse for r in results])
    skipping_comments = [comment for result in results for comment in result.skipping_comments]
    return Result(sql, can_parse, skipping_comments)

def createNot(results):
    sql = '(NOT ' + results.sql + ')'
    return Result(sql, results.can_parse, results.skipping_comments)

def iterateListOrDictionary(json_object):
  if isinstance(json_object, list):
    for el in json_object:
      yield el
  elif isinstance(json_object, dict):
    yield json_object
  else:
    raise TypeError("Input 'json_object' must be a list or dictionary")
  
def _parse_bool(bool_json: dict):
  comments = []
  results = []
  mustOrFiltCount = 0
  for andPhrase in ['must', 'filter']:
    if andPhrase in bool_json:
      for el in iterateListOrDictionary(bool_json[andPhrase]):
        mustOrFiltCount += 1
        results.append(_parse_query(el))
  minimum_should_match = 1
  if 'minimum_should_match' in bool_json:
    minimum_should_match = bool_json['minimum_should_match']
    if minimum_should_match != 0 or minimum_should_match != 1:
      comments.append('Skipping {minimum_should_match} minimum_should_match, assuming 1')
      minimum_should_match = 1
  else:
    if mustOrFiltCount > 1:
      minimum_should_match = 0
  
  if minimum_should_match == 1:
    resultsOr = []
    if 'should' in bool_json:
      for el in iterateListOrDictionary(bool_json['should']):      
        resultsOr.append(_parse_query(el))
    if len(resultsOr) > 0:
      results.append(createResultOr(resultsOr))

  # Must not
  if 'must_not' in bool_json:
    resultsNot = []
    for el in iterateListOrDictionary(bool_json['must_not']):
      resultsNot.append(_parse_query(el))
    if len(resultsNot) > 0:
      results.append(createNot(createResultOr(resultsNot)))
  # print("  _parse_bool, bool_json: ", bool_json, " results: ", results[0])
  return createResultAnd(results)

def _parse_multi_match(multi_match_json: dict):
  # TODO: Way more complex
  if 'type' not in multi_match_json or multi_match_json['type'] == 'best_fields':
    return Result('any_field contains ' + multi_match_json['query'], True)
  else:
    return Result('Not implemented', False, ['Invalid multi_match'])

# term_type = 'term' or 'terms'
def _parse_term(term_json: dict, term_type):
  if len(term_json) != 1:
    return Result(term_type + ' len should be 1', False, ['Invalid ' + term_type])
  for key in term_json.keys():
    return Result(key + ' = ' + str(term_json[key]), True)

def _parse_match(match_json: dict, match_type: str):
  if len(match_json) != 1:
    return Result('match len supported = 1', False, ['Invalid match'])
  for key in match_json.keys():
    return Result(key + ' ' + match_type + ('s ' if match_type[-1] == 'e' else 'es ') + str(match_json[key]), True)

def _parse_simple_query_string(simple_query_string_json: dict):
  fields = simple_query_string_json["fields"]
  if len(fields) == 1 \
      and all([c.isalpha() or c == '-' or c == "." or c == '_' for c in fields[0]]) \
      and all([c.isalpha() or c.isnumeric() or c == "_" for c in simple_query_string_json['query']]):
    return Result(fields[0] + ' queries ' + simple_query_string_json['query'], True)
  return Result('"hard" simple_query_string not supported', False, ['"Hard" simple_query_string'])

def _parse_exists(exists_json: dict):
  if len(exists_json) != 1:
    return Result('exists len supported = 1', False, ['Invalid exists'])
  return Result('exists ' + exists_json['field'], True)

def _parse_range(range_json: dict):
  # TODO: Way more complex
  for key in range_json.keys():
    SQL = key
    if 'format' in range_json[key]:
      SQL += ' in format ' + str(range_json[key]['format'])
    if 'gte' in range_json[key]:
      SQL += ' >= ' + str(range_json[key]['gte'])
    if 'gt' in range_json[key]:
      SQL += ' > ' + str(range_json[key]['gt'])
    if 'lt' in range_json[key]:
      SQL += ' < ' + str(range_json[key]['lt'])
    if 'lte' in range_json[key]:
      SQL += ' <= ' + str(range_json[key]['lte'])
    return Result('(' + SQL + ')', True)
  return Result('Invalid', False, ['Invalid range, lack of key'])
      
def _parse_query(query_json: dict):
  # print("_parse_query:", len(query_json), query_json)
  if not isinstance(query_json, dict):
        raise TypeError("Input 'query_json' must be a dictionary ")
  # TODO: Check if no extra fields
  if 'bool' in query_json:
    return _parse_bool(query_json['bool'])
  elif 'query' in query_json:
    if len(query_json) == 1:
      return _parse_query(query_json['query'])
    return Result('Need only 1 query', False, ['Invalid query'])
  elif 'boosting' in query_json:
    result = _parse_query(query_json['boosting']['positive'])
    result.add_skipping_comment('Skipping boosting')
    return result
  elif 'constant_score' in query_json:
    result = _parse_query(query_json['constant_score']['filter'])
    result.add_skipping_comment('Skipping constant score')
    return result
  elif 'dis_max' in query_json:
    results = []
    for el in query_json['dis_max']['queries']:
      results.append(_parse_query(el))
    result = createResultOr(results)
    result.add_skipping_comment('Skipping dis_max')
    return result
  elif 'multi_match' in query_json:
    return _parse_multi_match(query_json['multi_match'])
  elif 'range' in query_json:
    return _parse_range(query_json['range'])
  elif 'term' in query_json:
    return _parse_term(query_json['term'], 'term')
  elif 'terms' in query_json:
    return _parse_term(query_json['terms'], 'terms')
  elif 'match' in query_json:
    return _parse_match(query_json['match'], 'match')
  elif 'match_phrase' in query_json:
    return _parse_match(query_json['match_phrase'], 'match_phrase')
  elif 'exists' in query_json:
    return _parse_exists(query_json['exists'])
  elif 'simple_query_string' in query_json:
    return _parse_simple_query_string(query_json['simple_query_string'])
  elif 'nested' in query_json:
    return Result('Not implemented yet', False, ['"Nested" not implemented yet'])
  elif 'wildcard' in query_json:
    return Result('Not implemented yet', False, ['"Wildcard" not implemented yet'])
  elif 'prefix' in query_json:
    return Result('Not implemented yet', False, ['"Prefix" not implemented yet'])
  else:  
    return Result('Not implemented yet', False, ['Invalid query'])

def safe_parse_query(request_json):
  try:
    return _parse_query(request_json)
  except Exception as e:
    print("safe_parse_query:", e)
    traceback.print_exc()
    print("json:", json.dumps(request_json, indent=2))
    print("\n\n")
    return Result('Invalid ' + str(e), False, ['Invalid query'])


def parsed_query_json(index_name, method, request_json):
  try:
    if method.startswith('/'):
      method = method[1:]

    with open(LOG_FILE_QUERY_PARSED, "a") as ofile:
      result = safe_parse_query(request_json)
      if result.can_parse:
        ofile.write("PASS: ")
        ofile.write("{index_name} {method}\n".format(index_name=index_name, method=method))
      else:
        ofile.write("FAIL: ")
        ofile.write("{index_name} {method}\n".format(index_name=index_name, method=method))
        ofile.write(str(result))
        ofile.write("\n")
        request_str = json.dumps(request_json, indent=2)
        ofile.write(request_str)
        ofile.write("\n\n")
  except Exception as e:
    print("parsed_query_json: Error. 4", e)
    traceback.print_exc()
