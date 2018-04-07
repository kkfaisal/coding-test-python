"""
Answer for question 5.
Json arrays are not considered for path specification

"""

import json
import sys


def json_parse(json_string,path_string):
    js_obj=json.loads(json_string)
    path = path_string.split('.')

    for k in path:
        js_obj=js_obj[k]
    return js_obj

if __name__ == '__main__':

    #Simple testing
    print(json_parse("""{"a":{"b":{"c":111}}}""",'a.b.c'))
    print(json_parse("""{ "root": { "user": { "username": "rb" } } }""", 'root.user.username'))

    #Command line arguements.
    js_string =sys.argv[1]
    path=sys.argv[2]

    print(js_string,path)
    print(json_parse(js_string,path))



