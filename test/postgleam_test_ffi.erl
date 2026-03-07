-module(postgleam_test_ffi).
-export([string_starts_with/2, string_contains/2]).

string_starts_with(String, Prefix) ->
    PrefixLen = byte_size(Prefix),
    case String of
        <<Prefix:PrefixLen/binary, _/binary>> -> true;
        _ -> false
    end.

string_contains(String, Substr) ->
    case binary:match(String, Substr) of
        {_, _} -> true;
        nomatch -> false
    end.
