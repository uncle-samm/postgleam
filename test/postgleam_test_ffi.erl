%% Test helper: run a function and report whether it panicked, the way a
%% web framework that rescues crashes would.
-module(postgleam_test_ffi).
-export([rescue/1]).

rescue(Fun) ->
    try Fun() of
        _ -> {ok, nil}
    catch
        _:_ -> {error, nil}
    end.
