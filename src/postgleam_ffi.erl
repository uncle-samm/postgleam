%% Erlang FFI for postgleam.
%%
%% Two things live here. The TLS upgrade: there is no Gleam TLS library for
%% the BEAM, and building `ssl:connect/3` options (including a verify_fun
%% closure) is clearer in Erlang. And `protect/2`: Gleam has no way to run
%% cleanup when a callback panics. Everything else is Gleam, or a direct
%% `@external` call into the Erlang standard library.
-module(postgleam_ffi).
-export([ssl_upgrade/7, ssl_send/2, ssl_recv/2, ssl_close/1, protect/2]).

%% Run Fun. If it raises, run Cleanup and raise the same exception again,
%% with its original stack trace.
protect(Fun, Cleanup) ->
    try Fun()
    catch Class:Reason:Stack ->
        _ = Cleanup(),
        erlang:raise(Class, Reason, Stack)
    end.

%% Upgrade a TCP socket to TLS.
%% Verify=true: verify_peer against CaFile (or the system store when empty),
%% with SNI and HTTPS-style host name matching (IP literals match the
%% certificate's IP SANs and send no SNI).
ssl_upgrade(TcpSock, Host, Timeout, Verify, CaFile, CertFile, KeyFile) ->
    %% The ssl application must be running; it is not started by default
    %% outside of a release. ensure_all_started is idempotent.
    case application:ensure_all_started(ssl) of
        {ok, _} -> ssl_connect(TcpSock, Host, Timeout, Verify, CaFile, CertFile, KeyFile);
        {error, Reason} -> {error, format_reason({ssl_not_started, Reason})}
    end.

ssl_connect(TcpSock, Host, Timeout, Verify, CaFile, CertFile, KeyFile) ->
    HostCharlist = binary_to_list(Host),
    IsIp = case inet:parse_address(HostCharlist) of
        {ok, _} -> true;
        _ -> false
    end,
    Sni = case IsIp of
        true -> [{server_name_indication, disable}];
        false -> [{server_name_indication, HostCharlist}]
    end,
    Verification = case Verify of
        true ->
            CaOpts = case CaFile of
                <<>> -> [{cacerts, public_key:cacerts_get()}];
                _ -> [{cacertfile, binary_to_list(CaFile)}]
            end,
            HostCheck = case IsIp of
                true -> [{verify_fun, ip_verify_fun(HostCharlist)}];
                false ->
                    [{customize_hostname_check,
                      [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]}]
            end,
            [{verify, verify_peer} | CaOpts ++ HostCheck];
        false ->
            [{verify, verify_none}]
    end,
    ClientCert = case CertFile of
        <<>> -> [];
        _ -> [{certfile, binary_to_list(CertFile)}, {keyfile, binary_to_list(KeyFile)}]
    end,
    case ssl:connect(TcpSock, Sni ++ Verification ++ ClientCert, Timeout) of
        {ok, SslSock} -> {ok, SslSock};
        {error, Reason} -> {error, format_reason(Reason)}
    end.

%% For IP literal hosts: run the default chain checks, then match the IP
%% against the certificate's subjectAltName iPAddress entries.
ip_verify_fun(HostCharlist) ->
    {ok, Ip} = inet:parse_address(HostCharlist),
    {fun(_Cert, {bad_cert, _} = Reason, _) -> {fail, Reason};
        (_Cert, {extension, _}, State) -> {unknown, State};
        (_Cert, valid, State) -> {valid, State};
        (Cert, valid_peer, State) ->
            case public_key:pkix_verify_hostname(Cert, [{ip, Ip}]) of
                true -> {valid, State};
                false -> {fail, {bad_cert, hostname_check_failed}}
            end
     end, []}.

ssl_send(SslSock, Data) ->
    case ssl:send(SslSock, Data) of
        ok -> {ok, nil};
        {error, Reason} -> {error, format_reason(Reason)}
    end.

ssl_recv(SslSock, Timeout) ->
    case ssl:recv(SslSock, 0, Timeout) of
        {ok, Data} -> {ok, Data};
        {error, timeout} -> {error, <<"timeout">>};
        {error, Reason} -> {error, format_reason(Reason)}
    end.

ssl_close(SslSock) ->
    _ = ssl:close(SslSock),
    nil.

format_reason(Reason) ->
    unicode:characters_to_binary(io_lib:format("~p", [Reason])).
