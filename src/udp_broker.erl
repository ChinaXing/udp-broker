-module(udp_broker).
-export([start/6, send/5]).
%% ------------------------------------------
% Send UDP Message parallel
% 
% can be used as perfermance benchmark client
%
% Created by ChinaXing <chen.yack@gmail.com>
%                        2015-01-16 16:08:04
%% ------------------------------------------

start(Host, Port, Parallel, Count, Delay, Message) ->
    io:format("parallel run ~B process to send udp packet to ~s:~B~n Count: ~B, Dealy: ~B~n",[Parallel,Host,Port,Count,Delay]),
    PidList = async_run(fun() -> send(Host, Port, Count, Delay, Message) end, Parallel),
    wait_proccesses(PidList),
    io:format("parallel process Done !~n",[]),
    ok.

%% --  a timer, execute <Fun> after <Time> million seconds -- %%
%% timer(Time,Fun) ->
%%     receive 
%% 	cancel ->
%% 	    void
%%     after Time ->
%% 	    Fun()
%%     end.


%% -- send <Message> to <Host:Port> repeat <Count> times with <Delay> interval -- %%
send(Host, Port, Count, Delay, MessageFun) ->
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_repeat(Socket, Host, Port, Count, MessageFun, Delay, 0).

send_repeat(Socket, _, _, COUNT, _, _, COUNT) ->
    gen_udp:close(Socket),
    ok;
send_repeat(Socket, Host, Port, Count, MessageFun, Delay, Index) ->
    Message = MessageFun(Index),
    ok = gen_udp:send(Socket, Host, Port, Message),
    io:format("~p send packet ~B~n", [self(), Index]),
    timer:sleep(Delay),
    send_repeat(Socket, Host, Port, Count, MessageFun, Delay, Index + 1).

%% --- remove first element occurred in list --- %%
list_remove_first([], _) -> [];
list_remove_first([I|L], E) when I /= E -> [I|list_remove_first(L,E)];
list_remove_first([_|L], _) -> L.

%% -- wait proecess list exits -- %%
wait_proccesses(PidList) ->
    io:format("PidList : ~w~n", [PidList]),
    case PidList of
	[] -> ok;
	_ ->
	    receive
		{'DOWN', _, process, Pid, _} ->
		    io:format("Pid :~p is Done.~n",[Pid]),
		    wait_proccesses(list_remove_first(PidList,Pid))
	    end
    end.

%% -- async parallel execute <Fun> by <Parallel> times -- %%
async_run(Fun, Parallel) -> async_run0(Fun, Parallel, 0, []).

async_run0(_ , Parallel, Index, PidList) when Index == Parallel -> PidList;
async_run0(Fun, Parallel, Index, PidList) ->
    {Pid, _} = spawn_monitor(Fun),
    io:format("spawn process :~p~n",[Pid]),
    async_run0(Fun, Parallel, Index + 1, [Pid|PidList]).
    
%% END %%
