:- begin_tests(docstore_hooks).
:- use_module(prolog/docstore).

:- dynamic(remove_check/1).

:- ds_hook(users, before_save, add_key).

add_key(In, [ key(test)|In ]).

:- ds_hook(multi, before_save, add_a).

:- ds_hook(multi, before_save, add_b).

add_a(In, [ a(1)|In ]).
    
add_b(In, [ b(2)|In ]).

:- ds_hook(users, before_remove, remove_test).

remove_test(Id):-
    assertz(remove_check(Id)).

test(save_hook, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(users, [ name(john) ], Id),
    ds_get(Id, Doc),
    memberchk(key(test), Doc).
    
test(save_hook_multi, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(multi, [ test(test) ], Id),
    ds_get(Id, Doc),
    memberchk(a(1), Doc),
    memberchk(b(2), Doc).
    
test(remove_hook, [ setup((retractall(remove_check(_)), ds_open('test.db'))),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(users, [ name(john) ], Id),
    ds_remove(Id),
    remove_check(Id).

:- end_tests(docstore_hooks).
