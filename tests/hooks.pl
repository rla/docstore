:- begin_tests(docstore_hooks).
:- use_module(prolog/docstore).

:- dynamic(remove_check/1).

docstore:ds_before_save(users, In, [ key(test)|In ]):-
    \+ memberchk(key(_), In).
    
docstore:ds_before_save(multi, In, [ a(1)|In ]):-
    \+ memberchk(a(_), In).
    
docstore:ds_before_save(multi, In, [ b(2)|In ]):-
    \+ memberchk(b(_), In).    
    
docstore:ds_before_remove(users, Id):-
    \+ remove_check(_),
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
