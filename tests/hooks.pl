:- begin_tests(docstore_hooks).
:- use_module(prolog/docstore).

docstore:ds_before_save(users, In, [ key(test)|In ]):-
    \+ memberchk(key(_), In).
    
docstore:ds_before_save(multi, In, [ a(1)|In ]):-
    \+ memberchk(a(_), In).
    
docstore:ds_before_save(multi, In, [ b(2)|In ]):-
    \+ memberchk(b(_), In).

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

:- end_tests(docstore_hooks).
