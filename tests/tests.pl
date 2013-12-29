:- begin_tests(docstore).

:- use_module(library(readutil)).
:- use_module(prolog/docstore).

test(insert_dict, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(user{name: john}),
    ds_all(user, List),
    length(List, 1).

test(insert_dict_id, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(user{name: john}, Id),
    ds_all(user, [User]),
    User.'$id' = Id.

test(find, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(user{name: john}),
    ds_insert(user{name: mary}),
    ds_all(user, All),
    length(All, 2),
    ds_find(user, name=john, List),
    length(List, 1).

test(find_subset_keys, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(user{name: john, age: 42}),
    ds_insert(user{name: mary, age: 24}),
    ds_find(user, name=john, [age], [John]),
    42 = John.age,
    \+ get_dict(name, John, _).

test(all_subset_keys, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(user{name: john, age: 42}),
    ds_insert(user{name: mary, age: 24}),
    ds_all(user, [age], [Item1, Item2]),
    \+ get_dict(name, Item1, _),
    \+ get_dict(name, Item2, _),
    _ = Item1.age,
    _ = Item2.age.

test(update, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(user{name: john, age: 42}, Id),
    ds_update(user{'$id': Id, age: 24}),
    ds_all(user, [John]),
    24 = John.age.

test(upsert, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_upsert(user{name: john, age: 42}, Id),
    ds_all(user, [John]),
    42 = John.age,
    New = John.put(age, 24),
    ds_upsert(New, NewId),
    Id = NewId,
    ds_all(user, [NewJohn]),
    24 = NewJohn.age.

test(get, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(user{name: john, age: 42}, Id),
    ds_get(Id, John),
    john = John.name,
    42 = John.age,
    (   ds_get('non-existing', _)
    ->  fail
    ;   true).

test(remove, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(user{name: john}, Id),
    ds_all(user, [_]),
    ds_remove(Id),
    ds_all(user, []).

test(remove_cond, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(user{name: john}),
    ds_insert(user{name: mary}),
    ds_all(user, [_, _]),
    ds_remove(user, name=john),
    ds_all(user, [Mary]),
    mary = Mary.name.

test(remove_col, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(user{name: john}),
    ds_insert(user{name: mary}),
    ds_all(user, [_, _]),
    ds_remove_col(user),
    ds_all(user, []).

:- dynamic(remove_check/1).

:- ds_hook(users, before_save, add_key).

add_key(In, Out):-
    Out = In.put(key, test).

:- ds_hook(multi, before_save, add_a).

:- ds_hook(multi, before_save, add_b).

add_a(In, Out):-
    Out = In.put(a, 1).

add_b(In, Out):-
    Out = In.put(b, 2).

:- ds_hook(users, before_remove, remove_test).

remove_test(Id):-
    assertz(remove_check(Id)).

test(save_hook, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(users, _{name: john}, Id),
    ds_get(Id, Doc),
    test = Doc.key.

test(save_hook_multi, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(multi, _{test: test}, Id),
    ds_get(Id, Doc),
    1 = Doc.a,
    2 = Doc.b.

test(remove_hook, [ setup((retractall(remove_check(_)), ds_open('test.db'))),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(users, _{name: john}, Id),
    ds_remove(Id),
    remove_check(Id).

same_content(File1, File2):-
    read_file_to_codes(File1, Codes1, []),
    read_file_to_codes(File2, Codes2, []),
    (   Codes1 = Codes2
    ->  true
    ;   throw(error(files_not_same_content(File1, File2)))).

test(snapshot, [ setup(ds_open('snapshot.test.db')),
        cleanup((ds_close, delete_file('snapshot.test.db'),
            delete_file('snapshot.db')))]):-
    ds_insert(users, _{name: john}, _),
    ds_snapshot('snapshot.db'),
    same_content('snapshot.test.db', 'snapshot.db').

:- end_tests(docstore).
