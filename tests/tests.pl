:- begin_tests(docstore).

:- use_module(library(readutil)).
:- use_module(prolog/docstore).

:- debug(docstore).

% Do not allow to open database
% with failed transaction or
% broken write.

test(failed):-
    catch((ds_open('tests/failed.db'), fail),
        error(failed_transaction), true).

test(broken):-
    catch((ds_open('tests/broken.db'), fail),
        _, true),
    catch((ds_insert(test{abc: 1}), fail),
        error(docstore_not_open), true).

test(insert_dict, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
    ds_all(vehicle, [Vehicle]),
    ds_close,
    ds_open('test.db'),
    get_dict(year, Vehicle, 1926),
    get_dict(make, Vehicle, chrysler),
    get_dict(model, Vehicle, imperial).

test(insert_dict_id, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}, Id),
    ds_close,
    ds_open('test.db'),
    ds_all(vehicle, [Vehicle]),
    Id = Vehicle.'$id'.

test(find, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
    ds_insert(vehicle{year: 1953, make: chevrolet, model: corvette}),
    ds_close,
    ds_open('test.db'),
    ds_all(vehicle, All),
    length(All, 2),
    ds_find(vehicle, year=1926, List),
    length(List, 1).

test(find_subset_keys, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
    ds_insert(vehicle{year: 1953, make: chevrolet, model: corvette}),
    ds_close,
    ds_open('test.db'),
    ds_find(vehicle, year=1926, [model], [Imperial]),
    imperial = Imperial.model,
    \+ get_dict(make, Imperial, _).

test(find_empty_subset_keys, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
    ds_insert(vehicle{year: 1953, make: chevrolet, model: corvette}),
    ds_close,
    ds_open('test.db'),
    ds_find(vehicle, year=1926, [], [Imperial]),
    \+ get_dict(model, Imperial, _),
    \+ get_dict(year, Imperial, _),
    \+ get_dict(make, Imperial, _),
    get_dict('$id', Imperial, _).

test(all_subset_keys, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
    ds_insert(vehicle{year: 1953, make: chevrolet, model: corvette}),
    ds_close,
    ds_open('test.db'),
    ds_all(vehicle, [year], [Vehicle1, Vehicle2]),
    \+ get_dict(make, Vehicle1, _),
    \+ get_dict(make, Vehicle2, _),
    get_dict(year, Vehicle1, 1926),
    get_dict(year, Vehicle2, 1953).

test(all_empty_subset_keys, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
    ds_insert(vehicle{year: 1953, make: chevrolet, model: corvette}),
    ds_close,
    ds_open('test.db'),
    ds_all(vehicle, [], [Vehicle1, Vehicle2]),
    \+ get_dict(make, Vehicle1, _),
    \+ get_dict(make, Vehicle2, _),
    \+ get_dict(year, Vehicle1, 1926),
    \+ get_dict(year, Vehicle2, 1953),
    get_dict('$id', Vehicle1, _),
    get_dict('$id', Vehicle2, _).

test(update, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}, Id),
    ds_update(vehicle{'$id': Id, year: 1962}),
    ds_close,
    ds_open('test.db'),
    ds_all(vehicle, [Vehicle]),
    1962 = Vehicle.year.

test(upsert, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}, Id),
    ds_all(vehicle, [Vehicle]),
    1926 = Vehicle.year,
    New = Vehicle.put(year, 1962),
    ds_upsert(New, NewId),
    ds_close,
    ds_open('test.db'),
    Id = NewId,
    ds_all(vehicle, [Updated]),
    1962 = Updated.year.

test(get, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}, Id),
    ds_close,
    ds_open('test.db'),
    ds_get(Id, Vehicle),
    imperial = Vehicle.model,
    1926 = Vehicle.year,
    (   ds_get('non-existing', _)
    ->  fail
    ;   true).

test(get_single_key, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}, Id),
    ds_close,
    ds_open('test.db'),
    ds_get(Id, model, Vehicle),
    imperial = Vehicle.model,
    \+ get_dict(make, Vehicle, _),
    \+ get_dict(year, Vehicle, _).

test(get_subset_keys, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}, Id),
    ds_close,
    ds_open('test.db'),
    ds_get(Id, [model, year], Vehicle),
    imperial = Vehicle.model,
    Id = Vehicle.'$id',
    \+ get_dict(make, Vehicle, _).

test(get_empty_subset_keys, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}, Id),
    ds_close,
    ds_open('test.db'),
    ds_get(Id, [], Vehicle),
    Id = Vehicle.'$id',
    \+ get_dict(make, Vehicle, _),
    \+ get_dict(year, Vehicle, _),
    \+ get_dict(model, Vehicle, _).

test(remove, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}, Id),
    ds_all(vehicle, [_]),
    ds_col_remove(vehicle, Id),
    ds_close,
    ds_open('test.db'),
    ds_all(vehicle, []).

test(remove_wrong_col, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}, Id),
    ds_all(vehicle, [_]),
    catch((ds_col_remove(car, Id), fail), _, true),
    ds_close,
    ds_open('test.db'),
    ds_all(vehicle, [_]).

test(remove_cond, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
    ds_insert(vehicle{year: 1953, make: chevrolet, model: corvette}),
    ds_all(vehicle, [_, _]),
    ds_col_remove_cond(vehicle, make=chevrolet),
    ds_close,
    ds_open('test.db'),
    ds_all(vehicle, [Vehicle]),
    1926 = Vehicle.year.

test(remove_col, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
    ds_insert(vehicle{year: 1953, make: chevrolet, model: corvette}),
    ds_all(vehicle, [_, _]),
    ds_remove_col(vehicle),
    ds_close,
    ds_open('test.db'),
    ds_all(vehicle, []).

test(tuples, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
    ds_insert(vehicle{year: 1953, make: chevrolet, model: corvette}),
    ds_close,
    ds_open('test.db'),
    findall([Year, Make], ds_tuples(vehicle, [year, make], [Year, Make]), Tuples),
    memberchk([1926, chrysler], Tuples),
    memberchk([1953, chevrolet], Tuples).

test(col_add_key, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
    ds_insert(vehicle{year: 1953, make: chevrolet, model: corvette}),
    ds_col_add_key(vehicle, points, 0),
    ds_all(vehicle, [Vehicle1, Vehicle2]),
    1926 = Vehicle1.year,
    0 = Vehicle1.points,
    1953 = Vehicle2.year,
    0 = Vehicle2.points.

test(col_remove_key, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
    ds_insert(vehicle{year: 1953, make: chevrolet, model: corvette}),
    ds_col_remove_key(vehicle, year),
    ds_close,
    ds_open('test.db'),
    ds_all(vehicle, [Vehicle1, Vehicle2]),
    (   get_dict(year, Vehicle1, _)
    ->  fail
    ;   true),
    (   get_dict(year, Vehicle2, _)
    ->  fail
    ;   true).

test(remove_key, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
    ds_insert(vehicle{year: 1953, make: chevrolet, model: corvette}, Id),
    ds_remove_key(Id, year),
    ds_close,
    ds_open('test.db'),
    ds_all(vehicle, [Vehicle1, Vehicle2]),
    get_dict(year, Vehicle1, 1926),
    (   get_dict(year, Vehicle2, _)
    ->  fail
    ;   true).

test(col_rename, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
    ds_insert(vehicle{year: 1953, make: chevrolet, model: corvette}),
    ds_col_rename(vehicle, car),
    ds_close,
    ds_open('test.db'),
    ds_all(vehicle, []),
    ds_all(car, [_, _]).

test(rename_key, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
    ds_insert(vehicle{year: 1953, make: chevrolet, model: corvette}),
    ds_col_rename_key(vehicle, year, y),
    ds_close,
    ds_open('test.db'),
    ds_all(vehicle, [Vehicle1, Vehicle2]),
    _ = Vehicle1.y,
    _ = Vehicle2.y,
    (   get_dict(year, Vehicle1, _)
    ->  fail
    ;   true),
    (   get_dict(year, Vehicle2, _)
    ->  fail
    ;   true).

% Tests simple save hook.

:- ds_hook(user, before_save, add_key).

add_key(In, Out):-
    Out = In.put(key, test).

test(save_hook, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(user{name: john}, Id),
    ds_get(Id, Doc),
    test = Doc.key.

% Tests multiple save hooks on same collection.

:- ds_hook(multi, before_save, add_a).

:- ds_hook(multi, before_save, add_b).

add_a(In, Out):-
    Out = In.put(a, 1).

add_b(In, Out):-
    Out = In.put(b, 2).

test(save_hook_multi, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(multi{test: test}, Id),
    ds_get(Id, Doc),
    1 = Doc.a,
    2 = Doc.b.

% Tests simple before_remove hook.

:- ds_hook(remove_hook_test, before_remove, remove_test).

:- dynamic(remove_check/1).

remove_test(Id):-
    assertz(remove_check(Id)).

test(remove_hook, [ setup((retractall(remove_check(_)), ds_open('test.db'))),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(remove_hook_test{name: abc}, Id),
    ds_col_remove(remove_hook_test, Id),
    remove_check(Id).

% Tests remove hook with nested action.

:- ds_hook(remove_hook_nest_test, before_remove, remove_nest_test).

remove_nest_test(_):-
    ds_remove_col(remove_nest).

:- ds_hook(remove_nest, before_remove, remove_nest_nest_test).

:- dynamic(remove_nest_check/1).

remove_nest_nest_test(Id):-
    assertz(remove_nest_check(Id)).

test(remove_hook_nest, [ setup((retractall(remove_nest_check(_)), ds_open('test.db'))),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(remove_nest{test: 123}, Id1),
    ds_insert(remove_hook_nest_test{name: abc}, Id2),
    ds_col_remove(remove_hook_nest_test, Id2),
    ds_close,
    ds_open('test.db'),
    ds_all(nest, []),
    remove_nest_check(Id1).

% Tests failing hook to discard transaction.

:- ds_hook(test_fail_hook, before_save, test_fail_hook).

test_fail_hook(_, _):-
    fail.

test(failing_hook, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    catch(ds_insert(test_fail_hook{name: abc}), _, true),
    ds_close,
    ds_open('test.db'),
    ds_all(test_fail_hook, []).

% Tests error-throwing hook to discard transaction.

:- ds_hook(test_error_hook, before_save, test_error_hook).

test_error_hook(_, _):-
    throw(error(test_error)).

test(failing_hook, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    catch(ds_insert(test_error_hook{name: abc}), _, true),
    ds_close,
    ds_open('test.db'),
    ds_all(test_fail_hook, []).

test(snapshot, [ setup(ds_open('snapshot.test.db')),
        cleanup((ds_close, delete_file('snapshot.test.db'),
            delete_file('snapshot.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
    ds_insert(vehicle{year: 1953, make: chevrolet, model: corvette}),
    ds_snapshot('snapshot.db'),
    ds_close,
    ds_open('snapshot.db'),
    ds_all(vehicle, [Vehicle1, Vehicle2]),
    1926 = Vehicle1.year,
    chrysler = Vehicle1.make,
    imperial = Vehicle1.model,
    1953 = Vehicle2.year,
    chevrolet = Vehicle2.make,
    corvette = Vehicle2.model.

test(snapshot_self, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
    ds_insert(vehicle{year: 1953, make: chevrolet, model: corvette}),
    ds_snapshot,
    ds_close,
    ds_open('test.db'),
    ds_all(vehicle, [Vehicle1, Vehicle2]),
    1926 = Vehicle1.year,
    chrysler = Vehicle1.make,
    imperial = Vehicle1.model,
    1953 = Vehicle2.year,
    chevrolet = Vehicle2.make,
    corvette = Vehicle2.model.

fail_pred:-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
    fail.

test(transaction_fail, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    ignore(ds_transactional(fail_pred)),
    ds_all(vehicle, []).

throw_pred:-
    ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
    throw(error(error_occurred)).

test(transaction_throw, [ setup(ds_open('test.db')),
        cleanup((ds_close, delete_file('test.db')))]):-
    catch(ds_transactional(throw_pred), _, true),
    ds_all(vehicle, []).

:- end_tests(docstore).
