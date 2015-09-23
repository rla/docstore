:- module(docstore, [
    ds_open/1,           % +File
    ds_close/0,
    ds_snapshot/1,       % +File
    ds_snapshot/0,
    ds_hook/3,           % +Col, +Action, :Goal
    ds_insert/1,         % +Dict
    ds_insert/2,         % +Dict, -Id
    ds_insert/3,         % +Col, +Dict, -Id
    ds_update/1,         % +Dict
    ds_update/2,         % +Id, +Dict
    ds_upsert/1,         % +Dict
    ds_upsert/2,         % +Dict, -Id
    ds_upsert/3,         % +Col, +Dict, -Id
    ds_move/3,           % +Col, +Id, +Col
    ds_col_get/3,        % +Col, +Id, -Dict
    ds_col_get/4,        % +Col, +Id, +Keys, -Dict
    ds_all/2,            % +Col, -List
    ds_all/3,            % +Col, +Keys, -List
    ds_all_ids/2,        % +Col, -List
    ds_find/3,           % +Col, +Cond, -List
    ds_find/4,           % +Col, +Cond, +Keys, -List
    ds_collection/2,     % ?Id, ?Col
    ds_col_remove/2,     % +Col, +Id
    ds_col_remove_cond/2,% +Col, Cond
    ds_remove_col/1,     % +Col
    ds_remove_key/2,     % +Id, +Key
    ds_tuples/3,         % +Col, +Keys, -Values
    ds_col_add_key/3,    % +Col, +Key, +Default
    ds_col_remove_key/2, % +Col, +Key
    ds_col_rename/2,     % +Col, +ColNew
    ds_col_rename_key/3, % +Col, +Key, +KeyNew
    ds_transactional/1,  % +Goal
    ds_uuid/1,           % -Uuid
    ds_id/2,             % +Doc, -Id
    ds_set_id/3          % +In, +Id, -Out
]).

/** <module> Document-oriented database

Generic thread-safe in-memory transactional
store for dict terms.
*/

:- use_module(library(apply)).
:- use_module(library(random)).
:- use_module(library(error)).
:- use_module(library(debug)).

:- dynamic(col/2).
:- dynamic(eav/3).
:- dynamic(file/2).

:- dynamic(hook/3).

%! ds_open(+File) is det.
%
% Opens the database file.
% Throws error(docstore_is_open) when
% the database is already open.

ds_open(File):-
    safely(ds_open_unsafe(File)).

ds_open_unsafe(File):-
    debug(docstore, 'opening database ~p', [File]),
    (   file(_, _)
    ->  throw(error(docstore_is_open))
    ;   must_be(atom, File),
        catch(loadall(File), Error, clean),
        (   nonvar(Error)
        ->  throw(Error)
        ;   true),
        open(File, append, Stream, [
            encoding('utf8'), lock(write)
        ]),
        assertz(file(File, Stream))).

% Cleans current database state.
% This is run on ds_close and during
% loading when loading fails.

clean:-
    debug(docstore, 'cleaning database', []),
    retractall(col(_, _)),
    retractall(eav(_, _, _)),
    retractall(file(_, _)).

%! ds_close is det.
%
% Closes the database. Removes in-memory data.
% Runs close hooks. Hooks are ran before
% the file is closed and data is purged from memory.
% Throws error(database_is_not_open) when
% the database file is not open.

ds_close:-
    safely(close_unsafe).

close_unsafe:-
    (   file(_, Stream)
    ->  true
    ;   throw(error(database_is_not_open))),
    close(Stream),
    clean,
    debug(docstore, 'database is closed', []).

:- dynamic(load_tx_begin).

% Loads database contents from
% the given file if it exists.

loadall(File):-
    exists_file(File), !,
    debug(docstore, 'loading from file ~p', [File]),
    retractall(load_tx_begin),
    setup_call_cleanup(
        open(File, read, Stream, [encoding('utf8')]),
        load(Stream),
        close(Stream)).

loadall(_).

% Loads database actions (assertz and retractall)
% from the stream until the end of the stream is
% reached.

load(Stream):-
    read_term(Stream, Term, [
        dotlists(true)
    ]),
    (Term = end_of_file
    ->  (   load_tx_begin
        ->  retractall(col(_, _)),
            retractall(eav(_, _, _)),
            throw(error(failed_transaction))
        ;   true)
    ;   load_term(Term),
        load(Stream)).

load_term(begin):-
    (   load_tx_begin
    ->  throw(error(double_begin))
    ;   assertz(load_tx_begin)).

load_term(end):-
    (   load_tx_begin
    ->  retractall(load_tx_begin)
    ;   throw(error(end_without_begin))).

load_term(assertz(Term)):-
    (   load_tx_begin
    ->  assertz(Term)
    ;   throw(error(no_tx_begin))).

load_term(retractall(Term)):-
    (   load_tx_begin
    ->  retractall(Term)
    ;   throw(error(no_tx_begin))).

load_term(Term):-
    throw(error(unknown_term(Term))).

%! ds_snapshot(+File) is det.
%
% Writes the current database
% snapshot into the file.

ds_snapshot(File):-
    safely(ds_snapshot_unsafe(File)).

ds_snapshot_unsafe(File):-
    setup_call_cleanup(
        open(File, write, Stream, [encoding('utf8')]),
        snapshot_dump(Stream),
        close(Stream)).

snapshot_dump(Stream):-
    write_goal(Stream, begin),
    ((  col(Col, Id),
        write_goal(Stream, assertz(col(Col, Id))),
        fail
    ) ; true),
    ((  eav(Id, Name, Value),
        write_goal(Stream, assertz(eav(Id, Name, Value))),
        fail
    ) ; true),
    write_goal(Stream, end).

%! ds_snapshot is det.
%
% Writes the snapshot of
% current database contents into
% its file. Implemented by running
% ds_snapshot/1 into a file (with a random
% name) and renaming the file using
% rename_file/2.

ds_snapshot:-
    safely(ds_snapshot_unsafe).

ds_snapshot_unsafe:-
    file(Current, CurStream),
    file_directory_name(Current, Dir),
    ds_uuid(Name),
    atomic_list_concat([Dir, Name], /, New),
    ds_snapshot(New),
    retractall(file(_, _)),
    close(CurStream),
    rename_file(New, Current),
    open(Current, append, NewStream, [
        encoding('utf8'), lock(write)
    ]),
    assertz(file(Current, NewStream)).

:- meta_predicate(ds_hook(+, +, :)).

%! ds_hook(+Col, +Action, :Goal) is det.
%
% Adds new save/remove hook.
% Action is one of: `before_save`, `before_remove`.
% before_save hooks are executed before insert
% and update. before_remove hooks are executed
% before the document removal. During update only
% the updated fields are passed to the before_save hooks.
% Hooks are run in the current transaction. Hooks that
% fail or throw exception will end the transaction
% and discard changes.

ds_hook(Col, Action, Goal):-
    (   hook(Col, Action, Goal)
    ->  true
    ;   assertz(hook(Col, Action, Goal))).

%! ds_insert(Doc) is det.
%
% Same as ds_insert/2 but the generated
% ID is ignored.

ds_insert(Doc):-
    ds_insert(Doc, _).

%! ds_insert(+Doc, -Id) is det.
%
% Same as ds_insert/3 but collection
% name is taken from dict tag.

ds_insert(Doc, Id):-
    must_be(dict, Doc),
    is_dict(Doc, Col),
    ds_insert(Col, Doc, Id).

%! ds_insert(+Col, +Doc, -Id) is det.
%
% Inserts new document into the given collection.
% Gives back the generated ID. Document must be a dict.
% All values in the dict must be ground.
% Throws error(doc_has_id) when the document has the
% $id key. Runs before_save hooks.

ds_insert(Col, Doc, Id):-
    must_be(atom, Col),
    must_be(nonvar, Doc),
    (   get_dict('$id', Doc, _)
    ->  throw(error(doc_has_id))
    ;   true),
    ds_transactional(insert_unsafe(Col, Doc, Id)).

insert_unsafe(Col, Doc, Id):-
    run_before_save_hooks(Col, Doc, Processed),
    ds_uuid(Id),
    dict_pairs(Processed, _, Pairs),
    must_be(ground, Pairs),
    run(assertz(col(Col, Id))),
    maplist(assert_eav(Id), Pairs).

assert_eav(Id, Name-Value):-
    run(assertz(eav(Id, Name, Value))).

% Executes save hooks.

run_before_save_hooks(Col, Doc, Out):-
    findall(Goal, hook(Col, before_save, Goal), Goals),
    run_before_save_goals(Goals, Doc, Out).

run_before_save_goals([Goal|Goals], Doc, Out):-
    debug(docstore, 'running save hook ~p', [Goal]),
    (   call(Goal, Doc, Tmp)
    ->  run_before_save_goals(Goals, Tmp, Out)
    ;   throw(error(before_save_hook_failed(Goal)))).

run_before_save_goals([], Doc, Doc).

%! ds_update(+Doc) is det.
%
% Updates the given document. Only
% changed properties are updated.
% Throws error if Doc contains no `$id`.
% Ignores updates to `$id`. Runs
% `before_save` hooks. Throws error(no_such_doc(Id))
% when no document with the given Id exists.

ds_update(Doc):-
    must_be(dict, Doc),
    ds_id(Doc, Id),
    (   col(Col, Id)
    ->  true
    ;   throw(error(no_such_doc(Id)))),
    ds_transactional(update_unsafe(Col, Id, Doc)).

%! ds_update(+Id, +Doc) is det.
%
% Updates the given document. The document
% id inside the document is ignored.

ds_update(Id, Doc):-
    must_be(dict, Doc),
    must_be(atom, Id),
    (   col(Col, Id)
    ->  true
    ;   throw(error(no_such_doc(Id)))),
    ds_transactional(update_unsafe(Col, Id, Doc)).

update_unsafe(Col, Id, Doc):-
    run_before_save_hooks(Col, Doc, Processed),
    dict_pairs(Processed, _, Pairs),
    must_be(ground, Pairs),
    update_props(Id, Pairs).

update_props(_, []).

update_props(Id, [Name-Value|Props]):-
    update_prop(Id, Name, Value),
    update_props(Id, Props).

update_prop(_, '$id', _):- !.

update_prop(Id, Name, Value):-
    eav(Id, Name, Old), !,
    (   Value = Old
    ->  true
    ;   prop_update_unsafe(Id, Name, Value)).

update_prop(Id, Name, Value):-
    run(assertz(eav(Id, Name, Value))).

prop_update_unsafe(Id, Name, Value):-
    run(retractall(eav(Id, Name, _))),
    run(assertz(eav(Id, Name, Value))).

%! ds_upsert(+Doc) is det.
%
% Same as ds_upsert/2 but ignores
% the generated id.

ds_upsert(Doc):-
    ds_upsert(Doc, _).

%! ds_upsert(+Doc, -Id) is det.
%
% Same as ds_upsert/3 but uses dict tag
% as collection name when inserting.

ds_upsert(Doc, Id):-
    must_be(dict, Doc),
    (   get_dict('$id', Doc, Id)
    ->  ds_update(Doc)
    ;   ds_insert(Doc, Id)).

%! ds_upsert(+Col, +Doc, -Id) is det.
%
% Inserts or updates the given document.

ds_upsert(Col, Doc, Id):-
    must_be(atom, Col),
    must_be(dict, Doc),
    (   get_dict('$id', Doc, Id)
    ->  ds_update(Doc)
    ;   ds_insert(Col, Doc, Id)).

%! ds_move(+Col, +Id, +NewCol) is det.
%
% Moves the given document into
% the new collection. Throws error
% when the document does not exist.

ds_move(Col, Id, NewCol):-
    must_be(atom, Id),
    must_be(atom, Col),
    must_be(atom, NewCol),
    (   col(Col, Id)
    ->  ds_transactional((
            run(retractall(col(_, Id))),
            run(assertz(col(NewCol, Id)))
        ))
    ;   throw(error(no_such_doc_in(Id, Col)))).

%! ds_col_get(+Col, +Id, -Doc) is semidet.
%
% Retrieves entry with the given id.
% Fails when the document with the given id
% does not exist or is not in the given collection.

ds_col_get(Col, Id, Doc):-
    must_be(atom, Col),
    must_be(atom, Id),
    col(Col, Id),
    doc(Id, Doc).

%! ds_col_get(+Col, +Id, +Keys, -Doc) is semidet.
%
% Retrieves entry with the given id.
% Retrieves subset of properties. Fails
% when the document with the given id does
% not exist or is not in the given collection.

ds_col_get(Col, Id, Keys, Doc):-
    must_be(atom, Col),
    must_be(atom, Id),
    col(Col, Id),
    doc(Id, Keys, Doc).

%! ds_all(+Col, -List) is det.
%
% Finds list of all documents in the given
% collection.

ds_all(Col, List):-
    must_be(atom, Col),
    findall(Doc, col_doc(Col, Doc), List).

%! ds_all(+Col, +Keys, -List) is det.
%
% Finds list of all documents in the given
% collection. Retrieves subset of keys. Subset
% will always contain '$id'.

ds_all(Col, Keys, List):-
    must_be(atom, Col),
    findall(Doc, col_doc(Col, Keys, Doc), List).

col_doc(Col, Doc):-
    col(Col, Id),
    doc(Id, Doc).

col_doc(Col, Keys, Doc):-
    col(Col, Id),
    doc(Id, Keys, Doc).

%! ds_all_ids(+Col, -List) is det.
%
% Retrieves the list of all document
% IDs in the collection.

ds_all_ids(Col, List):-
    must_be(atom, Col),
    findall(Id, col(Col, Id), List).

%! ds_find(+Col, +Cond, -List) is semidet.
%
% Finds collection entries that
% satisfy condition(s). Cond is one of:
% `Key = Value`, `Key \= Value`, `Key > Value`,
% `Key < Value`, `Key >= Value`, `Key =< Value`,
% `member(Value, Key)`, `(Cond1, Cond2)`, `(Cond1 ; Cond2)`.

ds_find(Col, Cond, List):-
    must_be(atom, Col),
    must_be(ground, Cond),
    findall(Doc, cond_doc(Col, Cond, Doc), List).

%! ds_find(+Col, +Cond, +Keys, -List) is semidet.
%
% Same as ds_find/3 but retrieves subset of keys.

ds_find(Col, Cond, Keys, List):-
    must_be(atom, Col),
    must_be(list(atom), Keys),
    must_be(ground, Cond),
    findall(Doc, cond_doc(Col, Cond, Keys, Doc), List).

cond_doc(Col, Cond, Dict):-
    col(Col, Id),
    cond(Cond, Id),
    doc(Id, Dict).

cond_doc(Col, Cond, Keys, Dict):-
    col(Col, Id),
    cond(Cond, Id),
    doc(Id, Keys, Dict).

% Succeeds when condition is satisfied
% on the given entity.

cond(Name = Value, Id):- !,
    eav(Id, Name, Value).

cond(Name \= Comp, Id):- !,
    eav(Id, Name, Value),
    Value \= Comp.

cond(Name > Comp, Id):- !,
    eav(Id, Name, Value),
    Value > Comp.

cond(Name < Comp, Id):- !,
    eav(Id, Name, Value),
    Value < Comp.

cond(Name >= Comp, Id):- !,
    eav(Id, Name, Value),
    Value >= Comp.

cond(Name =< Comp, Id):- !,
    eav(Id, Name, Value),
    Value =< Comp.

cond(member(Item, Name), Id):- !,
    eav(Id, Name, Value),
    memberchk(Item, Value).

cond(','(Left, Right), Id):- !,
    cond(Left, Id),
    cond(Right, Id).

cond(';'(Left, _), Id):- !,
    cond(Left, Id).

cond(';'(_, Right), Id):- !,
    cond(Right, Id).

cond(Cond, _):-
    throw(error(invalid_condition(Cond))).

% Finds document by ID.

doc(Id, Dict):-
    col(Col, Id),
    doc_kv_pairs(Id, Pairs),
    dict_pairs(Dict, Col, ['$id'-Id|Pairs]).

% Finds document by ID. Gives
% back subset of its properties.

doc(Id, Keys, Dict):-
    col(Col, Id),
    doc_kv_pairs(Id, Keys, Pairs),
    dict_pairs(Dict, Col, ['$id'-Id|Pairs]).

% Finds document properties
% with values.

doc_kv_pairs(Id, Pairs):-
    findall(Pair, doc_kv_pair(Id, Pair), Pairs).

% Finds document properties
% with values. Subset of properties.

doc_kv_pairs(Id, Key, Doc):-
    atom(Key), !,
    doc_kv_pairs(Id, [Key], Doc).

doc_kv_pairs(Id, Keys, Pairs):-
    (   key_list(Keys)
    ->  true
    ;   throw(error(invalid_key_set(Keys)))),
    findall(Pair, (
        member(Key, Keys),
        doc_kv_pair(Id, Key, Pair)
    ), Pairs).

key_list([]).

key_list([_|_]).

doc_kv_pair(Id, Name-Value):-
    eav(Id, Name, Value).

doc_kv_pair(Id, Name, Name-Value):-
    eav(Id, Name, Value).

%! ds_collection(?Id, ?Col) is semidet.
%
% Finds which collection the document
% belongs to.

ds_collection(Id, Col):-
    col(Col, Id).

%! ds_col_remove(+Col, +Id) is det.
%
% Removes the given document.
% Does nothing when the document
% does not exist. Runs `before_remove` hooks.

ds_col_remove(Col, Id):-
    must_be(atom, Id),
    must_be(atom, Col),
    (   col(Actual, Id)
    ->  (   Actual = Col
        ->  debug(docstore, 'removing document ~p', [Id]),
            ds_transactional(remove_unsafe(Id))
        ;   throw(error(document_not_in(Col))))
    ;   true).

remove_unsafe(Id):-
    run_before_remove_hooks(Id),
    run(retractall(eav(Id, _, _))),
    run(retractall(col(_, Id))).

run_before_remove_hooks(Id):-
    col(Col, Id), !,
    run_before_remove_hooks(Col, Id).

run_before_remove_hooks(Col, Id):-
    findall(Goal, hook(Col, before_remove, Goal), Goals),
    run_before_remove_goals(Goals, Id).

run_before_remove_goals([Goal|Goals], Id):-
    debug(docstore, 'running remove hook ~p', [Goal]),
    (   call(Goal, Id)
    ->  true
    ;   throw(error(before_remove_hook_fail(Goal)))),
    run_before_remove_goals(Goals, Id).

run_before_remove_goals([], _).

%! ds_col_remove_cond(+Col, +Cond) is det.
%
% Removes all documents from
% the collection that match the
% condition. Runs `before_remove` hooks.
% Cond expressions are same as in ds_find/3.

ds_col_remove_cond(Col, Cond):-
    must_be(atom, Col),
    findall(Id, (col(Col, Id), cond(Cond, Id)), Ids),
    ds_transactional(maplist(remove_unsafe, Ids)).

%! ds_remove_col(Col) is det.
%
% Removes all documents from
% the given collection. Is equivalent
% of running ds_remove/1 for each document
% in the collection. Runs `before_remove` hooks.

ds_remove_col(Col):-
    must_be(atom, Col),
    debug(docstrore, 'removing collection ~p', [Col]),
    ds_all_ids(Col, Ids),
    ds_transactional(maplist(remove_unsafe, Ids)).

%! ds_tuples(+Col, +Keys, -Values) is nondet.
%
% Provides backtrackable predicate-like view
% of documents. It does not support built-in
% indexing and therefore can be slow for purposes
% where some values are restricted.

ds_tuples(Col, Keys, Values):-
    must_be(atom, Col),
    must_be(list(atom), Keys),
    col(Col, Id),
    maplist(eav(Id), Keys, Values).

%! ds_remove_key(+Id, +Key) is det.
%
% Removes key from the given document.
% Does nothing when the document or entry
% does not exist.

ds_remove_key(Id, Key):-
    must_be(atom, Id),
    must_be(atom, Key),
    ds_transactional(ds_remove_key_unsafe(Id, Key)).

ds_remove_key_unsafe(Id, Key):-
    run(retractall(eav(Id, Key, _))).

%! ds_col_add_key(+Col, +Key, +Default) is det.
%
% Adds each document new key with the
% default value. Runs `before_save` hooks.

ds_col_add_key(Col, Key, Default):-
    must_be(atom, Col),
    must_be(atom, Key),
    must_be(ground, Default),
    ds_transactional(ds_col_add_key_unsafe(Col, Key, Default)).

ds_col_add_key_unsafe(Col, Key, Default):-
    ds_all_ids(Col, Ids),
    maplist(ds_add_key_unsafe(Col, Key, Default), Ids).

ds_add_key_unsafe(Col, Key, Default, Id):-
    dict_create(Dict, Col, ['$id'-Id, Key-Default]),
    update_unsafe(Col, Id, Dict).

%! ds_col_remove_key(+Col, +Key) is det.
%
% Removes given key from the document
% collection. Throws error(cannot_remove_id)
% when key is `$id`. `save_before` hooks are
% not executed.

ds_col_remove_key(Col, Key):-
    must_be(atom, Col),
    must_be(atom, Key),
    (   Key = '$id'
    ->  throw(error(cannot_remove_id))
    ;   true),
    ds_transactional(ds_col_remove_key_unsafe(Col, Key)).

ds_col_remove_key_unsafe(Col, Key):-
    ds_all_ids(Col, Ids),
    maplist(ds_col_remove_key_id(Key), Ids).

ds_col_remove_key_id(Key, Id):-
    run(retractall(eav(Id, Key, _))).

%! ds_col_rename(+Col, +ColNew) is det.
%
% Rename collection. Relatively expensive
% operation in terms of journal space. Needs
% entry per document in the collection.

ds_col_rename(Col, ColNew):-
    must_be(atom, Col),
    must_be(atom, ColNew),
    ds_transactional(ds_col_rename_unsafe(Col, ColNew)).

ds_col_rename_unsafe(Col, ColNew):-
    ds_all_ids(Col, Ids),
    run(retractall(col(Col, _))),
    maplist(new_col_id(ColNew), Ids).

new_col_id(Col, Id):-
    run(assertz(col(Col, Id))).

%! ds_col_rename_key(+Col, +Key, +KeyNew) is det.
%
% Renames a key in collection. Relatively expensive
% operation in terms of journal space. Needs
% 2 entries per document in the collection. Does
% not run hooks.

ds_col_rename_key(Col, Key, KeyNew):-
    must_be(atom, Col),
    must_be(atom, Key),
    must_be(atom, KeyNew),
    ds_transactional(ds_col_rename_key_unsafe(Col, Key, KeyNew)).

ds_col_rename_key_unsafe(Col, Key, KeyNew):-
    ds_all_ids(Col, Ids),
    maplist(rename_col_key(Key, KeyNew), Ids).

rename_col_key(Key, KeyNew, Id):-
    (   eav(Id, Key, Value)
    ->  run(retractall(eav(Id, Key, _))),
        run(assertz(eav(Id, KeyNew, Value)))
    ;   true).

:- meta_predicate(ds_transactional(0)).

%! ds_transactional(:Goal) is det.
%
% Runs given goal that modifies the database
% contents in a transactional mode. When the goal
% throws exception or fails, no changes by it
% are persisted.

ds_transactional(Goal):-
    safely(with_tx_unsafe(Goal)).

:- meta_predicate(with_tx_unsafe(0)).

% Starts transaction. Catches exception
% when exception happens. Rethrows the
% exception.

with_tx_unsafe(Goal):-
    begin,
    (   catch(Goal, Error, discard)
    ->  (   nonvar(Error)
        ->  debug(docstore, 'transactional run ended with exception', []),
            throw(Error) % rethrow
        ;   commit) % commit
    ;   discard, % discard on fail
        debug(docstore, 'transactional run failed', [])).

:- meta_predicate(safely(0)).

% Runs the Goal with global mutex.
% All modifications to the database
% are run using it.

safely(Goal):-
    with_mutex(db_store, Goal).

:- dynamic(log/1).
:- dynamic(tx/1).

:- meta_predicate(run(0)).

% Helper to run and log the goal.
% Throws error(database_not_open) when
% the database is not open.

run(Goal):-
    (   tx(_)
    ->  (   file(_, _)
        ->  assertz(log(Goal))
        ;   throw(error(docstore_not_open)))
    ;   throw(error(transaction_not_active))).

% Starts transaction. tx/1 is used for
% detecting transaction nesting. During
% transaction nesting, only the last commit
% has effect.

begin:-
    (   tx(N)
    ->  retractall(tx(_)),
        N1 is N + 1,
        debug(docstore, 'beginning fallthrough transaction (~p)', [N1]),
        assertz(tx(N1))
    ;   debug(docstore, 'beginning transaction (0)', []),
        assertz(tx(0))).

% Ends transaction. Has effect only
% when it ends the outer transaction.

commit:-
    (   tx(0)
    ->  debug(docstore, 'committing changes (0)', []),
        file(_, Stream),
        write_goal(Stream, begin),
        ((  log(Goal),
            Goal = _:Local,
            write_goal(Stream, Local),
            once(Goal),
            fail
        ) ; true),
        write_goal(Stream, end),
        flush_output(Stream),
        retractall(log(_)),
        retractall(tx(_))
    ;   tx(N),
        debug(docstore, 'fallthrough commit (~p)', [N]),
        retractall(tx(_)),
        N1 is N - 1,
        assertz(tx(N1))).

write_goal(Stream, Goal):-
    write_term(Stream, Goal, [
        ignore_ops,
        quoted,
        dotlists(true),
        nl(true),
        fullstop(true)
    ]).

% Ends transaction by empting the
% goal log. Has effect only when
% it ends the outer transaction.

discard:-
    (   tx(0)
    ->  debug(docstore, 'discarding changes', []),
        retractall(log(_)),
        retractall(tx(_))
    ;   tx(N),
        debug(docstore, 'fallthrough discard (~p)', [N]),
        retractall(tx(_)),
        N1 is N - 1,
        assertz(tx(N1))).

%! ds_id(+Doc, -Id) is det.
%
% Extracts document id from the
% given document. Equivalent to Doc.'$id'.

ds_id(Doc, Id):-
    (   get_dict('$id', Doc, Actual)
    ->  Id = Actual
    ;   throw(error(doc_has_no_id(Doc)))).

%! ds_set_id(+In, +Id, -Out) is det.
%
% Sets the document id. Throws error
% when the document is not a dict or
% id is not an atom.

ds_set_id(In, Id, Out):-
    must_be(dict, In),
    must_be(atom, Id),
    Out = In.put('$id', Id).

%! ds_uuid(-Id) is det.
%
% Generates UUID version 4 identifier.
% More info:
% http://en.wikipedia.org/wiki/Universally_unique_identifier

ds_uuid(Id):-
    uuid_pattern(Pat),
    maplist(fill, Pat),
    atom_chars(Id, Pat).

fill(Place):-
    var(Place), !,
    uuid_rand(Place).

fill(_).

uuid_rand(Hex):-
    List = ['0', '1', '2', '3', '4', '5',
        '6', '7', '8', '9', 'a', 'b',
        'c', 'd', 'e', 'f'],
    random_member(Hex, List).

% Patterns for UUID version 4.

uuid_pattern(Pat):-
    Pat = [_, _, _, _, _, _, _, _,
        -, _, _, _, _,
        -, '4', _, _, _,
        -, 'a', _, _, _,
        -, _, _, _, _, _, _, _, _, _, _, _, _].
