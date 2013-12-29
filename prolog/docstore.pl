:- module(docstore, [
    ds_open/1,       % +File
    ds_close/0,
    ds_snapshot/1,   % +File
    ds_snapshot/0,
    ds_hook/2,       % +Action, :Goal
    ds_hook/3,       % +Col, +Action, :Goal
    ds_insert/1,     % +Dict
    ds_insert/2,     % +Dict, -Id
    ds_insert/3,     % +Col, +Dict, -Id
    ds_update/1,     % +Dict
    ds_upsert/1,     % +Dict
    ds_upsert/2,     % +Dict, -Id
    ds_upsert/3,     % +Col, +Dict, -Id
    ds_get/2,        % +Id, -Dict
    ds_get/3,        % +Id, +Keys, -Dict
    ds_all/2,        % +Col, -List
    ds_all/3,        % +Col, +Keys, -List
    ds_all_ids/2,    % +Col, -List
    ds_find/3,       % +Col, +Cond, -List
    ds_find/4,       % +Col, +Cond, +Keys, -List
    ds_collection/2, % ?Id, ?Col
    ds_remove/1,     % +Id
    ds_remove/2,     % +Col, Cond
    ds_remove_col/1, % +Col
    ds_uuid/1        % -Uuid
]).

/** <module> Document-oriented database

Generic thread-safe store for dict terms.
*/

:- use_module(library(apply)).
:- use_module(library(random)).
:- use_module(library(error)).
:- use_module(library(debug)).

:- dynamic(col/2).
:- dynamic(eav/3).
:- dynamic(file/2).

:- dynamic(hook/3).
:- dynamic(hook/2).

%! ds_open(+File) is det.
%
% Opens the database file.
% Throws error(docstore_is_open) when
% the database is already open.
%
% Runs all open hooks.

ds_open(_):-
    file(_, _), !,
    throw(error(docstore_is_open)).

ds_open(File):-
    must_be(atom, File),
    loadall(File),
    open(File, append, Stream, [
        encoding('utf8'), lock(write)
    ]),
    assertz(file(File, Stream)),
    run_open_hooks.

run_open_hooks:-
    findall(Goal, hook(open, Goal), Goals),
    maplist(ignore, Goals).

%! ds_close is det.
%
% Closes the database. Removes in-memory data.
% Runs close hooks. Hooks are ran before
% the file is closed and data is purged from memory.
%
% Throws error(database_is_not_open) when
% the database file is not open.

ds_close:-
    safely(close_unsafe).

close_unsafe:-
    (   file(_, Stream)
    ->  true
    ;   throw(error(database_is_not_open))),
    run_close_hooks,
    close(Stream),
    retractall(col(_, _)),
    retractall(eav(_, _, _)),
    retractall(file(_, _)).

run_close_hooks:-
    findall(Goal, hook(close, Goal), Goals),
    maplist(ignore, Goals).

% Loads database contents from
% the given file if it exists.

loadall(File):-
    exists_file(File), !,
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
    ->  true
    ;   load_term(Term),
        load(Stream)).

load_term(assertz(Term)):-
    assertz(Term).

load_term(retractall(Term)):-
    retractall(Term).

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
    ((  col(Col, Id),
        write_goal(Stream, assertz(col(Col, Id))),
        fail
    ) ; true),
    ((  eav(Id, Name, Value),
        write_goal(Stream, assertz(eav(Id, Name, Value))),
        fail
    ) ; true).

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

:- meta_predicate(ds_hook(+, 0)).

%! ds_hook(+Action, :Goal) is det.
%
% Registers new hook for open or
% close action.

ds_hook(Action, Goal):-
    (   hook(Action, Goal)
    ->  true
    ;   assertz(hook(Action, Goal))).

:- meta_predicate(ds_hook(+, +, :)).

%! ds_hook(+Col, +Action, :Goal) is det.
%
% Adds new hook.
% Action is one of: [before_save, before_remove].

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
    is_dict(Doc, Col),
    ds_insert(Col, Doc, Id).

%! ds_insert(+Col, +Doc, -Id) is det.
%
% Inserts new document into the given collection.
% Gives back the generated ID. Document must be a dict.

ds_insert(Col, Doc, Id):-
    must_be(atom, Col),
    must_be(nonvar, Doc),
    safely(insert_unsafe(Col, Doc, Id)).

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

run_before_save_goals([ Goal|Goals ], Doc, Out):-
    (   call(Goal, Doc, Tmp)
    ->  run_before_save_goals(Goals, Tmp, Out)
    ;   run_before_save_goals(Goals, Doc, Out)).

run_before_save_goals([], Doc, Doc).

%! ds_update(+Doc) is semidet.
%
% Updates the given document. Only
% changed properties are updated.
% Throws error if Doc contains no $id.
% Ignores updates to '$id'. Runs
% before_save hooks.

ds_update(Doc):-
    must_be(nonvar, Doc),
    Id = Doc.'$id',
    col(Col, Id),
    safely(update_unsafe(Col, Id, Doc)).

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
    must_be(nonvar, Doc),
    (   get_dict('$id', Doc, Id)
    ->  ds_update(Doc)
    ;   ds_insert(Doc, Id)).

%! ds_upsert(+Col, +Doc, -Id) is det.
%
% Inserts or updates the given document.

ds_upsert(Col, Doc, Id):-
    must_be(atom, Col),
    must_be(nonvar, Doc),
    (   get_dict('$id', Doc, Id)
    ->  ds_update(Doc)
    ;   ds_insert(Col, Doc, Id)).

%! ds_get(+Id, -Doc) is semidet.
%
% Retrieves entry with the given id.

ds_get(Id, Doc):-
    must_be(atom, Id),
    doc(Id, Doc).

%! ds_get(+Id, +Keys, -Doc) is semidet.
%
% Retrieves entry with the given id.
% Retrieves subset of properties.

ds_get(Id, Keys, Doc):-
    must_be(atom, Id),
    doc(Id, Keys, Doc).

%! ds_all(+Col, -List) is det.
%
% Finds list of all documents in the given
% collection.

ds_all(Col, List):-
    must_be(atom, Col),
    findall(Doc, col_doc(Col, Doc), List).

%! ds_all(+Col, +Props, -List) is det.
%
% Finds list of all documents in the given
% collection. Retrieves subset of properties.

ds_all(Col, Props, List):-
    must_be(atom, Col),
    findall(Doc, col_doc(Col, Props, Doc), List).

col_doc(Col, Doc):-
    col(Col, Id),
    doc(Id, Doc).

col_doc(Col, Props, Doc):-
    col(Col, Id),
    doc(Id, Props, Doc).

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
% satisfy cond.

ds_find(Col, Cond, List):-
    must_be(atom, Col),
    findall(Doc, cond_doc(Col, Cond, Doc), List).

%! ds_find(+Col, +Cond, +Props, -List) is semidet.
%
% Finds collection entries that
% satisfy cond. Retrieves only the given
% attributes. Entries that have one or more
% attributes missing, are not retrieved.

ds_find(Col, Cond, Props, List):-
    must_be(atom, Col),
    must_be(list(atom), Props),
    findall(Doc, cond_doc(Col, Cond, Props, Doc), List).

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

cond(Name = Value, Id):-
    eav(Id, Name, Value).

cond(Name \= Comp, Id):-
    eav(Id, Name, Value),
    Value \= Comp.

cond(Name > Comp, Id):-
    eav(Id, Name, Value),
    Value > Comp.

cond(Name < Comp, Id):-
    eav(Id, Name, Value),
    Value < Comp.

cond(Name >= Comp, Id):-
    eav(Id, Name, Value),
    Value >= Comp.

cond(Name =< Comp, Id):-
    eav(Id, Name, Value),
    Value =< Comp.

cond(member(Item, Name), Id):-
    eav(Id, Name, Value),
    memberchk(Item, Value).

cond(','(Left, Right), Id):-
    cond(Left, Id),
    cond(Right, Id).

cond(';'(Left, _), Id):-
    cond(Left, Id).

cond(';'(_, Right), Id):-
    cond(Right, Id).

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

doc_kv_pairs(Id, Keys, Pairs):-
    Keys = [_|_],
    findall(Pair, (
        member(Key, Keys),
        doc_kv_pair(Id, Key, Pair)
    ), Pairs).

doc_kv_pairs(Id, Key, Doc):-
    atom(Key),
    doc_kv_pairs(Id, [Key], Doc).

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

%! ds_remove(+Id) is det.
%
% Removes the given document.
% Does nothing when the document
% does not exist. Runs ds_before_remove/2 hooks.

ds_remove(Id):-
    must_be(atom, Id),
    safely(remove_unsafe(Id)).

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

run_before_remove_goals([ Goal|Goals ], Id):-
    (call(Goal, Id) ; true),
    run_before_remove_goals(Goals, Id).

run_before_remove_goals([], _).

%! ds_remove(+Col, +Cond) is det.
%
% Removes all documents from
% the collection that match the
% condition. Runs ds_before_remove/2 hooks.

ds_remove(Col, Cond):-
    must_be(atom, Col),
    findall(Id, (col(Col, Id), cond(Cond, Id)), Ids),
    safely(maplist(remove_unsafe, Ids)).

%! ds_remove_col(Col) is det.
%
% Removes all documents from
% the given collection. Is equivalent
% of running remove/1 for each document
% in the collection. Runs ds_before_remove/2 hooks.

ds_remove_col(Col):-
    ds_all_ids(Col, Ids),
    safely(maplist(remove_unsafe, Ids)).

:- meta_predicate(safely(0)).

% Runs the Goal with global mutex.
% All modifications to the database
% are run using it.

safely(Goal):-
    with_mutex(db_store, Goal).

:- meta_predicate(run(0)).

% Helper to run and log the goal.
% Throws error(database_not_open) when
% the database is not open.

run(_):-
    \+ file(_, _), !,
    throw(error(docstore_not_open)).

run(Goal):-
    file(_, Stream),
    Goal = _:Local,
    write_goal(Stream, Local),
    flush_output(Stream),
    call(Goal).

write_goal(Stream, Goal):-
    write_term(Stream, Goal, [
        ignore_ops, quoted, dotlists(true)
    ]),
    write(Stream, '.'),
    nl(Stream).

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

