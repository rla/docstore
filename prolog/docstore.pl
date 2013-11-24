:- module(docstore, [
    ds_open/1,
    ds_close,
    ds_hook/3,
    ds_insert/2,
    ds_insert/3,
    ds_update/1,
    ds_upsert/3,
    ds_prop_insert/3,
    ds_prop_remove/2,
    ds_prop_update/3,
    ds_prop_list_push/3,
    ds_prop_list_remove/3,
    ds_prop_incr/2,
    ds_prop_decr/2,
    ds_get/2,
    ds_get/3,
    ds_all/2,
    ds_all/3,
    ds_all_ids/2,
    ds_find/3,
    ds_find/4,
    ds_collection/2,
    ds_remove/1,
    ds_remove/2,
    ds_remove_col/1,
    ds_uuid/1
]).

/** <module> Document-oriented database module.

Generic thread-safe store for key(value) terms.
Writes are serialized using a mutex. No aggregate
queries are supported (use find and calculate yourself).
*/

:- use_module(library(apply)).
:- use_module(library(random)).
:- use_module(library(error)).
:- use_module(library(debug)).

:- module_transparent(ds_hook/3).

:- dynamic(col/2).
:- dynamic(eav/3).
:- dynamic(file/2).

:- dynamic(hook/3).

%% ds_open(+File) is det.
%
% Opens the database file.
% Throws error(docstore_is_open) when
% the database is already open.

ds_open(_):-
    file(_, _), !,
    throw(error(docstore_is_open)).

ds_open(File):-
    must_be(atom, File),
    loadall(File),
    open(File, append, Stream, [encoding('utf8')]),
    assertz(file(File, Stream)).

%% ds_close is semidet.
%
% Closes the database. Removes in-memory data.
    
ds_close:-
    safely(close_unsafe).
    
close_unsafe:-
    file(_, Stream),
    close(Stream),    
    retractall(col(_, _)),
    retractall(eav(_, _, _)),
    retractall(file(_, _)).

% Loads database contents from
% the given file if it exists.
    
loadall(File):-
    exists_file(File), !,
    setup_call_cleanup(
        open(File, read, Stream, [encoding('utf8')]),
        load(Stream),
        close(Stream)
    ).

loadall(_).

% Loads database actions (assertz and retractall)
% from the stream until the end of the stream is
% reached.

load(Stream):-
    read(Stream, Term),
    (Term = end_of_file
    ->  true
    ;   load_term(Term),
        load(Stream)).
    
load_term(assertz(Term)):-
    assertz(Term).
    
load_term(retractall(Term)):-
    retractall(Term).

%% ds_hook(+Col, +Action, :Goal) is det.
%
% Adds new hook.
% Action is one of: [ before_save, before_remove ].
    
ds_hook(Col, Action, Mod:Goal):- !,
    assert_hook(Col, Action, Mod:Goal).
    
ds_hook(Col, Action, Goal):-
    context_module(Mod),
    assert_hook(Col, Action, Mod:Goal).
    
assert_hook(Col, Action, Goal):-
    hook(Col, Action, Goal), !.
    
assert_hook(Col, Action, Goal):-
    assertz(hook(Col, Action, Goal)).

%% ds_insert(+Col, +Doc) is det.
%
% Inserts new document into the given collection.
% Runs ds_before_save/2 hook.
    
ds_insert(Col, Doc):-
    ds_insert(Col, Doc, _).

%% ds_insert(+Col, +Doc, -Id) is det.
%
% Inserts new document into the given collection.
% Gives back the entity Id.
% Runs ds_before_save/2 hook.

ds_insert(Col, Doc, Id):-
    must_be(atom, Col),
    must_be(nonvar, Doc),
    safely(insert_unsafe(Col, Doc, Id)).
    
insert_unsafe(Col, Doc, Id):-
    run_before_save_hooks(Col, Doc, Processed),
    ds_uuid(Id),
    run(assertz(col(Col, Id))),
    maplist(assert_eav(Id), Processed).

assert_eav(Id, Term):-
    prop_term(Name, Value, Term),
    must_be(nonvar, Value),
    run(assertz(eav(Id, Name, Value))).

% Executes save hooks.
    
run_before_save_hooks(Col, Doc, Out):-
    findall(Goal, hook(Col, before_save, Goal), Goals),
    run_before_save_goals(Goals, Doc, Out).

run_before_save_goals([ Goal|Goals ], Doc, Out):-
    (call(Goal, Doc, Tmp)
    ->  run_before_save_goals(Goals, Tmp, Out)
    ;   run_before_save_goals(Goals, Doc, Out)).
    
run_before_save_goals([], Doc, Doc).

%% ds_update(+Doc) is semidet.
%
% Updates the given document. Only
% changed properties are updated.
% Fails if Doc contains no $id.
% Ignores updates to '$id'.
    
ds_update(Doc):-
    must_be(nonvar, Doc),
    memberchk('$id'(Id), Doc),
    col(Col, Id),
    safely(update_unsafe(Col, Id, Doc)).
    
update_unsafe(Col, Id, Doc):-
    run_before_save_hooks(Col, Doc, Processed),
    update_props(Id, Processed).

update_props(_, []).
    
update_props(Id, [Prop|Props]):-
    prop_term(Name, Value, Prop),
    update_prop(Id, Name, Value),
    update_props(Id, Props).
    
update_prop(_, '$id', _):- !.
    
update_prop(Id, Name, Value):-
    eav(Id, Name, Old), !,
    (Value = Old
    ->  true
    ;   prop_update_unsafe(Id, Name, Value)).
    
update_prop(Id, Name, Value):-
    run(assertz(eav(Id, Name, Value))).
    
%% ds_upsert(+Col, +Doc, -Id) is det.
%
% Inserts or updates the given document.

ds_upsert(Col, Doc, Id):-
    must_be(atom, Col),
    must_be(nonvar, Doc),
    (memberchk('$id'(Id), Doc)
    ->  ds_update(Doc)
    ;   ds_insert(Col, Doc, Id)).

%% ds_prop_insert(+Id, +Name, +Value) is semidet.
%
% Inserts property for the given document.
% Fails if document already has the property
% or the document does not exist.
% FIXME run ds_before_save hook.
    
ds_prop_insert(Id, Name, Value):-
    must_be(atom, Id),
    must_be(atom, Name),
    must_be(nonvar, Value),
    col(_, Id),
    \+ eav(Id, Name, Value),
    safely(run(assertz(eav(Id, Name, Value)))).
    
%% ds_prop_remove(+Id, +Name) is det.
%
% Removes property from the given
% document.
% FIXME run ds_before_save hook.
    
ds_prop_remove(Id, Name):-
    must_be(atom, Id),
    must_be(atom, Name),
    safely(run(retractall(eav(Id, Name, _)))).
    
%% ds_prop_update(+Id, +Name, +Value) is semidet.
%
% Updates property for the given document.
% Fails if the document does not exist.
% FIXME run ds_before_save hook.
    
ds_prop_update(Id, Name, Value):-
    must_be(atom, Id),
    must_be(atom, Name),
    must_be(nonvar, Value),
    col(_, Id),
    \+ eav(Id, Name, Value),
    safely(prop_update_unsafe(Id, Name, Value)).
    
prop_update_unsafe(Id, Name, Value):-
    run(retractall(eav(Id, Name, _))),
    run(assertz(eav(Id, Name, Value))).

%% ds_prop_list_push(+Id, +Name, +Value) is semidet.
%
% Treats property as list and appends new
% element to it.
% Fails if document already has the property
% or the document does not exist.
% FIXME run ds_before_save hook.
    
ds_prop_list_push(Id, Name, Value):-
    must_be(atom, Id),
    must_be(atom, Name),
    must_be(nonvar, Value),
    col(_, Id),
    eav(Id, Name, Old),
    append(Old, [Value], New),
    safely(prop_update_unsafe(Id, Name, New)).

%% ds_prop_list_remove(+Id, +Name, +Value) is semidet.
%
% Treats property as list and removes the
% element to it.
% Fails if document already has the property
% or the document does not exist.
% FIXME run ds_before_save hook.
    
ds_prop_list_remove(Id, Name, Value):-
    must_be(atom, Id),
    must_be(atom, Name),
    must_be(nonvar, Value),
    col(_, Id),
    eav(Id, Name, Old),
    delete(Old, Value, New),
    safely(prop_update_unsafe(Id, Name, New)).
    
%% prop_incr(+Id, +Name) is semidet.
%
% Treats property as number and increases it by 1.
% Fails if document already has the property
% or the document does not exist.
% FIXME run ds_before_save hook.
    
ds_prop_incr(Id, Name):-
    must_be(atom, Id),
    must_be(atom, Name),
    col(_, Id),
    eav(Id, Name, Old),
    New is Old + 1,
    safely(prop_update_unsafe(Id, Name, New)).
    
%% ds_prop_decr(+Id, +Name) is semidet.
%
% Treats property as number and decreases it by 1.
% Fails if document already has the property
% or the document does not exist.
% FIXME run ds_before_save hook.
    
ds_prop_decr(Id, Name):-
    must_be(atom, Id),
    must_be(atom, Name),
    col(_, Id),
    eav(Id, Name, Old),
    New is Old - 1,
    safely(prop_update_unsafe(Id, Name, New)).

%% ds_get(+Id, -Doc) is semidet.
%
% Retrieves entry with the given id.
    
ds_get(Id, Doc):-
    must_be(atom, Id),
    doc(Id, Doc).

%% ds_get(+Id, +Props, -Doc) is semidet.
%
% Retrieves entry with the given id.
% Retrieves subset of properties.
    
ds_get(Id, Props, Doc):-    
    must_be(atom, Id),
    doc(Id, Props, Doc).

%% ds_all(+Col, -List) is det.
%
% Finds list of all documents in the given
% collection.
    
ds_all(Col, List):-
    must_be(atom, Col),
    findall(Doc, col_doc(Col, Doc), List).
    
%% ds_all(+Col, +Props, -List) is det.
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

%% ds_all_ids(+Col, -List) is det.
%
% Retrieves the list of all document
% IDs in the collection.
    
ds_all_ids(Col, List):-
    must_be(atom, Col),
    findall(Id, col(Col, Id), List).

%% ds_find(+Col, +Cond, -List) is semidet.
%
% Finds collection entries that
% satisfy cond.

ds_find(Col, Cond, List):-
    must_be(atom, Col),
    findall(Doc, cond_doc(Col, Cond, Doc), List).

%% ds_find(+Col, +Cond, +Props, -List) is semidet.
%
% Finds collection entries that
% satisfy cond. Retrieves only the given
% attributes. Entries that have one or more
% attributes missing, are not retrieved.
    
ds_find(Col, Cond, Props, List):-
    must_be(atom, Col),
    must_be(list(atom), Props),
    findall(Doc, cond_doc(Col, Cond, Props, Doc), List).

cond_doc(Col, Cond, Doc):-
    col(Col, Id),
    cond(Cond, Id),
    doc(Id, Doc).
    
cond_doc(Col, Cond, Props, Doc):-
    col(Col, Id),
    cond(Cond, Id),
    doc(Id, Props, Doc).

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
    
doc(Id, ['$id'(Id)|Doc]):-
    doc_props(Id, Doc).

% Finds document by ID. Gives
% back subset of its properties.
    
doc(Id, Props, ['$id'(Id)|Doc]):-
    doc_props(Id, Props, Doc).
    
% Finds document properties
% with values.
    
doc_props(Id, Doc):-
    findall(Prop, doc_prop(Id, Prop), Doc).

% Finds document properties
% with values. Subset of properties.
    
doc_props(Id, Props, Doc):-
    Props = [_|_],
    findall(Prop, (member(Name, Props), doc_prop(Id, Name, Prop)), Doc).
    
doc_props(Id, Prop, Doc):-
    atom(Prop),
    doc_props(Id, [Prop], Doc).
    
doc_prop(Id, Prop):-
    eav(Id, Name, Value),
    prop_term(Name, Value, Prop).
    
doc_prop(Id, Name, Prop):-
    eav(Id, Name, Value),
    prop_term(Name, Value, Prop).

prop_term(Name, Value, Prop):-
    Prop =.. [Name, Value].

%% ds_collection(?Id, ?Col) is semidet.
%
% Finds which collection the document
% belongs to.

ds_collection(Id, Col):-
    col(Col, Id).

%% ds_remove(+Id) is det.
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

%% ds_remove(+Col, +Cond) is det.
%
% Removes all documents from
% the collection that match the
% condition. Runs ds_before_remove/2 hooks.
    
ds_remove(Col, Cond):-
    must_be(atom, Col),
    findall(Id, (col(Col, Id), cond(Cond, Id)), Ids),
    safely(maplist(remove_unsafe, Ids)).

%% ds_remove_col(Col) is det.
%
% Removes all documents from
% the given collection. Is equivalent
% of running remove/1 for each document
% in the collection. Runs ds_before_remove/2 hooks.
    
ds_remove_col(Col):-
    ds_all_ids(Col, Ids),
    safely(maplist(remove_unsafe, Ids)).
    
% Runs the Goal with global mutex.
% All modifications to the database
% are run using it.
    
safely(Goal):-
    with_mutex(db_store, Goal).

% Helper to run and log the goal.
% Throws error(database_not_open) when
% the database is not open.

run(_):-
    \+ file(_, _), !,
    throw(error(docstore_not_open)).
    
run(Goal):-
    file(_, Stream),
    write_canonical(Stream, Goal),
    write(Stream, '.'),
    nl(Stream),
    flush_output(Stream),
    call(Goal).
    
%% ds_uuid(-Id) is det.
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

