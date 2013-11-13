:- module(docstore, [
    open/1,
    close,    
    insert/2,
    insert/3,
    update/1,
    upsert/3,
    prop_insert/3,
    prop_remove/2,
    prop_update/3,
    prop_list_push/3,
    prop_list_remove/3,
    prop_incr/2,
    prop_decr/2,
    get/2,
    get/3,
    all/2,
    all/3,
    all_ids/2,
    find/3,
    find/4,    
    remove/1,
    remove/2,
    remove_col/1
]).

/** <module> Document-oriented database module.

Generic thread-safe store for key(value) terms.
Writes are serialized using a mutex. No aggregate
queries are supported (use find and calculate yourself).
*/

:- use_module(library(apply)).
:- use_module(library(random)).
:- use_module(library(error)).

:- dynamic(col/2).
:- dynamic(eav/3).
:- dynamic(file/2).

%% open(+File) is det.
%
% Opens the database file.
% Throws error(docstore_is_open) when
% the database is already open.

open(_):-
    file(_, _), !,
    throw(error(docstore_is_open)).

open(File):-
    must_be(atom, File),
    loadall(File),
    open(File, append, Stream, [encoding('utf8')]),
    assertz(file(File, Stream)).

%% close is semidet.
%
% Closes the database.
% Currently in-memory data stays
% in-memory but modifications will
% throw exception.
    
close:-
    file(_, Stream),
    close(Stream),
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
    (Term = end_of_file ->
        true
    ;
        load_term(Term),
        load(Stream)
    ).
    
load_term(assertz(Term)):-
    assertz(Term).
    
load_term(retractall(Term)):-
    retractall(Term).
    
%% insert(+Col, +Doc) is det.
%
% Inserts new document into the given collection.
    
insert(Col, Doc):-
    insert(Col, Doc, _).

%% insert(+Col, +Doc, -Id) is det.
%    
% Inserts new document into the given collection.
% Gives back the entity Id.
    
insert(Col, Doc, Id):-
    must_be(atom, Col),
    must_be(nonvar, Doc),
    safely(insert_unsafe(Col, Doc, Id)).
    
insert_unsafe(Col, Doc, Id):-
    uuid(Id),
    run(assertz(col(Col, Id))),
    maplist(assert_eav(Id), Doc).

assert_eav(Id, Term):-
    prop_term(Name, Value, Term),
    must_be(nonvar, Value),
    run(assertz(eav(Id, Name, Value))).

%% update(+Doc) is semidet.
%
% Updates the given document. Only
% changed properties are updated.
% Fails if Doc contains no $id.
% Ignores updates to '$id'.
    
update(Doc):-
    must_be(nonvar, Doc),
    memberchk('$id'(Id), Doc),
    safely(update_props(Id, Doc)).

update_props(_, []).
    
update_props(Id, [Prop|Props]):-
    prop_term(Name, Value, Prop),
    update_prop(Id, Name, Value),
    update_props(Id, Props).
    
update_prop(_, '$id', _):- !.
    
update_prop(Id, Name, Value):-
    eav(Id, Name, Old), !,
    (Value = Old -> true ; prop_update_unsafe(Id, Name, Value)).
    
update_prop(Id, Name, Value):-
    run(assertz(eav(Id, Name, Value))).
    
%% upsert(+Col, +Doc, -Id) is det.
%
% Inserts or updates the given document.

upsert(Col, Doc, Id):-
    must_be(atom, Col),
    must_be(nonvar, Doc),
    (memberchk('$id'(Id), Doc) -> update(Doc) ; insert(Col, Doc, Id)).

%% prop_insert(+Id, +Name, +Value) is semidet.
%
% Inserts property for the given document.
% Fails if document already has the property
% or the document does not exist.
    
prop_insert(Id, Name, Value):-
    must_be(atom, Id),
    must_be(atom, Name),
    must_be(nonvar, Value),
    col(_, Id),
    \+ eav(Id, Name, Value),
    safely(run(assertz(eav(Id, Name, Value)))).
    
%% prop_remove(+Id, +Name) is det.
%
% Removes property from the given
% document.
    
prop_remove(Id, Name):-
    must_be(atom, Id),
    must_be(atom, Name),
    safely(run(retractall(eav(Id, Name, _)))).
    
%% prop_update(+Id, +Name, +Value) is semidet.
%
% Updates property for the given document.
% Fails if the document does not exist.
    
prop_update(Id, Name, Value):-
    must_be(atom, Id),
    must_be(atom, Name),
    must_be(nonvar, Value),
    col(_, Id),
    \+ eav(Id, Name, Value),
    safely(prop_update_unsafe(Id, Name, Value)).
    
prop_update_unsafe(Id, Name, Value):-
    run(retractall(eav(Id, Name, _))),
    run(assertz(eav(Id, Name, Value))).

%% prop_list_push(+Id, +Name, +Value) is semidet.
%
% Treats property as list and appends new
% element to it.
% Fails if document already has the property
% or the document does not exist.
    
prop_list_push(Id, Name, Value):-
    must_be(atom, Id),
    must_be(atom, Name),
    must_be(nonvar, Value),
    col(_, Id),
    eav(Id, Name, Old),
    append(Old, [Value], New),
    safely(prop_update_unsafe(Id, Name, New)).

%% prop_list_remove(+Id, +Name, +Value) is semidet.
%
% Treats property as list and removes the
% element to it.
% Fails if document already has the property
% or the document does not exist.
    
prop_list_remove(Id, Name, Value):-
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
    
prop_incr(Id, Name):-
    must_be(atom, Id),
    must_be(atom, Name),
    col(_, Id),
    eav(Id, Name, Old),
    New is Old + 1,
    safely(prop_update_unsafe(Id, Name, New)).
    
%% prop_decr(+Id, +Name) is semidet.
%
% Treats property as number and decreases it by 1.
% Fails if document already has the property
% or the document does not exist.
    
prop_decr(Id, Name):-
    must_be(atom, Id),
    must_be(atom, Name),
    col(_, Id),
    eav(Id, Name, Old),
    New is Old - 1,
    safely(prop_update_unsafe(Id, Name, New)).

%% get(+Id, -Doc) is semidet.
%
% Retrieves entry with the given id.
    
get(Id, Doc):-
    must_be(atom, Id),
    doc(Id, Doc).

%% get(+Id, +Props, -Doc) is semidet.
%
% Retrieves entry with the given id.
% Retrieves subset of properties.
    
get(Id, Props, Doc):-    
    must_be(atom, Id),
    doc(Id, Props, Doc).

%% all(+Col, -List) is det.
%
% Finds list of all documents in the given
% collection.
    
all(Col, List):-
    must_be(atom, Col),
    findall(Doc, col_doc(Col, Doc), List).
    
%% all(+Col, +Props, -List) is det.
%
% Finds list of all documents in the given
% collection. Retrieves subset of properties.
    
all(Col, Props, List):-
    must_be(atom, Col),
    findall(Doc, col_doc(Col, Props, Doc), List).
    
col_doc(Col, Doc):-
    col(Col, Id),
    doc(Id, Doc).
    
col_doc(Col, Props, Doc):-
    col(Col, Id),
    doc(Id, Props, Doc).

%% all_ids(+Col, -List) is det.
%
% Retrieves the list of all document
% IDs in the collection.
    
all_ids(Col, List):-
    must_be(atom, Col),
    findall(Id, col(Col, Id), List).

%% find(+Col, +Cond, -List) is semidet.
%
% Finds collection entries that
% satisfy cond.

find(Col, Cond, List):-
    must_be(atom, Col),
    findall(Doc, cond_doc(Col, Cond, Doc), List).

%% find(+Col, +Cond, +Props, -List) is semidet.
%
% Finds collection entries that
% satisfy cond. Retrieves only the given
% attributes. Entries that have one or more
% attributes missing, are not retrieved.
    
find(Col, Cond, Props, List):-
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

%% remove(+Id) is det.
%
% Removes the given document.
% Does nothing when the document
% does not exist.

remove(Id):-
    must_be(atom, Id),
    safely(remove_unsafe(Id)).

remove_unsafe(Id):-
    run(retractall(eav(Id, _, _))),
    run(retractall(col(_, Id))).

%% remove(+Col, +Cond) is det.
%
% Removes all documents from
% the collection that match the
% condition.
    
remove(Col, Cond):-
    must_be(atom, Col),
    findall(Id, (col(Col, Id), cond(Cond, Id)), Ids),
    safely(maplist(remove_unsafe, Ids)).

%% remove_col(Col) is det.
%
% Removes all documents from
% the given collection. Is equivalent
% of running remove/1 for each document
% in the collection.
    
remove_col(Col):-
    all_ids(Col, Ids),
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
    
% Generates UUID version 4 identifier.    
    
uuid(Id):-
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

