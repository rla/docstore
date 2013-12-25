:- begin_tests(docstore_snapshot).

:- use_module(prolog/docstore).
:- use_module(library(readutil)).

same_content(File1, File2):-
    read_file_to_codes(File1, Codes1, []),
    read_file_to_codes(File2, Codes2, []),
    (   Codes1 = Codes2
    ->  true
    ;   throw(error(files_not_same_content(File1, File2)))).

test(snapshot1, [ setup(ds_open('snapshot.test.db')),
        cleanup((ds_close, delete_file('snapshot.test.db'),
            delete_file('snapshot.db')))]):-

    ds_insert(users, [ name(john) ]),
    ds_snapshot('snapshot.db'),
    same_content('snapshot.test.db', 'snapshot.db').

:- end_tests(docstore_snapshot).
