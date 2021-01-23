# docstore

Document-oriented transactional in-memory database for SWI-Prolog. Documents are represented
using [dicts](http://www.swi-prolog.org/pldoc/man?section=dicts) and are organized
into collections. Each document is assigned an unique identifier (`$id`) that can
be later used to retrieve/update/remove the document.

Data is stored in-memory and database changes are journaled onto the disk. This
works similar to [persistency.pl](http://www.swi-prolog.org/pldoc/doc/home/vnc/prolog/lib/swipl/library/persistency.pl)
except that the high-level interface is different (documents vs. predicates) and the library
is thread-safe. The library supports transactions and hooks (`before_save`, `before_remove`).

[![Build Status](https://travis-ci.org/rla/docstore.svg)](https://travis-ci.org/rla/docstore)

## Example usage

Open database:

    ?- ds_open('test.db').
    true.

Insert some data:

    ?- ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}).
    ?- ds_insert(vehicle{year: 1953, make: chevrolet, model: corvette}).
    ?- ds_insert(vehicle{year: 1954, make: cadillac, model: fleetwood}).

Query all documents in a collection:

    ?- ds_all(vehicle, List).
    List = [
        vehicle{'$id':'f3012622-cacb-4d7f-a22a-c36305274a80',
            make:chrysler, model:imperial, year:1926},
        vehicle{'$id':'23418d47-5835-41ff-a6b8-8748f3b2163e',
            make:chevrolet, model:corvette, year:1953},
        vehicle{'$id':'8c79f80f-d43e-4fad-a1bb-5fca23a195e0',
            make:cadillac, model:fleetwood, year:1954}].

Query by condition:

    ?- ds_find(vehicle, year=1953, List).
    List = [
        vehicle{'$id':'23418d47-5835-41ff-a6b8-8748f3b2163e',
            make:chevrolet, model:corvette, year:1953}].

Update:

    ?- ds_update(vehicle{'$id':'23418d47-5835-41ff-a6b8-8748f3b2163e', year: 1954}).
    ?- ds_col_get(vehicle, '23418d47-5835-41ff-a6b8-8748f3b2163e', Vehicle).
    Vehicle = vehicle{'$id':'23418d47-5835-41ff-a6b8-8748f3b2163e',
        make:chevrolet, model:corvette, year:1954}.

Remove:

    ?- ds_col_remove(vehicle, '23418d47-5835-41ff-a6b8-8748f3b2163e').
    ?- ds_all(vehicle, List).
    List = [
        vehicle{'$id':'f3012622-cacb-4d7f-a22a-c36305274a80',
            make:chrysler, model:imperial, year:1926},
        vehicle{'$id':'8c79f80f-d43e-4fad-a1bb-5fca23a195e0',
            make:cadillac, model:fleetwood, year:1954}].

## Transactions

Transactions are built-in to guarantee database consistency. Each predicate that
modifies database (such as `ds_insert`) starts implicit transaction. Transactions
can be nested. In that case only the outer transaction has effect. To use explicit
transaction, use `ds_transactional/1`. Example:

    fail_test:-
        ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}),
        fail.

Running this through `ds_transactional/1` causes no changes made by
`ds_insert` to be persisted as the predicate ends with `fail`. Same would
happen when the predicate threw an exception.

## Hooks

Two kinds of hooks are supported: before save and before remove. Hooks are
registered using the `ds_hook/3` predicate. Hook that fails or throws an
exception will abort the current transaction. Transactions inside hooks
are joined with the currently running transaction.

## Installation

Requires SWI-Prolog 7.x.

    pack_install(docstore).

## Changelog

 * 2021-01-23 version 2.0.2 - fix `load_tx_begin` not being a predicate indicator.
 * 2014-04-22 version 1.0.1 - use dot notation instead of get_dict_ex/3.
 * 2014-01-02 version 1.0.0 - switch to dicts, more tests, transactions.
 * 2013-12-23 versions 0.0.1/0.0.2 - docstore working with option lists.

### Version 2.x

 * `ds_remove/1` is removed, provided untyped access, use `ds_col_remove/2`.
 * `ds_remove/2` is removed, use `ds_col_remove_cond/2`.
 * `ds_id/2` can be used to extract document id.
 * `ds_get/2` and `ds_get/3` are removed, provided untyped access, use `ds_col_get/3`
   and `ds_col_get/4`.

## API documentation

See <http://packs.rlaanemets.com/docstore/doc/>.

## Debugging

Enable debugging with `debug(docstore)` on the console.

## Known issues

 * Disk representation is not very compact. This could be improved.
 * `before_save` hooks are only given updated values not whole document. This might change.

## Bug reports/feature requests

Please send bug reports/feature request through the GitHub project [page](https://github.com/rla/docstore).

## License

The MIT License.
