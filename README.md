# docstore

Document-oriented transactional in-memory database for SWI-Prolog. Documents are represented
using [dicts](http://www.swi-prolog.org/pldoc/man?section=dicts) and are organized
into collections. Each document is assigned an unique identifier (`$id`) that can
be later used to retrieve/update/remove the document.

Data is stored in-memory and database changes are journaled onto the disk. This
works similar to [persistency.pl](http://www.swi-prolog.org/pldoc/doc/home/vnc/prolog/lib/swipl/library/persistency.pl)
except that the high-level interface is different (documents vs. predicates) and the library
is thread-safe. The library supports transactions and hooks (`before_save`, `before_remove`).

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
    ?- ds_get('23418d47-5835-41ff-a6b8-8748f3b2163e', Vehicle).
    Vehicle = vehicle{'$id':'23418d47-5835-41ff-a6b8-8748f3b2163e',
        make:chevrolet, model:corvette, year:1954}.

Remove:

    ?- ds_remove('23418d47-5835-41ff-a6b8-8748f3b2163e').
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

    pack_install('http://packs.rlaanemets.com/docstore/docstore-*.tgz')

## Changelog

 * 2014-01-02 version 1.0.0 - switch to dicts, more tests, transactions.
 * 2013-12-23 versions 0.0.1/0.0.2 - docstore working with option lists.

## API documentation

See <http://packs.rlaanemets.com/docstore/doc/>.

## Known issues

 * Disk representation is not very compact. This could be improved.
 * `before_save` hooks are only given updated values not whole document. This might change.

## License

The MIT License.
