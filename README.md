# docstore

Document-oriented transactional database for SWI-Prolog. Documents are represented
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

    ? ds_insert(vehicle{year: 1926, make: chrysler, model: imperial}).
    ? ds_insert(vehicle{year: 1953, make: chevrolet, model: corvette}).
    ? ds_insert(vehicle{year: 1954, make: cadillac, model: fleetwood}).

Query all documents in a collection:

    ?- ds_all(vehicle, List).
    List = [
        vehicle{'$id':'6c1ad40e-dd18-46db-aae7-7ff9163cd4e3',
            make:chrysler, model:imperial, year:1926},
        vehicle{'$id':'5fe585b5-65a9-4cc4-a037-a58a801c4f36',
            make:chevrolet, model:corvette, year:1953},
        vehicle{'$id':'de48f9e3-0953-4a48-a46a-9818eaf1c83f',
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
`ds_insert` not to be persisted as the predicate ends with `fail`. Same would
happen when the predicate threw an exception.

## Installation

    pack_install('http://packs.rlaanemets.com/docstore/docstore-*.tgz')

## Changelog

 * 2014-01 version 1.0.0 - switch to dicts, more tests, transactions.
 * 2013-12 versions 0.0.1/0.0.2 - docstore working with option lists.

## API documentation

See <http://packs.rlaanemets.com/docstore/doc/>.

## Known issues

 * Disk representation is not very compact. This could be improved.
 * `before_save` hooks are only given updated values not whole document. This might change.

## License

The MIT License.
