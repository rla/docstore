# docstore

Document-oriented database for SWI-Prolog. Documents are represented
using [dicts](http://www.swi-prolog.org/pldoc/man?section=dicts).

## Example usage

Insert some data:

    

Query all documents in a collection:

    ?- ds_all(user, List).
    List = [user{'$id':'1c354c13-a981-40cc-ad88-2e2441a3f74a', name:john}, user{'$id':'625c675f-3a97-44cf-ab10-fa2641b5a33e', name:mary}].

## Installation

    pack_install('http://packs.rlaanemets.com/docstore/docstore-*.tgz')

## API documentation

See <http://packs.rlaanemets.com/docstore/doc/>.

## License

The MIT License.
