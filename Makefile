version:=$(shell swipl -q -s pack -g 'version(V),writeln(V)' -t halt)
packfile=docstore-$(version).tgz
remote=packs@packs.rlaanemets.com:/usr/share/nginx/packs.rlaanemets.com/docstore

test:
	swipl -s tests/tests -g run_tests -t halt

package: test
	tar cvzf $(packfile) prolog tests pack.pl README.md

doc:
	swipl -q -t 'doc_save(prolog, [doc_root(doc),format(html),title(docstore),if(true),recursive(false)])'

upload: package doc
	scp $(packfile) $(remote)/$(packfile)
	rsync -avz -e ssh doc $(remote)

.PHONY: test package upload doc all
