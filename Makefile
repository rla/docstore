version:=$(shell swipl -q -s pack -g 'version(V),writeln(V)' -t halt)
packfile=docstore-$(version).tgz
remote=www-data@packs.rlaanemets.com:/sites/packs.rlaanemets.com/public/docstore

test:
	swipl -s tests/tests.pl -g run_tests,halt -t 'halt(1)'

package: test
	tar cvzf $(packfile) prolog tests pack.pl LICENSE README.md

doc:
	swipl -q -t 'doc_save(prolog, [doc_root(doc),format(html),title(docstore),if(true),recursive(false)])'

upload: package doc
	scp $(packfile) $(remote)/$(packfile)
	rsync -avz -e ssh doc $(remote)

.PHONY: test package upload doc all
