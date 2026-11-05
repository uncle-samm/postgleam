.PHONY: build test test-setup smoke format format-check docs check push tag publish release bench bench-pure bench-integration bench-realworld bench-throughput bench-compare

build:
	gleam build

test:
	gleam test

test-setup:
	./scripts/setup_test_db.sh

smoke:
	./scripts/smoke_test.sh

format:
	gleam format src test

format-check:
	gleam format --check src test

docs:
	gleam docs build

check: format-check build test docs

push:
	sh scripts/push.sh

tag:
	sh scripts/tag.sh

publish:
	/usr/bin/expect scripts/publish.sh

release: check push tag publish

bench: bench-pure bench-integration

bench-pure:
	gleam build && erl -pa build/dev/erlang/*/ebin -noshell -run bench_runner main -- pure

bench-integration:
	gleam build && erl -pa build/dev/erlang/*/ebin -noshell -run bench_runner main -- integration

bench-realworld:
	gleam build && erl -pa build/dev/erlang/*/ebin -noshell -run bench_runner main -- realworld

bench-throughput:
	gleam build && erl -pa build/dev/erlang/*/ebin -noshell -run bench_runner main -- throughput

bench-compare:
	@if [ -z "$(BASELINE)" ] || [ -z "$(CURRENT)" ]; then \
		echo "Usage: make bench-compare BASELINE=bench/results/pure.json CURRENT=bench/results/pure.json"; \
		exit 1; \
	fi
	gleam build && erl -pa build/dev/erlang/*/ebin -noshell -run bench_runner main -- compare $(BASELINE) $(CURRENT)
