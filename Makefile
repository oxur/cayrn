CARGO ?= cargo
.PHONY: help build test fmt-check fmt lint doc check
help:
	@echo "build test fmt-check fmt lint doc check"
build:
	$(CARGO) build --workspace --locked
test:
	$(CARGO) test --workspace --all-targets --locked
fmt-check:
	$(CARGO) fmt --all -- --check
fmt:
	$(CARGO) fmt --all
lint:
	$(CARGO) clippy --workspace --all-targets --locked -- -D warnings
doc:
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --workspace --no-deps --locked
check: fmt-check test lint doc
