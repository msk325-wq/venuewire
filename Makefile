.PHONY: check fmt lint test up down bench

check: fmt lint test

fmt:
	cargo fmt --all --check

lint:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

up:
	docker compose up -d

down:
	docker compose down

bench:
	cargo run --release --bin vwbench
