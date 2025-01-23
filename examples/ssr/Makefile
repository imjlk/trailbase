run: build
	RUST_BACKTRACE=1 cargo run -- --data-dir=traildepot run --public-dir=solid-ssr-app/dist/client --dev --js-runtime-threads=2

build:
	cd solid-ssr-app; pnpm run build

clean:
	rm -rf solid-ssr-app/dist

.PHONY: run build clean
