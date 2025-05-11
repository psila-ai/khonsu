.PHONY: w b test stest

define DEBUGBUILDNOTIF
    ____       __                   ____        _ __    __
   / __ \___  / /_  __  ______ _   / __ )__  __(_) /___/ /
  / / / / _ \/ __ \/ / / / __ `/  / __  / / / / / / __  /
 / /_/ /  __/ /_/ / /_/ / /_/ /  / /_/ / /_/ / / / /_/ /
/_____/\___/_.___/\__,_/\__, /  /_____/\__,_/_/_/\__,_/
                       /____/
endef
export DEBUGBUILDNOTIF

w:
	cargo watch -c

b:
	cargo build

test:
	cargo test

# Below is for non distributed single threaded tests.
stest:
	cargo test --no-default-features -- --test-threads=1
