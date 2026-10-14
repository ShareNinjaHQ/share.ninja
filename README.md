# Share.Ninja client

Source of the [Share.Ninja](https://share.ninja) client core and the `sninja`
command-line tool, published so that the code handling your files and keys
can be read and audited.

## What is here

- `rust/share-ninja-core` — the client core: end-to-end encryption, the
  backend API client, cloud and peer-to-peer (WebRTC) transfers.
- `rust/share-ninja-client` — the client-side protocol types.
- `rust/share-ninja-commander` — the terminal UI used by `sninja`.
- `rust/share-ninja-cli` — the `sninja` binary.
- `rust/generated` — the backend API client generated from the OpenAPI
  contract, committed here as plain source.
- `rust/vendor` — patched copies of `webrtc`, `rtc`, `rtc-sctp`, `rtc-shared`
  and `tui-textarea`; each patch is described next to its entry in
  `rust/Cargo.toml`. These crates keep their own licenses.

The backend service and the desktop, iOS and Android applications are not
part of this repository.

## Building

```bash
cd rust
cargo build --release -p share-ninja-cli
./target/release/sninja --help
```

## Contributing

Development happens in an internal repository; this one receives snapshots of
it, each commit named after the internal revision it was taken from. Issues
are welcome — a fix is made internally and arrives here with the next
snapshot.

## License

Apache License 2.0, see [LICENSE](LICENSE).
