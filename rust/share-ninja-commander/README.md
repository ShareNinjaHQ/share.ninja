# share-ninja-commander — `sninja cmd`

MC-like dual-pane terminal file manager for the local filesystem and
[Share.Ninja](https://share.ninja) shares, built directly on
`share-ninja-core` (no local server): a panel is a `LocalFs` directory or a
share browsed on a warm download transfer; copies run on the core transfer
engine with MC's overwrite / error dialogs; login, the vault trust circle and
nearby devices ride the `sninja` binary's own backend session
(`share-ninja-client`).

A library crate: the `sninja` binary (`share-ninja-cli`) wires it up as the
`cmd` subcommand (`src/commands/cmd.rs`), sharing the CLI's config, device
identity and keychain item.

```bash
cargo run -p share-ninja-cli -- cmd                              # local ⇄ local
cargo run -p share-ninja-cli -- cmd https://share.ninja/#CLAIM   # share in the right pane
NINJA_BASE_URL=https://test.example.com sninja cmd --log-path /tmp/nc.log --debug-api
```

F1 lists the keys. Shares are read-only; copies go share → local or local → local.
A nearby offer from another device pops a dialog: Accept opens the share in
the other panel (F5 copies it), Decline answers no, Esc leaves it unanswered.
F3 opens the built-in viewer (text / hex, wrap, goto, search): it reads only
the blocks on screen straight off the source, so a share file streams over
P2P / S3 range reads and is never copied locally. F4 still shells out to
`mcedit` for local files.
