# share-ninja-cli — `sninja`

Command-line interface for Share.Ninja - secure file transfers - and the
commander, an MC-like dual-pane TUI file manager (`sninja cmd`). One binary,
one device identity.

## Installation

```bash
cargo install --path .
```

Or build directly:

```bash
cargo build --release -p share-ninja-cli
# Binary will be at target/release/sninja
```

## Usage

### Upload Files (put)

Upload files to cloud storage and create a share link:

```bash
sninja put [--description] [--validity] [--streams] path(s)
```

**Path semantics:**
- `/path/to/dir` - Include the directory itself (appears as `dir/` in the share)
- `/path/to/dir/` - Include only the directory contents (files appear at root level)
- `/path/to/dir/*` - Same as trailing slash, contents only

**Options:**
- `--description <text>` - Description for the share
- `--validity <seconds>` - Link validity in seconds (default: 604800 = 7 days)
- `--streams <n>` - Concurrent upload streams (default: 5, 1 = fully serial)

**Examples:**

```bash
# Upload a single file
sninja put myfile.txt

# Upload multiple files
sninja put file1.txt file2.pdf

# Upload a directory (includes the folder itself)
sninja put ./my-folder

# Upload directory contents only (files at root level)
sninja put ./my-folder/
sninja put "./my-folder/*"

# With a description
sninja put ./photos --description "Vacation photos"

# Custom validity (1 day = 86400 seconds)
sninja put ./docs --validity 86400
```

### Share Directly (direct)

Share files straight from this machine over P2P. Nothing is uploaded: the
command keeps running and serves the files to whoever opens the link, and the
share goes offline when it exits.

```bash
sninja direct [--description] [--validity] [--ttl] path(s)
```

Path semantics are the same as for `put`.

**Options:**
- `--description <text>` - Description for the share
- `--validity <seconds>` - Link validity in seconds (default: 604800 = 7 days)
- `--ttl <seconds>` - Stop serving and exit after this many seconds (default: until Ctrl+C)

**Examples:**

```bash
# Serve a folder until Ctrl+C
sninja direct ./my-folder

# Serve for ten minutes, then exit
sninja direct ./my-folder --ttl 600
```

### Download Files (get)

Download from a share URL:

```bash
sninja get [--filter] [--output] [--key] [--force] link_url
```

**Options:**
- `--filter <pattern>` - Filter pattern for files to download
- `--output <dir>` - Destination directory
- `--key <base64>` - Encryption key (extracted from URL fragment if present)
- `--force` - Overwrite existing files without prompting

**Examples:**

```bash
# Full URL with encryption key in fragment
sninja get "https://share.ninja/s/abc123#encryptionkey"

# Just the token (key provided separately)
sninja get abc123 --key "base64encodedkey"

# Specify output directory
sninja get "https://share.ninja/s/abc123#key" --output ~/Downloads/myfiles

# Overwrite existing files without prompting
sninja get "https://share.ninja/s/abc123#key" --force
```

### List Files in Share (list)

List files without downloading:

```bash
sninja list [OPTIONS] link_url
```

**Options:**
- `--key <base64>` - Encryption key (extracted from URL fragment if present)
- `--tree` - Output as tree view (default)
- `--table` - Output as flat table
- `--json` - Output as JSON
- `--non-recursive` - Show only top-level items
- `--path <path>` - Filter to items under a specific path

**Examples:**

```bash
# List with tree view (default)
sninja list "https://share.ninja/s/abc123#key"

# List as table
sninja list abc123 --key "key" --table

# JSON output
sninja list abc123 --key "key" --json

# List only top-level items
sninja list "https://share.ninja/s/abc123#key" --non-recursive

# List items under a specific path
sninja list "https://share.ninja/s/abc123#key" --path "subfolder"
```

### Commander (cmd)

MC-like dual-pane file manager: local filesystem ⇄ Share.Ninja shares, built
on `share-ninja-commander`. Shares are read-only; copies go share → local or
local → local. F1 lists the keys.

```bash
sninja cmd                                   # local ⇄ local
sninja cmd "https://share.ninja/#CLAIM"      # share in the right pane
sninja cmd --log-path /tmp/nc.log --debug-api
```

**Options:**
- `--log-path <file>` - Append the log to this file (the terminal is the TUI; nothing goes to stderr)
- `--debug-api` - Log Share.Ninja API requests/responses (same as the global `--verbose`)

### Configuration

Configure default settings:

```bash
# Set a custom API base URL (e.g. a test backend)
sninja configure set --base-url https://test.example.com

# Show current configuration
sninja configure show
```

Configuration is stored in `~/.config/share-ninja/config.toml`.

### API keys (keys)

Long-lived bearer credentials for programmatic access to your account
(`Authorization: Bearer <token>`). Needs a logged-in account (`sninja auth
login`); a key cannot manage keys.

```bash
# Create a key — the token is printed once and never shown again
sninja keys create "CI pipeline"
sninja keys create "Read-only bot" --scope link:read --expires 30d
sninja keys create "Export" --expires 2026-12-31 --json

# List live keys (told apart by their prefix; the secret is never listed)
sninja keys list

# Revoke by key id or by prefix
sninja keys revoke key_018f7b2e-3c4d-7a1b-9e6f-2a5c8d1e4b7a
sninja keys revoke c6n9rQ2e
```

**Options (create):**
- `--scope <SCOPE>` - Limit the key to a scope (repeatable); none = the account's full rights. Unknown scopes are refused by the server (`invalid_scopes`)
- `--expires <WHEN>` - A duration from now (`30d`, `12h`, `2w`) or a date (`YYYY-MM-DD`); default never
- `--json` - Print the created key as JSON, token included

### Secrets file (containers, headless hosts)

The device secrets (device key + refresh token) live in the system keychain.
On a host without one, keep them in a file instead:

```bash
sninja --secrets-file /var/lib/sninja/secrets.json auth login
SNINJA_SECRETS_FILE=/var/lib/sninja/secrets.json sninja links
```

The file must stay writable (the refresh token rotates on every grant) and
is written `0600` — treat it like an SSH private key. Per run, never saved
to the config.

## Options

```
Options:
      --base-url <URL>       Server origin (/api and /ws are appended), overrides the
                             configured one for this run
                             [env: NINJA_BASE_URL] [default: https://share.ninja]
      --secrets-file <PATH>  Keep the device secrets (key + refresh token) in this
                             file instead of the system keychain — for containers
                             and hosts without one. Must be writable; written 0600
                             [env: SNINJA_SECRETS_FILE]
  -v, --verbose              Enable verbose logging
      --debug                Enable debug logging (more detailed than verbose)
  -h, --help                 Print help
  -V, --version              Print version
```

## Environment Variables

- `NINJA_BASE_URL` - server origin, /api and /ws appended (default: https://share.ninja)
- `SNINJA_SECRETS_FILE` - keep the device secrets in this file instead of the system keychain

## Examples

```bash
# Upload a folder to cloud storage
sninja put ./my-folder

# Serve a folder directly from this machine (P2P)
sninja direct ./my-folder

# Download a shared folder
sninja get "https://share.ninja/s/xyz789#secretkey" -o ./my-download

# List contents first, then download
sninja list "https://share.ninja/s/xyz789#secretkey"
sninja get "https://share.ninja/s/xyz789#secretkey"

# Verbose mode for debugging
sninja -v get "https://share.ninja/s/abc123#key"
```

## Architecture

The CLI is built on top of `share_ninja_core` library which provides:

- **Backend API client** (`Back`) - Authentication and transfer coordination
- **Transfer** - High-level transfer orchestration (source/dest connection, stats, timer)
- **TransferItem** - Single item transfer logic between sources and destinations
- **S3 Storage** - S3 over backend-presigned URLs (no S3 SDK)
- **P2P Storage** - WebRTC-based peer-to-peer transfers
- **MultiSource** - Combines multiple sources for redundancy
- **NinjaStorageV2** - Encrypted storage with journaling and zip batching

### Transfer Flow

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Transfer  │────▶│ TransferItem │────▶│   Source    │
│             │     │              │     │ (S3/P2P/FS) │
│ - connect   │     │ - start()    │     └─────────────┘
│ - stats     │     │ - transfer   │            │
│ - timer     │     │   items      │            ▼
└─────────────┘     └──────────────┘     ┌─────────────┐
                                         │    Dest     │
                                         │ (S3/FS/Zip) │
                                         └─────────────┘
```

### P2P DataChannel Architecture

For P2P transfers, the CLI uses per-request DataChannels:

- **DataChannelWrapper** - Encapsulates RTCDataChannel with send/recv queues
- **Channel Pooling** - Reuses channels for efficiency (max 5)
- **AES-256-GCM Encryption** - End-to-end encryption
- **FileClient/FileServer** - Protocol handlers for file operations

## Debug Mode

```bash
# Verbose logging
sninja -v get "https://share.ninja/s/abc123#key"

# Full debug logging (includes WebRTC/DTLS)
sninja --debug get "https://share.ninja/s/abc123#key"
```
