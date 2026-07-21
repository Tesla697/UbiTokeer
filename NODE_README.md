# UbiTokeer Donor Node

Run this on a **donor's PC** so people can generate tokens from their Ubisoft
account for a specific game — **without the donor ever sharing their account**.

The credentials and DenuvoTicket stay entirely on the donor's machine. This client
connects *outbound* to the UbiTokeer backend (works behind a home router, no
port-forwarding), waits for a job, runs DenuvoTicket locally, and uploads only the
finished tokens back.

While it's running, the donor's game shows **in stock**. Close it / turn the PC
off, and the game automatically shows **out of stock** — nobody is left waiting.

> Tokens are minted **live per request** (each one is bound to the requester's
> D-Report), so the PC has to be **on and this client running** for the game to be
> available. It can't pre-generate a batch and go offline.

---

## Backend setup (we do this once)

1. In `config.json`, add the donor's node under `nodes`:
   ```json
   "nodes": {
     "pragmata-donor": { "key": "<a long random secret>" }
   }
   ```
2. In `accounts.json`, add a **remote** account for the game the donor serves:
   ```json
   {
     "email": "pragmata-donor",
     "accid": "",
     "folder": "",
     "uplay_ids": ["3357650"],
     "track_quota": false,
     "remote": true,
     "node_id": "pragmata-donor"
   }
   ```
   - `remote: true` + `node_id` route this game to the donor's PC.
   - `track_quota: false` = unlimited (no daily cap).
   - `uplay_ids` = the game(s) this donor serves.
3. Restart the backend. Give the donor `UbiTokeerNode.exe`, a `node_config.json`,
   and their `key`.

## Donor setup (they do this once)

1. Sign their Ubisoft account into `DenuvoTicket.exe` once, so a login session is
   saved (the same working setup used to generate tokens normally).
2. Copy `node_config.example.json` to `node_config.json` next to the exe and fill:
   - `backend_url` — e.g. `https://api.luastools.xyz`
   - `node_id` and `key` — the values we gave them
   - `folder` — the folder containing their `DenuvoTicket.exe` + login
   - `accid` — leave blank unless we tell them otherwise
   - `uplay_ids` — the game id(s) they're donating
3. Run `UbiTokeerNode.exe`. Leave it running whenever they're happy to donate.

That's it — when someone opens a ticket for that game, the token is generated on
the donor's PC and delivered like any other.

## Linux donor (native, no Wine)

DenuvoTicket is .NET 8, so it runs natively on Linux — use the **`DenuvoTicket-ubuntu-latest`** build (native `./DenuvoTicket` ELF), not the Windows one. The node auto-selects a pexpect PTY driver on Linux (`posix_cli_worker.py`) instead of winpty.

Donor setup:
1. Install the .NET 8 runtime + Python bits:
   ```
   sudo apt install dotnet-runtime-8.0 python3 python3-pip
   pip3 install pexpect requests
   # if zstd errors at runtime: sudo apt install libzstd-dev
   ```
2. Get the Linux DenuvoTicket build:
   `https://nightly.link/UplayDB/UplayApps/workflows/BUILD_DenuvoTicket/main/DenuvoTicket-ubuntu-latest.zip`
   then `chmod +x DenuvoTicket`.
3. Sign in once so a session is saved:
   `./DenuvoTicket -remember-me -remember-device -accid <a-fixed-guid> -usefilestore`
   (use the SAME guid in `node_config.json`'s `accid` so the device stays remembered).
4. Copy **`node_client.py`** + **`posix_cli_worker.py`** + **`node_config.json`** into one folder (the Linux node needs those two .py files only — not the `core/` package), point `folder` at the DenuvoTicket dir, and run:
   `python3 node_client.py`

> Known caveat: the ubuntu CI build bundles the *Windows* `x64/libzstd.dll` + `lzham.dll` (no `.so`). zstd falls back to the system `libzstd.so`; LZHAM has no Linux lib — if token generation ever errors on `lzham`, that path needs a compiled `liblzham.so`. Confirm with one manual `./DenuvoTicket` run before relying on it.

## Build the exe (from this repo, on Windows)

```
pip install -r requirements.txt pyinstaller
pyinstaller --noconfirm --clean node_client.spec
```
Output: `dist/UbiTokeerNode.exe`.

## Security notes

- The node authenticates with its **own** key, never the backend's master
  `api_key`, so a donor can only serve jobs — they can't drain the pool, read
  other accounts, or cancel other people's jobs.
- Only the finished tokens leave the donor's PC. The requester's `token_req` comes
  in; the account/login never goes out.
