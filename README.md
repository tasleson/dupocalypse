<img width="1536" height="1024" alt="dupocalypse" src="https://github.com/user-attachments/assets/b05b30c4-be40-4694-88cf-da52d1ce4003" />

```
Duplicate data meets its doomsday :-)
```


## Introduction

Welcome to **dupocalypse**, your last line of defense against the rising tide of duplicate data.
**This is a spin of [blk-archive](https://github.com/device-mapper-utils/blk-archive).  Think of it as blk-archive, but with a different direction on features.**  It has a different name to hopefully prevent confusion too.

This Rust-powered utility hunts down redundant blocks across files and block devices, then mercilessly deduplicates and compresses them into a tidy archive directory.

It’s not a full-blown backup system — more like a prepper bunker for your data.
You’ll still want to sync your archive to the cloud, a tape robot, or a USB drive buried in your backyard.

It works especially well with the **device-mapper thin provisioning** target:

- **Snapshot survival** — archive live data without downtime (assuming your're using snapshots)
- **Hole awareness** — skips unprovisioned regions of thin devices.
- **Delta wizardry** — computes differences between thin devices so you only process changed blocks.
- **Efficient resurrection** — restores to thin devices while maximizing data sharing (great for snapshots).

## Status

**ALPHA** — a.k.a. *bring your hard hat*.

Expect dragons, bugs, maybe lost data, and it might even compile!
Check out [`doc/TODO.md`](doc/TODO.md) for what’s still on fire.

## Documentation

For now, the best way to understand dupocalypse is to read the **Use Cases** document in the `doc/` directory.

All files are Markdown (so they work fine in `less`, `cat`, or your editor of choice),
but they were written in [Obsidian](https://obsidian.md), which gives you a smoother, more immersive experience —
perfect for contemplating your deduplication destiny.

## Building

dupocalypse is written in **Rust**, the language of fearless concurrency and mildly alarming compiler messages.

You’ll need a modern Rust toolchain — install it the easy way with [rustup](https://rustup.rs), or if
you're using a modern enough distro, you'll likely be good to go.

### Build dependencies
Some of the rust libraries have build dependencies which need to be satisfied.

* Fedora/EL9(enable CRB repo): `$ sudo dnf install cargo clang-devel device-mapper-devel`
* ubuntu: `sudo apt-get install cargo libclang-dev libdevmapper-dev libsystemd-dev pkg-config`

Build via the standard *cargo* tool.

> cargo build --release

Do not forget the --release flag, debug builds can be an order of magnitude slower,
ideal for those who enjoy a more contemplative approach to computing.

The executable will be in `target/release/`.
