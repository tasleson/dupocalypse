# Environment Variables

This document describes the environment variables that can be used to configure dupocalypse.

## Archive Location

### `DUPOCALYPSE_DIR`

Specifies the default archive directory location. This can be overridden using the `--archive/-a` command line argument.

* Default: None (must be specified either via environment variable or command line argument)
* Used by: All commands except `create` (which requires explicit directory specification)
* Example: `DUPOCALYPSE_DIR=/path/to/archive dupocalypse list`

## Performance Tuning

### `DUPOCALYPSE_DECOMPRESS_BUFF_SIZE_MB`

Controls the buffer size used for decompression operations when reading compressed slab files.

* Default: `4` (4 megabytes)
* Unit: Megabytes
* Used by: Slab file reading operations when decompression is enabled
* Example: `DUPOCALYPSE_DECOMPRESS_BUFF_SIZE_MB=8 dupocalypse unpack -a /path/to/archive -s mystream`
