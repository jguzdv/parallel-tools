# Stub headers

Enough of `<lustre/lustreapi.h>` to **compile** `lustre-migrate-file.c` and to
build and run `test_resolve` on a machine without `lustre-client-devel`.

It does not link against a real `liblustreapi` and it must never be used to
claim the program works — only that it still builds and that the offline
tests, which never call into Lustre, still pass.

```bash
cc -std=gnu99 -Wall -Wextra -Wno-unused-parameter -Wconversion -Wshadow \
   -fsyntax-only -include stub/darwin_shim.h -Istub lustre-migrate-file.c

cc -O1 -std=gnu99 -Wall -Wextra -Wno-unused-parameter -Wno-unused-function \
   -include stub/darwin_shim.h -Istub -I. -o test_resolve test_resolve.c
```

`darwin_shim.h` only renames `st_atim`/`st_mtim`, which macOS spells
`st_atimespec`/`st_mtimespec`. On Linux it does nothing.

When the signatures here disagree with a real header, the real one is right:
these were reconstructed from the call sites, not copied.
