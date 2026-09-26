/* Only so the file compiles on macOS. Linux names these st_atim/st_mtim. */
#ifndef DARWIN_SHIM_H
#define DARWIN_SHIM_H
#if defined(__APPLE__)
#include <sys/stat.h>
#define st_atim st_atimespec
#define st_mtim st_mtimespec
#define st_ctim st_ctimespec
#endif
#endif
