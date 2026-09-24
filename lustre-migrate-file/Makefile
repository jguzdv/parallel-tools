# lustre-migrate-file
#
#   make            build the program (needs lustre-client-devel)
#   make test       build and run the offline tests (no Lustre needed)
#   make install    install into $(PREFIX)/bin
#
# The offline tests cover path resolution, OST parsing and base64 - the parts
# that do not talk to Lustre. Everything that does has to be tested on a real
# filesystem; see README.md.

PREFIX  ?= /usr/local
CC      ?= cc
CFLAGS  ?= -O2 -g -std=gnu99 -Wall -Wextra -Wno-unused-parameter \
           -D_FORTIFY_SOURCE=2 -fstack-protector-strong
LDLIBS  ?= -llustreapi

PROG    := lustre-migrate-file
SRC     := lustre-migrate-file.c
TEST    := test_resolve
TESTSRC := test_resolve.c

# Set STUBDIR to a directory holding a stub lustre/lustreapi.h to run the
# offline tests on a machine without the Lustre client headers.
STUBDIR ?=
ifneq ($(STUBDIR),)
TESTINC := -I$(STUBDIR)
endif

.PHONY: all test clean install check-headers

all: $(PROG)

$(PROG): $(SRC)
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)

check-headers:
	@echo '#include <lustre/lustreapi.h>' > .hdrtest.c
	@echo 'int main(void){return 0;}' >> .hdrtest.c
	@$(CC) -fsyntax-only .hdrtest.c 2>/dev/null \
		&& echo "lustre/lustreapi.h found" \
		|| echo "lustre/lustreapi.h NOT found - install lustre-client-devel"
	@rm -f .hdrtest.c

test: $(TEST)
	./$(TEST)

$(TEST): $(TESTSRC) $(SRC)
	$(CC) $(CFLAGS) -Wno-unused-function $(TESTINC) -I. -o $@ $(TESTSRC)

install: $(PROG)
	install -d $(DESTDIR)$(PREFIX)/bin
	install -m 0755 $(PROG) $(DESTDIR)$(PREFIX)/bin/$(PROG)

clean:
	rm -f $(PROG) $(TEST) .hdrtest.c
	rm -rf $(PROG).dSYM $(TEST).dSYM
