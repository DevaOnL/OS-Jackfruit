all:
	$(MAKE) -C boilerplate

ci:
	$(MAKE) -C boilerplate ci

clean:
	$(MAKE) -C boilerplate clean

.PHONY: all ci clean
