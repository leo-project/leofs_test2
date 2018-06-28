.PHONY: deps compile exec clean xref eunit

REBAR = $(shell pwd)/rebar3

all: deps compile
	@$(REBAR) escriptize

deps:
	@$(REBAR) get-deps

compile:
	@$(REBAR) compile

clean:
	@$(REBAR) clean

xref:
	@$(REBAR) xref skip_deps=true

distclean:
	@$(REBAR) delete-deps
	@$(REBAR) clean
