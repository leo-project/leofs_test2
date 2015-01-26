.PHONY: deps compile exec clean xref eunit

REBAR := ./rebar

all: deps compile
	@$(REBAR) skip_deps=true escriptize

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
