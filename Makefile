.PHONY: deps compile exec clean xref eunit ansible

REBAR := ./rebar

all: deps compile ansible
	@$(REBAR) skip_deps=true escriptize

ansible:
	@if [ ! -d leofs_ansible ]; then git clone https://github.com/leo-project/leofs_ansible.git; fi
	@cp ${ANSIBLE_KEY} leofs_ansible/ansible_key
	@(cd leofs_ansible;chmod 600 ansible_key;cp ../ansible/* .)

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
