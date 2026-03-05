from metaflow.extension_support.plugins import merge_lists, process_plugins, resolve_plugins

# CLI commands provided by this extension
CLIS_DESC = [
    ("gha", ".gha_cli.cli"),
]

# Step decorators provided by this extension
STEP_DECORATORS_DESC = [
    ("gha", ".gha_decorator.GHADecorator"),
]

# No flow decorators
FLOW_DECORATORS_DESC = []

__mf_extensions__ = "gha"
