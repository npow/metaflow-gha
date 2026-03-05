"""
gha_decorator.py

@gha step decorator — runs this step on a GitHub Actions worker VM.

Usage:
    @gha
    @step
    def my_step(self):
        ...

Or applied flow-wide via CLI:
    python flow.py run --with=gha
"""
from __future__ import annotations

import sys

from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException

# Decorators that are incompatible with @gha (each uses its own compute backend)
_INCOMPATIBLE_DECORATORS = {
    "batch", "kubernetes", "argo_workflows_internal",
    "schedule", "trigger", "trigger_on_finish",
}

# GHA-hosted runner hard limits
_GHA_MAX_CPU = 4
_GHA_MAX_MEMORY_MB = 16384  # 16 GB


class GHAException(MetaflowException):
    headline = "GHA decorator error"


class GHADecorator(StepDecorator):
    """
    Specifies that this step should execute on a GitHub Actions runner VM.

    Parameters
    ----------
    workers : int, default 20
        Maximum number of parallel GHA job runners to spin up for this run.
        Capped at 20 (GitHub's concurrency limit for standard runners).
    timeout : int, default 21600
        Maximum wall-clock seconds for a single task (default: 6 hours, GHA max).
    max_retries : int, default 2
        Number of times to retry a failed task before marking it as permanently failed.
    """

    name = "gha"
    defaults = {
        "workers": 20,
        "timeout": 21600,
        "max_retries": 2,
    }

    # Set by runtime_init — shared across all instances of this decorator
    package_url: str | None = None
    package_sha: str | None = None
    package_metadata: str | None = None

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        if flow_datastore.TYPE != "s3":
            raise GHAException("@gha requires --datastore=s3.")

        # Block incompatible decorators
        for deco in decos:
            if deco.name in _INCOMPATIBLE_DECORATORS:
                raise GHAException(
                    f"@gha is incompatible with @{deco.name} on step '{step}'."
                )

        # Warn if @resources exceeds GHA limits
        for deco in decos:
            if deco.name == "resources":
                cpu = int(deco.attributes.get("cpu") or 1)
                mem = int(deco.attributes.get("memory") or 0)
                if cpu > _GHA_MAX_CPU:
                    logger(
                        f"WARNING: @resources(cpu={cpu}) on step '{step}' exceeds "
                        f"GitHub Actions runner limit of {_GHA_MAX_CPU} CPUs.",
                        bad=True,
                    )
                if mem > _GHA_MAX_MEMORY_MB:
                    logger(
                        f"WARNING: @resources(memory={mem}) on step '{step}' exceeds "
                        f"GitHub Actions runner limit of {_GHA_MAX_MEMORY_MB} MB.",
                        bad=True,
                    )

        self.logger = logger
        self.environment = environment
        self.step = step
        self.flow_datastore = flow_datastore

    def runtime_init(self, flow, graph, package, run_id):
        self.flow = flow
        self.graph = graph
        self.package = package
        self.run_id = run_id

    def runtime_task_created(
        self, task_datastore, task_id, split_index, input_paths, is_cloned, ubf_context
    ):
        if not is_cloned:
            self._save_package_once(self.flow_datastore, self.package)

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        if retry_count <= max_user_code_retries:
            # Route execution through `gha step` subcommand.
            # cli_args.command_args[0] is already the step name (set by runtime).
            # cli_args.command_options already has run-id, task-id, input-paths,
            # split-index, retry-count, max-user-code-retries, tag, namespace,
            # ubf-context (all set by MetaflowRuntime.default_args before this call).
            cli_args.commands = ["gha", "step"]
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)
            cli_args.command_options.update(
                {
                    "flow-name": self.flow.name,
                    "workers": self.attributes["workers"],
                    "timeout": self.attributes["timeout"],
                    "max-retries": self.attributes["max_retries"],
                }
            )
            cli_args.entrypoint[0] = sys.executable

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_retries,
        ubf_context,
        inputs,
    ):
        self.metadata = metadata
        self.task_datastore = task_datastore

    @classmethod
    def _save_package_once(cls, flow_datastore, package):
        if cls.package_url is None:
            if hasattr(package, "package_url"):
                # Pre-uploaded package path (FEAT_ALWAYS_UPLOAD_CODE_PACKAGE)
                cls.package_url = package.package_url()
                cls.package_sha = package.package_sha()
                cls.package_metadata = package.package_metadata
            else:
                cls.package_url, cls.package_sha = flow_datastore.save_data(
                    [package.blob], len_hint=1
                )[0]
                cls.package_metadata = package.package_metadata
