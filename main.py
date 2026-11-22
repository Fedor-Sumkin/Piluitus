"""PILUITUS process entry point."""
from runtime.application_process_runtime import ApplicationProcessRuntime


_application_process_runtime = ApplicationProcessRuntime(
    module_namespace=globals()
).create()
globals().update(_application_process_runtime.compatibility_exports())


if __name__ == "__main__":
    _application_process_runtime.run()
