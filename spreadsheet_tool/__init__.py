from .version import APP_NAME, APP_SLUG, __version__

__all__ = ["APP_NAME", "APP_SLUG", "SpreadsheetApp", "__version__"]


def __getattr__(name: str):
    if name == "SpreadsheetApp":
        from .ui import SpreadsheetApp

        return SpreadsheetApp
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
