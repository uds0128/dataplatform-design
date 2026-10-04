import importlib
from typing import Any

class ComponentFactory:
    """Dynamically loads classes based on module path string."""
    @staticmethod
    def create(class_path: str, *args, **kwargs) -> Any:
        try:
            module_name, class_name = class_path.rsplit(".", 1)
            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)
            return cls(*args, **kwargs)
        except (ImportError, AttributeError, ValueError) as e:
            raise ValueError(f"Failed to load class {class_path}: {e}")
