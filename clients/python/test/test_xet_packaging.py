import ast
import sys
import unittest
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


CLIENT_ROOT = Path(__file__).resolve().parents[1]


def setup_assignment(name):
    module = ast.parse((CLIENT_ROOT / "setup.py").read_text())
    for node in module.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return ast.literal_eval(node.value)
    raise AssertionError(f"{name} assignment not found")


class TestXETPackaging(unittest.TestCase):
    def test_hf_xet_is_required_and_pinned(self):
        pyproject = tomllib.loads((CLIENT_ROOT / "pyproject.toml").read_text())

        self.assertEqual(pyproject["tool"]["poetry"]["dependencies"]["hf_xet"], "==1.4.3")
        self.assertNotIn("extras", pyproject["tool"]["poetry"])
        self.assertIn("hf_xet == 1.4.3", setup_assignment("REQUIRES"))
        self.assertNotIn("EXTRAS_REQUIRES", (CLIENT_ROOT / "setup.py").read_text())


if __name__ == "__main__":
    unittest.main()
