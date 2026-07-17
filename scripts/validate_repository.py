"""Credential-free repository validation used locally and in normal CI."""

import ast
import re
import sys
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXCLUDED_PARTS = {
    ".git",
    ".terraform",
    ".venv",
    ".venv-dbt",
    ".venv-dbt2",
    ".venv-v12",
    "target",
    "logs",
    "data",
    "__pycache__",
}
LINK_PATTERN = re.compile(r"!?(?:\[[^\]]*\])\(([^)]+)\)")
SECRET_PATTERNS = {
    "private key": re.compile(r"-----BEGIN (?:RSA |EC )?PRIVATE KEY-----"),
    "Google API key": re.compile(r"AIza[0-9A-Za-z_-]{35}"),
    "AWS access key": re.compile(r"AKIA[0-9A-Z]{16}"),
}


def repository_files(suffixes: set[str]) -> list[Path]:
    return [
        path
        for path in PROJECT_ROOT.rglob("*")
        if path.is_file()
        and path.suffix.lower() in suffixes
        and not EXCLUDED_PARTS.intersection(path.relative_to(PROJECT_ROOT).parts)
    ]


def validate_python() -> list[str]:
    errors = []
    for path in repository_files({".py"}):
        try:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (SyntaxError, UnicodeDecodeError) as exc:
            errors.append(f"Python syntax: {path.relative_to(PROJECT_ROOT)}: {exc}")
    return errors


def validate_yaml() -> list[str]:
    errors = []
    for path in repository_files({".yml", ".yaml"}):
        try:
            list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
        except yaml.YAMLError as exc:
            errors.append(f"YAML syntax: {path.relative_to(PROJECT_ROOT)}: {exc}")
    return errors


def validate_markdown_links() -> list[str]:
    errors = []
    for path in repository_files({".md"}):
        content = path.read_text(encoding="utf-8")
        for target in LINK_PATTERN.findall(content):
            target = target.strip().split("#", 1)[0]
            if not target or target.startswith(("http://", "https://", "mailto:")):
                continue
            resolved = (path.parent / target).resolve()
            if not resolved.exists():
                errors.append(
                    f"Markdown link: {path.relative_to(PROJECT_ROOT)} -> {target}"
                )
    return errors


def validate_no_embedded_secrets() -> list[str]:
    errors = []
    text_suffixes = {
        ".py", ".sql", ".yml", ".yaml", ".md", ".tf", ".txt", ".example"
    }
    for path in repository_files(text_suffixes):
        content = path.read_text(encoding="utf-8", errors="ignore")
        for label, pattern in SECRET_PATTERNS.items():
            if pattern.search(content):
                errors.append(f"Potential {label}: {path.relative_to(PROJECT_ROOT)}")
    return errors


def main() -> None:
    errors = [
        *validate_python(),
        *validate_yaml(),
        *validate_markdown_links(),
        *validate_no_embedded_secrets(),
    ]
    if errors:
        print("REPOSITORY VALIDATION FAILED")
        for error in errors:
            print(f"- {error}")
        raise SystemExit(1)
    print("REPOSITORY VALIDATION PASSED: Python, YAML, links, secret patterns")


if __name__ == "__main__":
    main()
