"""Phase 3 verification: HTML structural sanity (tag balance) for edited pages.

A tolerant stack-based parse using html.parser: reports elements that are
closed without being opened, or left unclosed at EOF (excluding voids and
implicitly-closed table elements). Purely structural; not a full validator.
"""

from __future__ import annotations

from html.parser import HTMLParser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

VOID = {
    "area", "base", "br", "col", "embed", "hr", "img", "input", "link",
    "meta", "param", "source", "track", "wbr",
}
IMPLIED_CLOSE = {"p", "li", "td", "th", "tr", "option", "dt", "dd"}


class BalanceChecker(HTMLParser):
    """Track open elements and report structural anomalies."""

    def __init__(self, name: str) -> None:
        super().__init__(convert_charrefs=False)
        self.name = name
        self.stack: list[str] = []
        self.errors: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in VOID:
            return
        self.stack.append(tag)

    def handle_endtag(self, tag: str) -> None:
        if tag in VOID:
            return
        if not self.stack:
            self.errors.append(f"unexpected </{tag}> at line {self.getpos()[0]}")
            return
        if self.stack[-1] == tag:
            self.stack.pop()
            return
        if tag in IMPLIED_CLOSE and tag in self.stack:
            while self.stack and self.stack[-1] != tag:
                self.errors.append(
                    f"implicitly closed <{self.stack[-1]}> by </{tag}> at line {self.getpos()[0]}"
                )
                self.stack.pop()
            if self.stack:
                self.stack.pop()
            return
        self.errors.append(
            f"mismatched </{tag}> (open: <{self.stack[-1]}>) at line {self.getpos()[0]}"
        )

    def finish(self) -> list[str]:
        for leftover in self.stack:
            self.errors.append(f"unclosed <{leftover}>")
        return self.errors


def check(path: Path) -> int:
    """Parse one page and print structural findings."""
    parser = BalanceChecker(path.name)
    parser.feed(path.read_text(encoding="utf-8", errors="replace"))
    errors = parser.finish()
    print(f"[{path.name}] structural findings: {len(errors)}")
    for message in errors[:20]:
        print(f"  {message}")
    return len(errors)


def main() -> int:
    """Check all Phase 3 edited pages."""
    pages = [ROOT / "index.html", ROOT / "report.html", ROOT / "legal.html"]
    total = sum(check(page) for page in pages)
    print("RESULT:", "FAIL" if total else "PASS — no structural anomalies")
    return 1 if total else 0


if __name__ == "__main__":
    raise SystemExit(main())
