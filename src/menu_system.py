"""Shared menu utilities for the interactive CLI experience."""

from __future__ import annotations

"""Reusable menu infrastructure for the interactive CLI."""

from dataclasses import dataclass
from typing import Callable, List, Optional, Sequence

import os


class MenuStyle:
    """ANSI helpers and layout constants for terminal menus."""

    HEADER = "\033[95m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    END = "\033[0m"

    TOP_LINE = "═" * 60
    MID_LINE = "─" * 60

    @staticmethod
    def clear_screen() -> None:
        os.system("cls" if os.name == "nt" else "clear")


@dataclass
class MenuItem:
    """Representation of a single menu option."""

    key: str
    label: str
    description: str = ""
    icon: str = "•"
    badge: Optional[str] = None
    action: Optional[Callable[[], None]] = None

    def render(self) -> str:
        """Return the formatted string for the menu item."""

        badge_text = (
            f" {MenuStyle.CYAN}({self.badge}){MenuStyle.END}" if self.badge else ""
        )
        description = (
            f"\n      {MenuStyle.BLUE}{self.description}{MenuStyle.END}"
            if self.description
            else ""
        )
        return (
            f"  [{MenuStyle.BOLD}{self.key}{MenuStyle.END}] {self.icon} "
            f"{MenuStyle.BOLD}{self.label}{MenuStyle.END}{badge_text}{description}"
        )


class NavigationContext:
    """Tracks the breadcrumb trail within the menu hierarchy."""

    def __init__(self, root: str = "Main Menu") -> None:
        self.root = root
        self.path: List[str] = [root]

    def push(self, name: str) -> None:
        self.path.append(name)

    def pop(self) -> None:
        if len(self.path) > 1:
            self.path.pop()

    def go_home(self) -> None:
        self.path = [self.root]

    def set_root(self, name: str) -> None:
        self.root = name
        self.path = [name]

    @property
    def breadcrumb(self) -> str:
        return " > ".join(self.path)


class Menu:
    """Base menu class responsible for rendering and input handling."""

    def __init__(
        self,
        title: str,
        context: NavigationContext,
        *,
        intro_lines: Optional[Sequence[str]] = None,
        help_lines: Optional[Sequence[str]] = None,
        show_help_hint: bool = True,
    ) -> None:
        self.title = title
        self.context = context
        self.items: List[Optional[MenuItem]] = []
        self.intro_lines = list(intro_lines or [])
        self.help_lines = list(help_lines or [])
        self.show_help_hint = show_help_hint

    def add_item(
        self,
        key: str,
        label: str,
        description: str,
        *,
        icon: str = "•",
        badge: Optional[str] = None,
        action: Optional[Callable[[], None]] = None,
    ) -> None:
        self.items.append(
            MenuItem(key=key, label=label, description=description, icon=icon, badge=badge, action=action)
        )

    def add_separator(self) -> None:
        self.items.append(None)

    def display(self) -> None:
        MenuStyle.clear_screen()
        print(MenuStyle.TOP_LINE)
        print(f"{MenuStyle.BOLD}{MenuStyle.CYAN}{self.title.upper()}{MenuStyle.END}")
        print(MenuStyle.TOP_LINE)
        breadcrumb = self.context.breadcrumb
        if breadcrumb:
            print(f"{MenuStyle.BLUE}Location: {breadcrumb}{MenuStyle.END}\n")
        for line in self.intro_lines:
            print(line)
        if self.intro_lines:
            print()
        for item in self.items:
            if item is None:
                print(f"  {MenuStyle.MID_LINE}")
            else:
                print(item.render())
        print()
        print(MenuStyle.MID_LINE)
        shortcuts = [f"{MenuStyle.YELLOW}[H]{MenuStyle.END} Home", f"{MenuStyle.YELLOW}[Q]{MenuStyle.END} Quit"]
        if len(self.context.path) > 1:
            shortcuts.insert(0, f"{MenuStyle.YELLOW}[B]{MenuStyle.END} Back")
        if self.show_help_hint and self.help_lines:
            shortcuts.append(f"{MenuStyle.YELLOW}[?]{MenuStyle.END} Help")
        print("  " + " | ".join(shortcuts))
        print(MenuStyle.MID_LINE)

    def get_choice(self, prompt: str = "Choice") -> str:
        return input(f"\n{MenuStyle.GREEN}▶{MenuStyle.END} {prompt}: ").strip()

    def show(self) -> str:
        self.display()
        selection = self.get_choice()
        lowered = selection.lower()
        if lowered == "b" and len(self.context.path) > 1:
            return "back"
        if lowered == "h":
            return "home"
        if lowered == "q":
            return "quit"
        if lowered == "?":
            return "help"
        return selection

    def display_help(self) -> None:
        if not self.help_lines:
            print(f"\n{MenuStyle.BLUE}No additional help available.{MenuStyle.END}")
        else:
            print(f"\n{MenuStyle.CYAN}{MenuStyle.BOLD}Help{MenuStyle.END}")
            print(MenuStyle.MID_LINE)
            for line in self.help_lines:
                print(line)
        input(f"\n{MenuStyle.GREEN}Press Enter to continue...{MenuStyle.END}")


class ConfirmationPrompt:
    """Utility helpers for richer confirmation flows."""

    @staticmethod
    def confirm(
        message: str,
        *,
        default: bool = False,
        details: Optional[Sequence[str]] = None,
        warning: bool = False,
    ) -> bool:
        if warning:
            print(f"\n{MenuStyle.RED}{MenuStyle.BOLD}⚠️  WARNING{MenuStyle.END}")
        print(f"\n{MenuStyle.BOLD}{message}{MenuStyle.END}")
        if details:
            for line in details:
                print(f"{MenuStyle.BLUE}{line}{MenuStyle.END}")
        prompt = "[Y/n]" if default else "[y/N]"
        if warning:
            prompt = f"{MenuStyle.RED}{prompt}{MenuStyle.END}"
        response = input(
            f"\n{MenuStyle.GREEN}▶{MenuStyle.END} Confirm {prompt}: "
        ).strip().lower()
        if not response:
            return default
        return response in {"y", "yes"}

    @staticmethod
    def select_option(
        message: str,
        options: Sequence[tuple[str, str]],
        *,
        allow_cancel: bool = True,
    ) -> Optional[str]:
        print(f"\n{MenuStyle.BOLD}{message}{MenuStyle.END}\n")
        for index, (_, label) in enumerate(options, start=1):
            print(f"  [{index}] {label}")
        if allow_cancel:
            print(f"  [{MenuStyle.YELLOW}0{MenuStyle.END}] Cancel")
        choice = input(f"\n{MenuStyle.GREEN}▶{MenuStyle.END} Select: ").strip()
        if allow_cancel and choice == "0":
            return None
        try:
            position = int(choice) - 1
        except ValueError:
            return None
        if 0 <= position < len(options):
            return options[position][0]
        return None


class ProgressIndicator:
    """Simple progress renderer for long-running operations."""

    BAR_LENGTH = 40

    @staticmethod
    def render(channel: str, current: int, total: int, messages: int, coords: int) -> None:
        progress = 0 if total == 0 else current / total
        filled = int(progress * ProgressIndicator.BAR_LENGTH)
        bar = "█" * filled + "░" * (ProgressIndicator.BAR_LENGTH - filled)
        percentage = progress * 100
        print(
            f"\r{MenuStyle.CYAN}[{current}/{total}]{MenuStyle.END} {bar} "
            f"{percentage:5.1f}% | {MenuStyle.BOLD}{channel}{MenuStyle.END} | "
            f"{MenuStyle.GREEN}{messages} msgs{MenuStyle.END}, "
            f"{MenuStyle.YELLOW}{coords} coords{MenuStyle.END}",
            end="",
        )

    @staticmethod
    def finish() -> None:
        print()
