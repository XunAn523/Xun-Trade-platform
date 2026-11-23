"""Immutable, acyclic Consumer cursor stream-generation registry."""

from __future__ import annotations

from dataclasses import dataclass

from ..domain._validation import DomainValidationError, require_positive, require_text


class CursorStreamRegistryError(DomainValidationError):
    """A cursor stream registry or reset context is unsafe to use."""


@dataclass(frozen=True, order=True)
class CursorStreamRefV2:
    """One account Journal generation addressed without display cursor text."""

    journal_id: str
    stream_generation: int

    def __post_init__(self) -> None:
        require_text(self.journal_id, "cursor stream journal_id")
        require_positive(self.stream_generation, "cursor stream generation")


@dataclass(frozen=True)
class CursorStreamResetV2:
    """One durable old-stream → successor edge and its opaque reset context."""

    reset_context: str
    source: CursorStreamRefV2
    target: CursorStreamRefV2

    def __post_init__(self) -> None:
        require_text(self.reset_context, "cursor reset_context", max_length=128)
        if self.source == self.target:
            raise CursorStreamRegistryError("cursor reset edge cannot target itself")


@dataclass(frozen=True)
class CursorStreamRegistryV2:
    """Verified per-account generation graph with exactly one current tail."""

    current: CursorStreamRefV2
    streams: tuple[CursorStreamRefV2, ...]
    resets: tuple[CursorStreamResetV2, ...] = ()
    schema_version: int = 1

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise CursorStreamRegistryError("cursor stream registry schema is unsupported")
        streams = tuple(self.streams)
        resets = tuple(self.resets)
        if not streams or self.current not in streams:
            raise CursorStreamRegistryError("cursor stream registry lacks current generation")
        if len(set(streams)) != len(streams):
            raise CursorStreamRegistryError("cursor stream registry repeats a generation")
        contexts = tuple(reset.reset_context for reset in resets)
        if len(set(contexts)) != len(contexts):
            raise CursorStreamRegistryError("cursor stream registry repeats reset context")
        sources = tuple(reset.source for reset in resets)
        if len(set(sources)) != len(sources):
            raise CursorStreamRegistryError("cursor stream registry branches a source generation")
        if any(reset.source not in streams or reset.target not in streams for reset in resets):
            raise CursorStreamRegistryError("cursor stream registry has a dangling reset edge")
        successor = {reset.source: reset.target for reset in resets}
        predecessor = {reset.target for reset in resets}
        roots = tuple(stream for stream in streams if stream not in predecessor)
        if len(roots) != 1:
            raise CursorStreamRegistryError("cursor stream registry must have one root generation")
        if self.current in successor:
            raise CursorStreamRegistryError(
                "cursor stream registry current generation has a successor"
            )
        visited: set[CursorStreamRefV2] = set()
        node = roots[0]
        while node not in visited:
            visited.add(node)
            next_node = successor.get(node)
            if next_node is None:
                break
            node = next_node
        if node != self.current or len(visited) != len(streams):
            raise CursorStreamRegistryError("cursor stream registry is cyclic or disconnected")
        object.__setattr__(self, "streams", streams)
        object.__setattr__(self, "resets", resets)

    def resolve_reset_context(self, *, source: CursorStreamRefV2) -> tuple[str, ...]:
        """Return the only registered context chain from source to current."""

        if source not in self.streams:
            raise CursorStreamRegistryError("cursor source generation is not registered")
        by_source = {reset.source: reset for reset in self.resets}
        contexts: list[str] = []
        node = source
        while node != self.current:
            reset = by_source.get(node)
            if reset is None:
                raise CursorStreamRegistryError("cursor source has no path to current generation")
            contexts.append(reset.reset_context)
            node = reset.target
        return tuple(contexts)

    def require_context(
        self,
        *,
        source: CursorStreamRefV2,
        reset_context: str,
    ) -> CursorStreamRefV2:
        """Validate a supplied first reset context and return its immediate target."""

        require_text(reset_context, "cursor reset_context", max_length=128)
        reset = next(
            (
                item
                for item in self.resets
                if item.source == source and item.reset_context == reset_context
            ),
            None,
        )
        if reset is None:
            raise CursorStreamRegistryError("cursor reset_context does not match source generation")
        return reset.target
