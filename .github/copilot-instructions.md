# GitHub Copilot Instructions for Zig Project

## Language and Version
- Target Zig version: `0.15.2` (or latest stable)
- Follow the official Zig Style Guide and idiomatic practices.

## Coding Standards & Idioms
- **Memory Management:** Always prefer explicit allocators (`std.mem.Allocator`). Use `defer allocator.free(ptr)` or `defer someObject.deinit()` immediately after allocation.
- **Error Handling:** Use `!T` return types and `try` / `catch` / `if (error)` rather than panic, unless in a `main` initialization phase.
- **Null Safety:** Prefer `?T` (optional) over raw pointers and use `orelse` or `if (ptr) |p|` for unwrapping.
- **Documentation:** Use `///` for doc comments on public declarations.

## Project Structure & Build
- All code must comply with `build.zig`.
- Prefer `std.debug.assert` for safety checks that should be removed in `-Drelease-fast`.
- Ensure proper build artifact generation (executable/library) in `build.zig`.

## AI Interaction Guidelines
- If unsure about a memory allocation strategy, suggest the safer, idiomatic Zig pattern.
- Favor clarity over brevity.
- When refactoring, maintain error safety and memory hygiene.
