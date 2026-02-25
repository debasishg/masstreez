const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── Library module (exposed to consumers) ───────────────────────────
    const masstree_mod = b.addModule("masstree", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    // ── Static library artifact ─────────────────────────────────────────
    const lib = b.addLibrary(.{
        .name = "masstree",
        .root_module = masstree_mod,
    });
    b.installArtifact(lib);

    // ── Unit tests (inline tests in every src/ module) ──────────────────
    const mod_tests = b.addTest(.{
        .root_module = masstree_mod,
    });
    const run_mod_tests = b.addRunArtifact(mod_tests);

    const test_step = b.step("test", "Run all unit and integration tests");
    test_step.dependOn(&run_mod_tests.step);

    // ── External test files ─────────────────────────────────────────────
    // Phase 1: external test files disabled until Phase 2 tree operations
    // are implemented. All Phase 1 tests are inline in src/ modules.
    //
    // TODO(Phase 2): Re-enable external test files:
    //   tests/key_tests.zig, tests/leaf_tests.zig, tests/interior_tests.zig,
    //   tests/layer_tests.zig, tests/tree_tests.zig, tests/integration_tests.zig

    // ── Benchmarks ──────────────────────────────────────────────────────
    // Phase 1: benchmarks disabled until Phase 2 tree operations.
    // TODO(Phase 2): Re-enable bench/main.zig
}
