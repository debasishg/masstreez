const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── Static library ──────────────────────────────────────────────────
    const lib = b.addStaticLibrary(.{
        .name = "masstree",
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    b.installArtifact(lib);

    // ── Unit tests (per module + integration) ───────────────────────────
    const test_sources = [_][]const u8{
        "src/key.zig",
        "src/leaf.zig",
        "src/interior.zig",
        "src/layer.zig",
        "src/tree.zig",
        "src/root.zig",
        "tests/key_tests.zig",
        "tests/leaf_tests.zig",
        "tests/interior_tests.zig",
        "tests/layer_tests.zig",
        "tests/tree_tests.zig",
        "tests/integration_tests.zig",
    };

    const test_step = b.step("test", "Run all unit and integration tests");
    for (test_sources) |src| {
        const t = b.addTest(.{
            .root_source_file = b.path(src),
            .target = target,
            .optimize = optimize,
        });
        const run = b.addRunArtifact(t);
        test_step.dependOn(&run.step);
    }

    // ── Benchmarks ──────────────────────────────────────────────────────
    const bench_exe = b.addExecutable(.{
        .name = "bench",
        .root_source_file = b.path("bench/main.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    b.installArtifact(bench_exe);

    const run_bench = b.addRunArtifact(bench_exe);
    run_bench.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_bench.addArgs(args);
    }
    const bench_step = b.step("bench", "Run benchmarks");
    bench_step.dependOn(&run_bench.step);
}
