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
    const ext_test_sources = [_][]const u8{
        "tests/key_tests.zig",
        "tests/leaf_tests.zig",
        "tests/interior_tests.zig",
        "tests/layer_tests.zig",
        "tests/tree_tests.zig",
        "tests/integration_tests.zig",
    };

    for (ext_test_sources) |src| {
        const t = b.addTest(.{
            .root_module = b.createModule(.{
                .root_source_file = b.path(src),
                .target = target,
                .optimize = optimize,
                .imports = &.{
                    .{ .name = "masstree", .module = masstree_mod },
                },
            }),
        });
        const run = b.addRunArtifact(t);
        test_step.dependOn(&run.step);
    }

    // ── Benchmarks ──────────────────────────────────────────────────────
    const bench_exe = b.addExecutable(.{
        .name = "bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/main.zig"),
            .target = target,
            .optimize = .ReleaseFast,
            .imports = &.{
                .{ .name = "masstree", .module = masstree_mod },
            },
        }),
    });
    b.installArtifact(bench_exe);

    const run_bench = b.addRunArtifact(bench_exe);
    run_bench.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_bench.addArgs(args);
    const bench_step = b.step("bench", "Run benchmarks");
    bench_step.dependOn(&run_bench.step);
}
