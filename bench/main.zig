//! Benchmark harness for masstree-zig.
//!
//! Run with:  `zig build bench`
//!
//! Each benchmark is timed via `std.time.Timer` and reports wall-clock
//! milliseconds and throughput (ops/sec).

const std = @import("std");
const Masstree = @import("masstree").Masstree;

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn fmt_key(buf: []u8, prefix: []const u8, i: usize) []u8 {
    return std.fmt.bufPrint(buf, "{s}{d:0>10}", .{ prefix, i }) catch unreachable;
}

fn report(comptime name: []const u8, ns: u64, n: usize) void {
    const ms = @as(f64, @floatFromInt(ns)) / 1_000_000.0;
    const ops = @as(f64, @floatFromInt(n)) / (@as(f64, @floatFromInt(ns)) / 1_000_000_000.0);
    std.debug.print("  {s:<34} {d:>10.2} ms  {d:>14.0} ops/s\n", .{ name, ms, ops });
}

// ─── Individual benchmarks ───────────────────────────────────────────────────

fn benchSeqInsert(a: std.mem.Allocator, n: usize) !u64 {
    var t = try Masstree.init(a);
    defer t.deinit();
    var timer = try std.time.Timer.start();
    for (0..n) |i| {
        var buf: [64]u8 = undefined;
        try t.put(fmt_key(&buf, "si_", i), i);
    }
    return timer.read();
}

fn benchSeqGet(a: std.mem.Allocator, n: usize) !u64 {
    var t = try Masstree.init(a);
    defer t.deinit();
    for (0..n) |i| {
        var buf: [64]u8 = undefined;
        try t.put(fmt_key(&buf, "sg_", i), i);
    }
    var timer = try std.time.Timer.start();
    for (0..n) |i| {
        var buf: [64]u8 = undefined;
        std.mem.doNotOptimizeAway(t.get(fmt_key(&buf, "sg_", i)));
    }
    return timer.read();
}

fn benchSeqDelete(a: std.mem.Allocator, n: usize) !u64 {
    var t = try Masstree.init(a);
    defer t.deinit();
    for (0..n) |i| {
        var buf: [64]u8 = undefined;
        try t.put(fmt_key(&buf, "sd_", i), i);
    }
    var timer = try std.time.Timer.start();
    for (0..n) |i| {
        var buf: [64]u8 = undefined;
        std.mem.doNotOptimizeAway(t.remove(fmt_key(&buf, "sd_", i)));
    }
    return timer.read();
}

fn benchRandInsert(a: std.mem.Allocator, n: usize) !u64 {
    var prng = std.Random.DefaultPrng.init(0xDEADBEEF);
    const rng = prng.random();

    const keys = try a.alloc([64]u8, n);
    defer a.free(keys);
    const lens = try a.alloc(usize, n);
    defer a.free(lens);
    for (0..n) |i| {
        const r = rng.int(u64);
        const k = std.fmt.bufPrint(&keys[i], "rnd_{x:0>16}", .{r}) catch unreachable;
        lens[i] = k.len;
    }

    var t = try Masstree.init(a);
    defer t.deinit();
    var timer = try std.time.Timer.start();
    for (0..n) |i| try t.put(keys[i][0..lens[i]], i);
    return timer.read();
}

fn benchRandGet(a: std.mem.Allocator, n: usize) !u64 {
    var prng = std.Random.DefaultPrng.init(0xDEADBEEF);
    const rng = prng.random();

    const keys = try a.alloc([64]u8, n);
    defer a.free(keys);
    const lens = try a.alloc(usize, n);
    defer a.free(lens);

    var t = try Masstree.init(a);
    defer t.deinit();
    for (0..n) |i| {
        const r = rng.int(u64);
        const k = std.fmt.bufPrint(&keys[i], "rnd_{x:0>16}", .{r}) catch unreachable;
        lens[i] = k.len;
        try t.put(keys[i][0..lens[i]], i);
    }

    var timer = try std.time.Timer.start();
    for (0..n) |i| std.mem.doNotOptimizeAway(t.get(keys[i][0..lens[i]]));
    return timer.read();
}

fn benchMixed(a: std.mem.Allocator, n: usize) !u64 {
    var t = try Masstree.init(a);
    defer t.deinit();
    for (0..n / 2) |i| {
        var buf: [64]u8 = undefined;
        try t.put(fmt_key(&buf, "mx_", i), i);
    }

    var timer = try std.time.Timer.start();
    for (0..n) |i| {
        var buf: [64]u8 = undefined;
        const k = fmt_key(&buf, "mx_", i);
        switch (i % 3) {
            0 => try t.put(k, i),
            1 => std.mem.doNotOptimizeAway(t.get(k)),
            2 => std.mem.doNotOptimizeAway(t.remove(k)),
            else => unreachable,
        }
    }
    return timer.read();
}

fn benchLongKeyInsert(a: std.mem.Allocator, n: usize) !u64 {
    var t = try Masstree.init(a);
    defer t.deinit();
    var timer = try std.time.Timer.start();
    for (0..n) |i| {
        var buf: [128]u8 = undefined;
        const k = std.fmt.bufPrint(
            &buf,
            "long_prefix_that_pushes_deep_into_trie_layers_{d:0>10}",
            .{i},
        ) catch unreachable;
        try t.put(k, i);
    }
    return timer.read();
}

fn benchLongKeyGet(a: std.mem.Allocator, n: usize) !u64 {
    var t = try Masstree.init(a);
    defer t.deinit();
    for (0..n) |i| {
        var buf: [128]u8 = undefined;
        const k = std.fmt.bufPrint(
            &buf,
            "long_prefix_that_pushes_deep_into_trie_layers_{d:0>10}",
            .{i},
        ) catch unreachable;
        try t.put(k, i);
    }
    var timer = try std.time.Timer.start();
    for (0..n) |i| {
        var buf: [128]u8 = undefined;
        const k = std.fmt.bufPrint(
            &buf,
            "long_prefix_that_pushes_deep_into_trie_layers_{d:0>10}",
            .{i},
        ) catch unreachable;
        std.mem.doNotOptimizeAway(t.get(k));
    }
    return timer.read();
}

// ─── Main ────────────────────────────────────────────────────────────────────

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const a = gpa.allocator();

    const sizes = [_]usize{ 1_000, 10_000, 100_000 };

    std.debug.print("\n", .{});
    std.debug.print("  ══════════════════════════════════════════════════════════════════\n", .{});
    std.debug.print("  Masstree-Zig Benchmark Suite\n", .{});
    std.debug.print("  ══════════════════════════════════════════════════════════════════\n", .{});

    for (sizes) |n| {
        std.debug.print("\n  N = {d}\n", .{n});
        std.debug.print("  ──────────────────────────────────────────────────────────────\n", .{});

        report("Sequential Insert", try benchSeqInsert(a, n), n);
        report("Sequential Get", try benchSeqGet(a, n), n);
        report("Sequential Delete", try benchSeqDelete(a, n), n);
        report("Random Insert", try benchRandInsert(a, n), n);
        report("Random Get", try benchRandGet(a, n), n);
        report("Mixed (insert/get/delete)", try benchMixed(a, n), n);
        report("Long Key Insert (58B)", try benchLongKeyInsert(a, n), n);
        report("Long Key Get (58B)", try benchLongKeyGet(a, n), n);
    }

    std.debug.print("\n  ══════════════════════════════════════════════════════════════════\n\n", .{});
}
