//! Benchmark harness for masstree-zig.
//!
//! Run with:  `zig build bench`   (or `zig build bench -Doptimize=ReleaseFast`)
//!
//! Each benchmark is timed via `std.time.Timer` and reports wall-clock
//! milliseconds and throughput (ops/sec).
//!
//! ## Benchmark Groups
//!
//! 1. **Single-threaded** — sequential/random insert, get, delete, mixed
//! 2. **Long keys** — keys spanning multiple trie layers (24-58 bytes)
//! 3. **Range scan** — full iteration over all keys
//! 4. **Concurrent** — multi-threaded insert, mixed read-write, contention

const std = @import("std");
const masstree = @import("masstree");
const Tree = masstree.MassTree(u64);

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn fmt_key(buf: []u8, prefix: []const u8, i: usize) []u8 {
    return std.fmt.bufPrint(buf, "{s}{d:0>10}", .{ prefix, i }) catch unreachable;
}

fn report(comptime name: []const u8, ns: u64, n: usize) void {
    const ms = @as(f64, @floatFromInt(ns)) / 1_000_000.0;
    const ops = @as(f64, @floatFromInt(n)) / (@as(f64, @floatFromInt(ns)) / 1_000_000_000.0);
    std.debug.print("  {s:<40} {d:>10.2} ms  {d:>14.0} ops/s\n", .{ name, ms, ops });
}

fn report_mt(comptime name: []const u8, ns: u64, total_ops: usize, threads: usize) void {
    const ms = @as(f64, @floatFromInt(ns)) / 1_000_000.0;
    const ops = @as(f64, @floatFromInt(total_ops)) / (@as(f64, @floatFromInt(ns)) / 1_000_000_000.0);
    std.debug.print("  {s:<40} {d:>10.2} ms  {d:>14.0} ops/s  ({d}T)\n", .{ name, ms, ops, threads });
}

// ─── Single-Threaded Benchmarks ──────────────────────────────────────────────

fn benchSeqInsert(a: std.mem.Allocator, n: usize) !u64 {
    var t = try Tree.init(a);
    defer t.deinit();
    var timer = try std.time.Timer.start();
    for (0..n) |i| {
        var buf: [64]u8 = undefined;
        _ = try t.put(fmt_key(&buf, "si_", i), i);
    }
    return timer.read();
}

fn benchSeqGet(a: std.mem.Allocator, n: usize) !u64 {
    var t = try Tree.init(a);
    defer t.deinit();
    for (0..n) |i| {
        var buf: [64]u8 = undefined;
        _ = try t.put(fmt_key(&buf, "sg_", i), i);
    }
    var timer = try std.time.Timer.start();
    for (0..n) |i| {
        var buf: [64]u8 = undefined;
        std.mem.doNotOptimizeAway(t.get(fmt_key(&buf, "sg_", i)));
    }
    return timer.read();
}

fn benchSeqDelete(a: std.mem.Allocator, n: usize) !u64 {
    var t = try Tree.init(a);
    defer t.deinit();
    for (0..n) |i| {
        var buf: [64]u8 = undefined;
        _ = try t.put(fmt_key(&buf, "sd_", i), i);
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

    var t = try Tree.init(a);
    defer t.deinit();
    var timer = try std.time.Timer.start();
    for (0..n) |i| _ = try t.put(keys[i][0..lens[i]], i);
    return timer.read();
}

fn benchRandGet(a: std.mem.Allocator, n: usize) !u64 {
    var prng = std.Random.DefaultPrng.init(0xDEADBEEF);
    const rng = prng.random();

    const keys = try a.alloc([64]u8, n);
    defer a.free(keys);
    const lens = try a.alloc(usize, n);
    defer a.free(lens);

    var t = try Tree.init(a);
    defer t.deinit();
    for (0..n) |i| {
        const r = rng.int(u64);
        const k = std.fmt.bufPrint(&keys[i], "rnd_{x:0>16}", .{r}) catch unreachable;
        lens[i] = k.len;
        _ = try t.put(keys[i][0..lens[i]], i);
    }

    var timer = try std.time.Timer.start();
    for (0..n) |i| std.mem.doNotOptimizeAway(t.get(keys[i][0..lens[i]]));
    return timer.read();
}

fn benchMixed(a: std.mem.Allocator, n: usize) !u64 {
    var t = try Tree.init(a);
    defer t.deinit();
    for (0..n / 2) |i| {
        var buf: [64]u8 = undefined;
        _ = try t.put(fmt_key(&buf, "mx_", i), i);
    }

    var timer = try std.time.Timer.start();
    for (0..n) |i| {
        var buf: [64]u8 = undefined;
        const k = fmt_key(&buf, "mx_", i);
        switch (i % 3) {
            0 => _ = try t.put(k, i),
            1 => std.mem.doNotOptimizeAway(t.get(k)),
            2 => std.mem.doNotOptimizeAway(t.remove(k)),
            else => unreachable,
        }
    }
    return timer.read();
}

fn benchLongKeyInsert(a: std.mem.Allocator, n: usize) !u64 {
    var t = try Tree.init(a);
    defer t.deinit();
    var timer = try std.time.Timer.start();
    for (0..n) |i| {
        var buf: [128]u8 = undefined;
        const k = std.fmt.bufPrint(
            &buf,
            "long_prefix_that_pushes_deep_into_trie_layers_{d:0>10}",
            .{i},
        ) catch unreachable;
        _ = try t.put(k, i);
    }
    return timer.read();
}

fn benchLongKeyGet(a: std.mem.Allocator, n: usize) !u64 {
    var t = try Tree.init(a);
    defer t.deinit();
    for (0..n) |i| {
        var buf: [128]u8 = undefined;
        const k = std.fmt.bufPrint(
            &buf,
            "long_prefix_that_pushes_deep_into_trie_layers_{d:0>10}",
            .{i},
        ) catch unreachable;
        _ = try t.put(k, i);
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

fn benchRangeAll(a: std.mem.Allocator, n: usize) !u64 {
    var t = try Tree.init(a);
    defer t.deinit();
    for (0..n) |i| {
        var buf: [64]u8 = undefined;
        _ = try t.put(fmt_key(&buf, "rng_", i), i);
    }

    var timer = try std.time.Timer.start();
    var it = t.range_all();
    var count: usize = 0;
    while (it.next()) |entry| {
        std.mem.doNotOptimizeAway(entry.value);
        count += 1;
    }
    const elapsed = timer.read();
    std.debug.assert(count == n);
    return elapsed;
}

// ─── Concurrent Benchmarks ───────────────────────────────────────────────────

/// Worker: disjoint inserts (each thread has its own key space).
fn concurrentInsertWorker(tree_ptr: *Tree, tid: usize, ops: usize) void {
    const base = tid * 1_000_000;
    for (0..ops) |i| {
        const key_val: u64 = @intCast(base + i);
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, key_val, .big);
        _ = tree_ptr.put(&buf, key_val) catch return;
    }
}

/// Worker: reads from a pre-populated tree.
fn concurrentGetWorker(tree_ptr: *Tree, ops: usize) void {
    for (0..ops) |i| {
        const key_val: u64 = @intCast(i);
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, key_val, .big);
        std.mem.doNotOptimizeAway(tree_ptr.get(&buf));
    }
}

/// Worker: mixed operations (insert 50%, get 25%, remove 25%).
fn concurrentMixedWorker(tree_ptr: *Tree, tid: usize, ops: usize) void {
    const base = tid * 1_000_000;
    for (0..ops) |i| {
        var buf: [8]u8 = undefined;
        switch (i % 4) {
            0, 1 => {
                const key_val: u64 = @intCast(base + i);
                std.mem.writeInt(u64, &buf, key_val, .big);
                _ = tree_ptr.put(&buf, key_val) catch return;
            },
            2 => {
                const key_val: u64 = @intCast(base + i / 2);
                std.mem.writeInt(u64, &buf, key_val, .big);
                std.mem.doNotOptimizeAway(tree_ptr.get(&buf));
            },
            3 => {
                const key_val: u64 = @intCast(base + i / 4);
                std.mem.writeInt(u64, &buf, key_val, .big);
                std.mem.doNotOptimizeAway(tree_ptr.remove(&buf));
            },
            else => unreachable,
        }
    }
}

/// Worker: hot-key contention (all threads write to a small key space).
fn concurrentHotKeyWorker(tree_ptr: *Tree, ops: usize, hot_range: usize) void {
    for (0..ops) |i| {
        const key_val: u64 = @intCast(i % hot_range);
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, key_val, .big);
        _ = tree_ptr.put(&buf, key_val) catch return;
    }
}

fn benchConcurrentInsert(a: std.mem.Allocator, n: usize, num_threads: usize) !u64 {
    var t = try Tree.init(a);
    defer t.deinit();

    const ops_per_thread = n / num_threads;
    const threads = try a.alloc(std.Thread, num_threads);
    defer a.free(threads);

    var timer = try std.time.Timer.start();
    for (0..num_threads) |tid| {
        threads[tid] = try std.Thread.spawn(.{}, concurrentInsertWorker, .{ &t, tid, ops_per_thread });
    }
    for (threads) |thr| thr.join();
    return timer.read();
}

fn benchConcurrentGet(a: std.mem.Allocator, n: usize, num_threads: usize) !u64 {
    var t = try Tree.init(a);
    defer t.deinit();

    // Pre-populate
    for (0..n) |i| {
        const key_val: u64 = @intCast(i);
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, key_val, .big);
        _ = try t.put(&buf, key_val);
    }

    const threads = try a.alloc(std.Thread, num_threads);
    defer a.free(threads);

    var timer = try std.time.Timer.start();
    for (0..num_threads) |tid| {
        _ = tid;
        threads[0] = undefined;
    }
    timer = try std.time.Timer.start();
    for (0..num_threads) |i| {
        threads[i] = try std.Thread.spawn(.{}, concurrentGetWorker, .{ &t, n });
    }
    for (threads) |thr| thr.join();
    return timer.read();
}

fn benchConcurrentMixed(a: std.mem.Allocator, n: usize, num_threads: usize) !u64 {
    var t = try Tree.init(a);
    defer t.deinit();

    const ops_per_thread = n / num_threads;
    const threads = try a.alloc(std.Thread, num_threads);
    defer a.free(threads);

    var timer = try std.time.Timer.start();
    for (0..num_threads) |tid| {
        threads[tid] = try std.Thread.spawn(.{}, concurrentMixedWorker, .{ &t, tid, ops_per_thread });
    }
    for (threads) |thr| thr.join();
    return timer.read();
}

fn benchConcurrentHotKey(a: std.mem.Allocator, n: usize, num_threads: usize) !u64 {
    var t = try Tree.init(a);
    defer t.deinit();

    const hot_range: usize = 64;
    const ops_per_thread = n / num_threads;
    const threads = try a.alloc(std.Thread, num_threads);
    defer a.free(threads);

    var timer = try std.time.Timer.start();
    for (0..num_threads) |i| {
        threads[i] = try std.Thread.spawn(.{}, concurrentHotKeyWorker, .{ &t, ops_per_thread, hot_range });
    }
    for (threads) |thr| thr.join();
    return timer.read();
}

// ─── Main ────────────────────────────────────────────────────────────────────

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const a = gpa.allocator();

    const sizes = [_]usize{ 1_000, 10_000, 100_000 };
    const thread_counts = [_]usize{ 1, 2, 4, 8 };

    std.debug.print("\n", .{});
    std.debug.print("  ══════════════════════════════════════════════════════════════════════\n", .{});
    std.debug.print("  Masstree-Zig Benchmark Suite\n", .{});
    std.debug.print("  ══════════════════════════════════════════════════════════════════════\n", .{});

    // ── Single-threaded benchmarks ──
    for (sizes) |n| {
        std.debug.print("\n  ── Single-Threaded, N = {d} ──\n", .{n});
        std.debug.print("  ──────────────────────────────────────────────────────────────────\n", .{});

        report("Sequential Insert", try benchSeqInsert(a, n), n);
        report("Sequential Get", try benchSeqGet(a, n), n);
        report("Sequential Delete", try benchSeqDelete(a, n), n);
        report("Random Insert", try benchRandInsert(a, n), n);
        report("Random Get", try benchRandGet(a, n), n);
        report("Mixed (insert/get/delete)", try benchMixed(a, n), n);
        report("Long Key Insert (58B)", try benchLongKeyInsert(a, n), n);
        report("Long Key Get (58B)", try benchLongKeyGet(a, n), n);
        report("Range Scan (all keys)", try benchRangeAll(a, n), n);
    }

    // ── Concurrent benchmarks ──
    const concurrent_n: usize = 100_000;
    for (thread_counts) |tc| {
        std.debug.print("\n  ── Concurrent, N = {d}, Threads = {d} ──\n", .{ concurrent_n, tc });
        std.debug.print("  ──────────────────────────────────────────────────────────────────\n", .{});

        const total_insert = (concurrent_n / tc) * tc;
        report_mt("Disjoint Insert", try benchConcurrentInsert(a, concurrent_n, tc), total_insert, tc);
        report_mt("Read-Only (pre-populated)", try benchConcurrentGet(a, concurrent_n, tc), concurrent_n * tc, tc);
        report_mt("Mixed (50% ins, 25% get, 25% del)", try benchConcurrentMixed(a, concurrent_n, tc), total_insert, tc);
        report_mt("Hot-Key Contention (64 keys)", try benchConcurrentHotKey(a, concurrent_n, tc), total_insert, tc);
    }

    std.debug.print("\n  ══════════════════════════════════════════════════════════════════════\n\n", .{});
}
