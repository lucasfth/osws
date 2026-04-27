//! check_architecture.zig — OSWS clean-architecture checker
//!
//! Usage:
//!   zig run scripts/check_architecture.zig -- <repo-root>
//!   zig run scripts/check_architecture.zig -- .          (from repo root)
//!
//! Defaults repo-root to ".." (run from scripts/ directory).
//! Exit code: 0 = clean, 1 = violations found.
//! Tests: zig test scripts/check_architecture.zig

const std = @import("std");

// ANSI colors
const GREEN = "\x1b[32m";
const RED = "\x1b[31m";
const BOLD = "\x1b[1m";
const RESET = "\x1b[0m";

const LAYER_NAMES = [_][]const u8{ "core", "infrastructure", "application", "test/bench" };

const ProjectLayer = struct { name: []const u8, layer: u8 };

const PROJECT_LAYERS = [_]ProjectLayer{
    .{ .name = "OSWS.Models", .layer = 0 },
    .{ .name = "OSWS.Common", .layer = 0 },
    .{ .name = "OSWS.Library", .layer = 1 },
    .{ .name = "OSWS.KeyManager", .layer = 1 },
    .{ .name = "OSWS.ParquetSolver", .layer = 1 },
    .{ .name = "OSWS.WebApi", .layer = 2 },
    .{ .name = "OSWS.WebApi.Tests", .layer = 3 },
    .{ .name = "OSWS.ParquetSolver.Tests", .layer = 3 },
    .{ .name = "OSWS.Performance.Benchmarks", .layer = 3 },
};

fn getLayer(name: []const u8) ?u8 {
    for (PROJECT_LAYERS) |e| {
        if (std.mem.eql(u8, e.name, name)) return e.layer;
    }
    return null;
}

/// "OSWS.WebApi.csproj" -> "OSWS.WebApi"
fn projectNameFromCsproj(basename: []const u8) []const u8 {
    if (std.mem.endsWith(u8, basename, ".csproj"))
        return basename[0 .. basename.len - 7];
    return basename;
}

/// `<ProjectReference Include="..\OSWS.Models\OSWS.Models.csproj" />`
/// returns `..\OSWS.Models\OSWS.Models.csproj`
fn extractIncludeValue(line: []const u8) ?[]const u8 {
    const key = "Include=\"";
    const pos = std.mem.indexOf(u8, line, key) orelse return null;
    const after = line[pos + key.len ..];
    const end = std.mem.indexOf(u8, after, "\"") orelse return null;
    return after[0..end];
}

/// Basename that handles both / and \ separators (csproj files use \ on Windows).
fn pathBasename(path: []const u8) []const u8 {
    var i = path.len;
    while (i > 0) {
        i -= 1;
        if (path[i] == '/' or path[i] == '\\') return path[i + 1 ..];
    }
    return path;
}

/// "..\OSWS.Models\OSWS.Models.csproj" -> "OSWS.Models"
fn projectNameFromRef(include_val: []const u8) []const u8 {
    return projectNameFromCsproj(pathBasename(include_val));
}

/// Returns the full OSWS qualified namespace from a using directive.
/// "using OSWS.Models.Entities;" -> "OSWS.Models.Entities"
/// "using OSWS.Common;"          -> "OSWS.Common"
/// "using System;"               -> null
fn extractOswsNamespace(line: []const u8) ?[]const u8 {
    const trimmed = std.mem.trim(u8, line, " \t\r\n");
    if (!std.mem.startsWith(u8, trimmed, "using OSWS.")) return null;
    const after = trimmed["using ".len..]; // e.g. "OSWS.Models.Entities;"
    for (after, 0..) |c, i| {
        if (c == ';' or c == ' ' or c == '\r' or c == '\n') return after[0..i];
    }
    if (std.mem.endsWith(u8, after, ";")) return after[0 .. after.len - 1];
    return after;
}

/// Resolves a full namespace to its owning project name.
/// "OSWS.Models.Entities"                          -> "OSWS.Models"
/// "OSWS.Performance.Benchmarks.DatasetGenerators" -> "OSWS.Performance.Benchmarks"
/// "OSWS.Unknown.Foo"                              -> null
fn resolveProjectName(ns: []const u8) ?[]const u8 {
    for (PROJECT_LAYERS) |e| {
        if (std.mem.eql(u8, ns, e.name)) return e.name;
        if (ns.len > e.name.len and
            std.mem.startsWith(u8, ns, e.name) and
            ns[e.name.len] == '.')
        {
            return e.name;
        }
    }
    return null;
}

/// "OSWS.WebApi/Endpoints/S3Get.cs" -> "OSWS.WebApi"
fn projectFromPath(path: []const u8) ?[]const u8 {
    const sep = std.mem.indexOfAny(u8, path, "/\\") orelse return null;
    return path[0..sep];
}

fn shouldSkip(path: []const u8) bool {
    for ([_][]const u8{ "/bin/", "/obj/", "/Migrations/", "\\bin\\", "\\obj\\" }) |s| {
        if (std.mem.indexOf(u8, path, s) != null) return true;
    }
    return std.mem.startsWith(u8, path, "bin/") or std.mem.startsWith(u8, path, "obj/");
}

const ProjectInfo = struct {
    name: []const u8,
    project_refs: std.ArrayList([]const u8),
    package_refs: std.ArrayList([]const u8),
    usings: std.StringHashMap(void),

    fn init(name: []const u8, alloc: std.mem.Allocator) ProjectInfo {
        return .{
            .name = name,
            .project_refs = .{},
            .package_refs = .{},
            .usings = std.StringHashMap(void).init(alloc),
        };
    }
};

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const args = try std.process.argsAlloc(alloc);
    const repo_root = if (args.len > 1) args[1] else "..";

    var projects = std.StringHashMap(ProjectInfo).init(alloc);

    // ── Phase 1: parse .csproj files ─────────────────────────────────────────
    {
        var dir = try std.fs.cwd().openDir(repo_root, .{ .iterate = true });
        defer dir.close();
        var walker = try dir.walk(alloc);
        defer walker.deinit();

        while (try walker.next()) |entry| {
            if (entry.kind != .file) continue;
            if (shouldSkip(entry.path)) continue;
            if (!std.mem.endsWith(u8, entry.basename, ".csproj")) continue;
            if (std.mem.indexOf(u8, entry.path, "BenchmarkDotNet.Autogenerated") != null) continue;

            const proj_name = try alloc.dupe(u8, projectNameFromCsproj(entry.basename));
            var info = ProjectInfo.init(proj_name, alloc);

            const content = dir.readFileAlloc(alloc, entry.path, 1024 * 1024) catch continue;
            var lines = std.mem.splitScalar(u8, content, '\n');
            while (lines.next()) |line| {
                const t = std.mem.trim(u8, line, " \t\r");
                if (std.mem.indexOf(u8, t, "<ProjectReference") != null) {
                    if (extractIncludeValue(t)) |val| {
                        try info.project_refs.append(alloc, try alloc.dupe(u8, projectNameFromRef(val)));
                    }
                } else if (std.mem.indexOf(u8, t, "<PackageReference") != null) {
                    if (extractIncludeValue(t)) |val| {
                        try info.package_refs.append(alloc, try alloc.dupe(u8, val));
                    }
                }
            }

            try projects.put(proj_name, info);
        }
    }

    // ── Phase 2: parse .cs files for using directives ────────────────────────
    {
        var dir = try std.fs.cwd().openDir(repo_root, .{ .iterate = true });
        defer dir.close();
        var walker = try dir.walk(alloc);
        defer walker.deinit();

        while (try walker.next()) |entry| {
            if (entry.kind != .file) continue;
            if (shouldSkip(entry.path)) continue;
            if (!std.mem.endsWith(u8, entry.basename, ".cs")) continue;

            const proj_name = projectFromPath(entry.path) orelse continue;
            const info_ptr = projects.getPtr(proj_name) orelse continue;

            const content = dir.readFileAlloc(alloc, entry.path, 2 * 1024 * 1024) catch continue;
            var lines = std.mem.splitScalar(u8, content, '\n');
            while (lines.next()) |line| {
                const full_ns = extractOswsNamespace(line) orelse continue;
                const proj_ref = resolveProjectName(full_ns) orelse continue;
                if (std.mem.eql(u8, proj_ref, proj_name)) continue; // skip self-reference
                try info_ptr.usings.put(proj_ref, {}); // proj_ref is a compile-time literal
            }
        }
    }

    // ── Phase 3: report ───────────────────────────────────────────────────────
    var out_buf: [8192]u8 = undefined;
    var fw = std.fs.File.stdout().writer(&out_buf);
    defer fw.interface.flush() catch {};
    const stdout = &fw.interface;
    const Violation = struct { from: []const u8, to: []const u8, from_layer: u8, to_layer: u8 };
    var violations: std.ArrayList(Violation) = .{};

    // Sort projects by layer for display
    var proj_list: std.ArrayList(*ProjectInfo) = .{};
    var iter = projects.valueIterator();
    while (iter.next()) |v| try proj_list.append(alloc, v);
    std.mem.sort(*ProjectInfo, proj_list.items, {}, struct {
        fn lt(_: void, a: *ProjectInfo, b: *ProjectInfo) bool {
            return (getLayer(a.name) orelse 99) < (getLayer(b.name) orelse 99);
        }
    }.lt);

    try stdout.print("{s}=== OSWS Architecture Analysis ==={s}\n\n", .{ BOLD, RESET });

    // ── Project Dependency Graph ──────────────────────────────────────────────
    try stdout.print("{s}[Project Dependency Graph]{s}\n", .{ BOLD, RESET });
    for (proj_list.items) |info| {
        const from_layer = getLayer(info.name) orelse 99;
        const layer_name = if (from_layer < LAYER_NAMES.len) LAYER_NAMES[from_layer] else "unknown";
        try stdout.print("  {s} (layer {d} - {s})\n", .{ info.name, from_layer, layer_name });
        if (info.project_refs.items.len == 0) {
            try stdout.print("    (no project references)\n", .{});
        }
        for (info.project_refs.items) |ref| {
            const to_layer = getLayer(ref) orelse 99;
            const violation = from_layer < 3 and to_layer > from_layer;
            if (violation) {
                try stdout.print("    {s}-> {s} (layer {d}) [ProjectReference] VIOLATION{s}\n", .{ RED, ref, to_layer, RESET });
                try violations.append(alloc, .{ .from = info.name, .to = ref, .from_layer = from_layer, .to_layer = to_layer });
            } else {
                try stdout.print("    {s}-> {s} (layer {d}) [ProjectReference]{s}\n", .{ GREEN, ref, to_layer, RESET });
            }
        }
    }
    try stdout.print("\n", .{});

    // ── NuGet Package References ──────────────────────────────────────────────
    try stdout.print("{s}[NuGet Package References]{s}\n", .{ BOLD, RESET });
    for (proj_list.items) |info| {
        if (info.package_refs.items.len == 0) continue;
        try stdout.print("  {s}:", .{info.name});
        for (info.package_refs.items, 0..) |pkg, i| {
            if (i > 0) try stdout.writeAll(",");
            try stdout.print(" {s}", .{pkg});
        }
        try stdout.writeAll("\n");
    }
    try stdout.print("\n", .{});

    // ── Using Namespaces per Project ──────────────────────────────────────────
    try stdout.print("{s}[Using Namespaces per Project]{s}\n", .{ BOLD, RESET });
    for (proj_list.items) |info| {
        const from_layer = getLayer(info.name) orelse 99;
        try stdout.print("  {s}:\n", .{info.name});
        if (info.usings.count() == 0) {
            try stdout.print("    (no OSWS using directives)\n", .{});
            continue;
        }
        var using_iter = info.usings.keyIterator();
        while (using_iter.next()) |ns| {
            const to_layer = getLayer(ns.*) orelse 99;
            const violation = from_layer < 3 and to_layer > from_layer;
            if (violation) {
                try stdout.print("    {s}x {s} (layer {d}){s}\n", .{ RED, ns.*, to_layer, RESET });
                try violations.append(alloc, .{ .from = info.name, .to = ns.*, .from_layer = from_layer, .to_layer = to_layer });
            } else {
                try stdout.print("    {s}v {s} (layer {d}){s}\n", .{ GREEN, ns.*, to_layer, RESET });
            }
        }
    }
    try stdout.print("\n", .{});

    // ── Violations Summary ────────────────────────────────────────────────────
    try stdout.print("{s}[Violations]{s}\n", .{ BOLD, RESET });
    if (violations.items.len == 0) {
        try stdout.print("  {s}No violations found{s}\n", .{ GREEN, RESET });
    } else {
        for (violations.items) |v| {
            try stdout.print("  {s}VIOLATION: {s} references {s} (layer {d} -> layer {d}){s}\n", .{ RED, v.from, v.to, v.from_layer, v.to_layer, RESET });
        }
    }
    try stdout.print("\n", .{});

    if (violations.items.len > 0) std.process.exit(1);
}

// ── Tests ────────────────────────────────────────────────────────────────────

test "projectNameFromCsproj" {
    try std.testing.expectEqualStrings("OSWS.WebApi", projectNameFromCsproj("OSWS.WebApi.csproj"));
    try std.testing.expectEqualStrings("OSWS.Models", projectNameFromCsproj("OSWS.Models.csproj"));
}

test "extractIncludeValue" {
    const line = "    <ProjectReference Include=\"..\\OSWS.Models\\OSWS.Models.csproj\" />";
    const val = extractIncludeValue(line);
    try std.testing.expect(val != null);
    try std.testing.expectEqualStrings("..\\OSWS.Models\\OSWS.Models.csproj", val.?);
}

test "projectNameFromRef" {
    try std.testing.expectEqualStrings("OSWS.Models", projectNameFromRef("..\\OSWS.Models\\OSWS.Models.csproj"));
    try std.testing.expectEqualStrings("OSWS.Common", projectNameFromRef("../OSWS.Common/OSWS.Common.csproj"));
}

test "extractOswsNamespace" {
    try std.testing.expectEqualStrings("OSWS.Models", extractOswsNamespace("using OSWS.Models;").?);
    try std.testing.expectEqualStrings("OSWS.Models.Entities", extractOswsNamespace("using OSWS.Models.Entities;").?);
    try std.testing.expectEqualStrings("OSWS.Common", extractOswsNamespace("    using OSWS.Common;").?);
    try std.testing.expect(extractOswsNamespace("using System;") == null);
    try std.testing.expect(extractOswsNamespace("using Microsoft.Extensions.Logging;") == null);
}

test "resolveProjectName" {
    try std.testing.expectEqualStrings("OSWS.Models", resolveProjectName("OSWS.Models").?);
    try std.testing.expectEqualStrings("OSWS.Models", resolveProjectName("OSWS.Models.Entities").?);
    try std.testing.expectEqualStrings("OSWS.Performance.Benchmarks", resolveProjectName("OSWS.Performance.Benchmarks.DatasetGenerators").?);
    try std.testing.expect(resolveProjectName("OSWS.Unknown.Foo") == null);
}

test "projectFromPath" {
    try std.testing.expectEqualStrings("OSWS.WebApi", projectFromPath("OSWS.WebApi/Endpoints/S3Get.cs").?);
    try std.testing.expectEqualStrings("OSWS.ParquetSolver", projectFromPath("OSWS.ParquetSolver/Helpers/Cryptography.cs").?);
}

test "getLayer" {
    try std.testing.expectEqual(@as(u8, 0), getLayer("OSWS.Models").?);
    try std.testing.expectEqual(@as(u8, 0), getLayer("OSWS.Common").?);
    try std.testing.expectEqual(@as(u8, 1), getLayer("OSWS.Library").?);
    try std.testing.expectEqual(@as(u8, 2), getLayer("OSWS.WebApi").?);
    try std.testing.expectEqual(@as(u8, 3), getLayer("OSWS.WebApi.Tests").?);
    try std.testing.expect(getLayer("OSWS.Unknown") == null);
}
