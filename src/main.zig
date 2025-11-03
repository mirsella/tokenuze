const std = @import("std");
const tokenuze = @import("tokenuze");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    try tokenuze.run(gpa.allocator());
}
