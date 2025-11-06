const std = @import("std");

pub const ArrayWriter = struct {
    base: std.Io.Writer,
    list: *std.ArrayList(u8),
    allocator: std.mem.Allocator,

    pub fn init(list: *std.ArrayList(u8), allocator: std.mem.Allocator) ArrayWriter {
        return .{
            .base = .{
                .vtable = &.{
                    .drain = ArrayWriter.drain,
                    .sendFile = std.Io.Writer.unimplementedSendFile,
                    .flush = ArrayWriter.flush,
                    .rebase = std.Io.Writer.defaultRebase,
                },
                .buffer = &.{},
            },
            .list = list,
            .allocator = allocator,
        };
    }

    pub fn writer(self: *ArrayWriter) *std.Io.Writer {
        return &self.base;
    }

    fn drain(
        writer_ptr: *std.Io.Writer,
        data: []const []const u8,
        splat: usize,
    ) std.Io.Writer.Error!usize {
        const self: *ArrayWriter = @fieldParentPtr("base", writer_ptr);
        var written: usize = 0;
        for (data) |chunk| {
            if (chunk.len == 0) continue;
            self.list.appendSlice(self.allocator, chunk) catch return error.WriteFailed;
            written += chunk.len;
        }
        if (splat > 1 and data.len != 0) {
            const last = data[data.len - 1];
            const extra = splat - 1;
            for (0..extra) |_| {
                self.list.appendSlice(self.allocator, last) catch return error.WriteFailed;
                written += last.len;
            }
        }
        return written;
    }

    fn flush(_: *std.Io.Writer) std.Io.Writer.Error!void {
        return;
    }
};
