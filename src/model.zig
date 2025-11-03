const std = @import("std");

pub const MILLION = 1_000_000.0;
pub const PRICING_URL = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json";

pub const DateFilters = struct {
    since: ?[10]u8 = null,
    until: ?[10]u8 = null,
};

pub const ParseDateError = error{
    InvalidFormat,
    InvalidDate,
};

pub fn parseFilterDate(input: []const u8) ParseDateError![10]u8 {
    if (input.len != 8) return error.InvalidFormat;

    _ = std.fmt.parseInt(u16, input[0..4], 10) catch return error.InvalidFormat;
    const month = std.fmt.parseInt(u8, input[4..6], 10) catch return error.InvalidFormat;
    const day = std.fmt.parseInt(u8, input[6..8], 10) catch return error.InvalidFormat;

    if (month == 0 or month > 12) return error.InvalidDate;
    if (day == 0 or day > 31) return error.InvalidDate;

    var buffer: [10]u8 = undefined;
    std.mem.copyForwards(u8, buffer[0..4], input[0..4]);
    buffer[4] = '-';
    std.mem.copyForwards(u8, buffer[5..7], input[4..6]);
    buffer[7] = '-';
    std.mem.copyForwards(u8, buffer[8..10], input[6..8]);

    return buffer;
}

pub const TokenUsage = struct {
    input_tokens: u64 = 0,
    cached_input_tokens: u64 = 0,
    output_tokens: u64 = 0,
    reasoning_output_tokens: u64 = 0,
    total_tokens: u64 = 0,

    pub fn add(self: *TokenUsage, other: TokenUsage) void {
        self.input_tokens += other.input_tokens;
        self.cached_input_tokens += other.cached_input_tokens;
        self.output_tokens += other.output_tokens;
        self.reasoning_output_tokens += other.reasoning_output_tokens;
        self.total_tokens += other.total_tokens;
    }
};

pub const TokenUsageEvent = struct {
    session_id: []const u8,
    timestamp: []const u8,
    local_iso_date: [10]u8,
    model: []const u8,
    usage: TokenUsage,
    is_fallback: bool,
};

pub const ModelPricing = struct {
    input_cost_per_m: f64,
    cached_input_cost_per_m: f64,
    output_cost_per_m: f64,
};

pub const PricingMap = std.StringHashMap(ModelPricing);

pub const ModelSummary = struct {
    name: []const u8,
    is_fallback: bool,
    pricing_available: bool,
    cost_usd: f64,
    usage: TokenUsage,
};

pub const DailySummary = struct {
    iso_date: []const u8,
    display_date: []const u8,
    usage: TokenUsage,
    cost_usd: f64,
    models: std.ArrayListUnmanaged(ModelSummary),
    model_index: std.StringHashMap(usize),
    missing_pricing: std.ArrayListUnmanaged([]const u8),

    pub fn init(allocator: std.mem.Allocator, iso_date: []const u8, display_date: []const u8) DailySummary {
        return .{
            .iso_date = iso_date,
            .display_date = display_date,
            .usage = .{},
            .cost_usd = 0,
            .models = .{},
            .model_index = std.StringHashMap(usize).init(allocator),
            .missing_pricing = .{},
        };
    }

    pub fn deinit(self: *DailySummary, allocator: std.mem.Allocator) void {
        self.models.deinit(allocator);
        self.model_index.deinit();
        self.missing_pricing.deinit(allocator);
    }
};

pub const SummaryTotals = struct {
    usage: TokenUsage = .{},
    cost_usd: f64 = 0,
    missing_pricing: std.ArrayListUnmanaged([]const u8),

    pub fn init() SummaryTotals {
        return .{
            .usage = .{},
            .cost_usd = 0,
            .missing_pricing = .{},
        };
    }

    pub fn deinit(self: *SummaryTotals, allocator: std.mem.Allocator) void {
        self.missing_pricing.deinit(allocator);
    }
};

pub fn buildDailySummaries(
    allocator: std.mem.Allocator,
    arena: std.mem.Allocator,
    events: []const TokenUsageEvent,
    summaries: *std.ArrayListUnmanaged(DailySummary),
    date_index: *std.StringHashMap(usize),
    filters: DateFilters,
) !void {
    for (events) |event| {
        const iso_slice = event.local_iso_date[0..];
        if (!withinFilters(filters, iso_slice)) continue;

        if (date_index.get(iso_slice)) |idx| {
            try updateSummary(&summaries.items[idx], allocator, arena, &event);
            continue;
        }

        const iso_copy = try arena.dupe(u8, iso_slice);
        const display = try formatDisplayDate(arena, iso_copy);
        try summaries.append(allocator, DailySummary.init(allocator, iso_copy, display));
        const summary_idx = summaries.items.len - 1;
        try date_index.put(iso_copy, summary_idx);
        try updateSummary(&summaries.items[summary_idx], allocator, arena, &event);
    }
}

pub fn applyPricing(
    allocator: std.mem.Allocator,
    summary: *DailySummary,
    pricing_map: *PricingMap,
    missing_set: *std.StringHashMap(u8),
) void {
    summary.cost_usd = 0;
    summary.missing_pricing.clearRetainingCapacity();

    for (summary.models.items) |*model| {
        if (pricing_map.get(model.name)) |rate| {
            model.pricing_available = true;
            model.cost_usd = calculateCost(model.usage, rate);
            summary.cost_usd += model.cost_usd;
        } else {
            model.pricing_available = false;
            model.cost_usd = 0;
            _ = missing_set.put(model.name, 1) catch {};
            appendUniqueString(allocator, &summary.missing_pricing, model.name) catch {};
        }
    }
}

pub fn accumulateTotals(
    allocator: std.mem.Allocator,
    summaries: *std.ArrayListUnmanaged(DailySummary),
    totals: *SummaryTotals,
) void {
    for (summaries.items) |summary| {
        totals.usage.add(summary.usage);
        totals.cost_usd += summary.cost_usd;
        for (summary.missing_pricing.items) |name| {
            appendUniqueString(allocator, &totals.missing_pricing, name) catch {};
        }
    }
}

pub fn collectMissingModels(
    allocator: std.mem.Allocator,
    missing_set: *std.StringHashMap(u8),
    output: *std.ArrayListUnmanaged([]const u8),
) !void {
    var iterator = missing_set.iterator();
    while (iterator.next()) |entry| {
        try appendUniqueString(allocator, output, entry.key_ptr.*);
    }
}

fn withinFilters(filters: DateFilters, iso: []const u8) bool {
    if (filters.since) |since_value| {
        if (std.mem.lessThan(u8, iso, since_value[0..])) return false;
    }
    if (filters.until) |until_value| {
        if (std.mem.lessThan(u8, until_value[0..], iso)) return false;
    }
    return true;
}

fn updateSummary(
    summary: *DailySummary,
    allocator: std.mem.Allocator,
    arena: std.mem.Allocator,
    event: *const TokenUsageEvent,
) !void {
    summary.usage.add(event.usage);

    if (summary.model_index.get(event.model)) |idx| {
        var model_summary = &summary.models.items[idx];
        model_summary.usage.add(event.usage);
        if (event.is_fallback) model_summary.is_fallback = true;
        return;
    }

    const stored_name = try arena.dupe(u8, event.model);
    try summary.models.append(allocator, .{
        .name = stored_name,
        .is_fallback = event.is_fallback,
        .pricing_available = false,
        .cost_usd = 0,
        .usage = event.usage,
    });
    const new_index = summary.models.items.len - 1;
    try summary.model_index.put(summary.models.items[new_index].name, new_index);
}

fn calculateCost(usage: TokenUsage, pricing: ModelPricing) f64 {
    const non_cached = if (usage.input_tokens > usage.cached_input_tokens)
        usage.input_tokens - usage.cached_input_tokens
    else
        0;
    const cached = if (usage.cached_input_tokens > usage.input_tokens)
        usage.input_tokens
    else
        usage.cached_input_tokens;

    const input_cost = (@as(f64, @floatFromInt(non_cached)) / MILLION) * pricing.input_cost_per_m;
    const cached_cost = (@as(f64, @floatFromInt(cached)) / MILLION) * pricing.cached_input_cost_per_m;
    const output_cost = (@as(f64, @floatFromInt(usage.output_tokens)) / MILLION) * pricing.output_cost_per_m;

    return input_cost + cached_cost + output_cost;
}

fn appendUniqueString(
    allocator: std.mem.Allocator,
    list: *std.ArrayListUnmanaged([]const u8),
    value: []const u8,
) !void {
    for (list.items) |existing| {
        if (std.mem.eql(u8, existing, value)) return;
    }
    try list.append(allocator, value);
}

fn formatDisplayDate(arena: std.mem.Allocator, iso_date: []const u8) ![]u8 {
    if (iso_date.len < 10) return error.InvalidDate;

    const year = try std.fmt.parseInt(u16, iso_date[0..4], 10);
    const month = try std.fmt.parseInt(u8, iso_date[5..7], 10);
    const day = try std.fmt.parseInt(u8, iso_date[8..10], 10);

    const months = [_][]const u8{
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    };
    if (month == 0 or month > months.len) return error.InvalidDate;

    return std.fmt.allocPrint(arena, "{s} {d:0>2}, {d}", .{ months[month - 1], day, year });
}
