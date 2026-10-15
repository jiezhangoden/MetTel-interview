namespace VendorApi;

public static class Simulator
{
    public const string Connected = "CONNECTED";
    public const string Offline = "OFFLINE";
    public const string NeverActivated = "NEVER_ACTIVATED";
    public const string Stable = "STABLE";
    public const string Disconnected = "DISCONNECTED";
    public const string InService = "IN_SERVICE";
    public const string OutOfService = "OUT_OF_SERVICE";

    public static readonly IReadOnlyList<(string Host, int Count)> Hosts =
    [
        ("host_1", 100),
        ("host_2", 200),
        ("host_3", 1000),
    ];

    public static bool TryParseHost(string host, out int hostNumber, out int deviceCount)
    {
        hostNumber = 0;
        deviceCount = 0;
        for (var i = 0; i < Hosts.Count; i++)
        {
            if (!string.Equals(Hosts[i].Host, host, StringComparison.Ordinal))
                continue;
            hostNumber = i + 1;
            deviceCount = Hosts[i].Count;
            return true;
        }
        return false;
    }

    public static TimeSpan ResponseDelay(int hostNumber, double scale)
    {
        if (hostNumber < 1)
            throw new ArgumentOutOfRangeException(nameof(hostNumber));
        var factor = scale < 0 ? 0 : scale;
        return TimeSpan.FromMinutes(hostNumber * factor);
    }

    public static IReadOnlyList<VendorHost> HostCatalog()
    {
        return Hosts.Select(host =>
        {
            if (!TryParseHost(host.Host, out var number, out var count))
                throw new InvalidOperationException($"Unknown host '{host.Host}'.");
            return new VendorHost(host.Host, $"/vendor/{host.Host}/devices", count, number);
        }).ToArray();
    }

    public static VendorDevicesResponse VendorSnapshot(string host, DateTimeOffset now)
    {
        if (!TryParseHost(host, out var hostNumber, out var deviceCount))
            throw new ArgumentException($"Unknown host '{host}'.", nameof(host));

        var edges = new EdgeDto[deviceCount];
        for (var index = 1; index <= deviceCount; index++)
            edges[index - 1] = BuildEdge(hostNumber, index, now);

        return new VendorDevicesResponse(host, Iso(now), edges);
    }

    public static CompanyInventoryResponse CompanyInventory(DateTimeOffset now)
    {
        var devices = new List<CompanyDeviceDto>(1305);
        foreach (var (host, count) in Hosts)
        {
            var hostNumber = host[^1] - '0';
            for (var index = 1; index <= count; index++)
            {
                if (index == 19)
                    continue;
                devices.Add(BuildCompanyDevice(hostNumber, index, serialOverride: null, edgeOverride: null));
            }
        }

        for (var n = 1; n <= 5; n++)
        {
            devices.Add(BuildCompanyDevice(
                hostNumber: 0,
                index: n,
                serialOverride: $"COMPANY-ONLY-{n:D3}",
                edgeOverride: 9_000_000 + n));
        }

        return new CompanyInventoryResponse(Iso(now), devices);
    }

    public static string SerialFor(int hostNumber, int index) => $"SIM-{hostNumber}-{index:D5}";

    public static int EdgeIdFor(int hostNumber, int index) => hostNumber * 1_000_000 + index;

    private static EdgeDto BuildEdge(int hostNumber, int index, DateTimeOffset now)
    {
        var picture = PictureFor(index, now);
        var edgeId = EdgeIdFor(hostNumber, index);
        var serial = SerialFor(hostNumber, index);
        var links = picture.HasLinks
            ? new List<LinkDto>
            {
                BuildLink(edgeId, serial, index, "GE1", picture.Link1, picture.WindowStart, now),
                BuildLink(edgeId, serial, index, "GE2", picture.Link2, picture.WindowStart, now),
            }
            : new List<LinkDto>();

        return new EdgeDto(
            edgeId,
            serial,
            $"edge-{serial}",
            ModelFor(index),
            picture.EdgeState,
            Iso(picture.WindowStart),
            Iso(picture.EdgeState == Connected ? now : picture.WindowStart),
            picture.EdgeState == NeverActivated ? OutOfService : InService,
            $"00000000-0000-4000-8000-{edgeId:x12}",
            5000 + hostNumber * 100 + (index % 40),
            links);
    }

    private static LinkDto BuildLink(
        int edgeId,
        string serial,
        int index,
        string interfaceName,
        string state,
        DateTimeOffset windowStart,
        DateTimeOffset now)
    {
        var slot = interfaceName == "GE1" ? 1 : 2;
        var lastActive = state == Stable ? now : windowStart;
        return new LinkDto(
            edgeId * 10 + slot,
            edgeId,
            interfaceName,
            $"{serial} {interfaceName}",
            $"10.{edgeId / 1_000_000}.{index % 250}.{slot}",
            state,
            Stable,
            "WAN",
            "ETHERNET",
            Iso(lastActive));
    }

    private static CompanyDeviceDto BuildCompanyDevice(
        int hostNumber,
        int index,
        string? serialOverride,
        int? edgeOverride)
    {
        var company = Companies[(Math.Max(index, 1) - 1) % Companies.Length];
        var serial = serialOverride ?? SerialFor(hostNumber, index);
        var edgeId = edgeOverride ?? EdgeIdFor(hostNumber, index);
        var siteNumber = hostNumber == 0 ? 9000 + index : hostNumber * 1000 + index;
        return new CompanyDeviceDto(
            serial,
            edgeId,
            company.Id,
            company.Name,
            $"SITE-{siteNumber}",
            $"{company.City} site {siteNumber}",
            company.City,
            company.Region,
            company.Offset,
            ManagementFor(index),
            TierFor(index),
            [
                new CompanyLinkDto("GE1", $"CKT-{serial}-GE1", "Metro Fiber", $"{company.City} primary"),
                new CompanyLinkDto("GE2", $"CKT-{serial}-GE2", "Coast Cable", $"{company.City} backup"),
            ]);
    }

    private static string ManagementFor(int index)
    {
        if (index % 23 == 0)
            return "Unmanaged";
        if (index % 10 == 0)
            return "Managed - Priority";
        return "Managed";
    }

    private static string TierFor(int index)
    {
        if (index % 7 == 0)
            return "Silver";
        if (index % 3 == 0)
            return "Gold";
        return "Platinum";
    }

    private static string ModelFor(int index) => (index % 4) switch
    {
        0 => "edge680",
        1 => "edge610",
        2 => "edge640",
        _ => "edge710",
    };

    private static Picture PictureFor(int index, DateTimeOffset now)
    {
        var minute = now.ToUnixTimeSeconds() / 60;
        var stuck = DateTimeOffset.Parse("2026-09-01T00:00:00Z");

        if (index is >= 1 and <= 3)
            return new Picture(Offline, Disconnected, Disconnected, true, stuck);
        if (index is >= 4 and <= 6)
            return EdgeWindow(minute, downMinutes: 3, upMinutes: 9);
        if (index is >= 7 and <= 9)
            return EdgeWindow(minute, downMinutes: 8, upMinutes: 8);
        if (index is >= 10 and <= 12)
            return LinkWindow(minute, downMinutes: 6, upMinutes: 10);
        if (index is >= 13 and <= 15)
            return LinkWindow(minute, downMinutes: 14, upMinutes: 10);
        if (index is >= 16 and <= 18)
            return new Picture(Connected, Disconnected, Stable, true, stuck);
        if (index == 20)
            return new Picture(NeverActivated, Disconnected, Disconnected, false, stuck);

        return new Picture(Connected, Stable, Stable, true, stuck);
    }

    private static Picture EdgeWindow(long minute, int downMinutes, int upMinutes)
    {
        var (down, start) = Window(minute, downMinutes, upMinutes);
        return down
            ? new Picture(Offline, Disconnected, Disconnected, true, start)
            : new Picture(Connected, Stable, Stable, true, start);
    }

    private static Picture LinkWindow(long minute, int downMinutes, int upMinutes)
    {
        var (down, start) = Window(minute, downMinutes, upMinutes);
        return new Picture(Connected, down ? Disconnected : Stable, Stable, true, start);
    }

    private static (bool Down, DateTimeOffset Start) Window(long minute, int downMinutes, int upMinutes)
    {
        var cycle = downMinutes + upMinutes;
        var pos = (int)(minute % cycle);
        var down = pos < downMinutes;
        var startMinute = down ? minute - pos : minute - (pos - downMinutes);
        return (down, DateTimeOffset.FromUnixTimeSeconds(startMinute * 60));
    }

    private static string Iso(DateTimeOffset value) =>
        value.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss'Z'");

    private readonly record struct Picture(
        string EdgeState,
        string Link1,
        string Link2,
        bool HasLinks,
        DateTimeOffset WindowStart);

    private readonly record struct CompanyProfile(
        string Id,
        string Name,
        string City,
        string Region,
        int Offset);

    private static readonly CompanyProfile[] Companies =
    [
        new("C-NORTH", "Northwind Clinics", "Chicago", "Illinois", -5),
        new("C-HARBOR", "Harbor Schools", "Newark", "New Jersey", -4),
        new("C-CONTOSO", "Contoso Logistics", "Dallas", "Texas", -5),
        new("C-FAB", "Fabrikam Retail", "Seattle", "Washington", -8),
        new("C-ADV", "Adventure Works", "Phoenix", "Arizona", -7),
        new("C-DATUM", "Datum Research", "Reykjavik", "Iceland", 0),
    ];
}
