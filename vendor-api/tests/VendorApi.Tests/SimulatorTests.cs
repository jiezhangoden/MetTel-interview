using VendorApi;
using Xunit;

namespace VendorApi.Tests;

public class SimulatorTests
{
    [Fact]
    public void Hosts_have_the_agreed_device_counts_and_delays()
    {
        var hosts = Simulator.HostCatalog();

        Assert.Equal(["host_1", "host_2", "host_3"], hosts.Select(host => host.Host));
        Assert.Equal([100, 200, 1000], hosts.Select(host => host.DeviceCount));
        Assert.Equal([1, 2, 3], hosts.Select(host => host.DelayMinutes));
        Assert.Equal(TimeSpan.FromMinutes(1), Simulator.ResponseDelay(1, scale: 1));
        Assert.Equal(TimeSpan.FromMinutes(2), Simulator.ResponseDelay(2, scale: 1));
        Assert.Equal(TimeSpan.FromMinutes(3), Simulator.ResponseDelay(3, scale: 1));
        Assert.Equal(TimeSpan.Zero, Simulator.ResponseDelay(3, scale: 0));
    }

    [Fact]
    public void Vendor_snapshot_is_stable_for_the_same_instant()
    {
        var now = DateTimeOffset.Parse("2026-09-28T08:00:00Z");

        var first = Simulator.VendorSnapshot("host_1", now);
        var second = Simulator.VendorSnapshot("host_1", now);

        Assert.Equal(100, first.Edges.Count);
        Assert.Equal(first.Edges.Select(edge => edge.SerialNumber), second.Edges.Select(edge => edge.SerialNumber));
        Assert.Equal("SIM-1-00001", first.Edges[0].SerialNumber);
        Assert.Equal(1_000_001, first.Edges[0].Id);
        Assert.Equal(200, Simulator.VendorSnapshot("host_2", now).Edges.Count);
        Assert.Equal(1000, Simulator.VendorSnapshot("host_3", now).Edges.Count);
    }

    [Fact]
    public void Scripted_edges_change_only_on_their_clock()
    {
        var down = AtMinute(0);
        var up = AtMinute(3);
        var stillDown = AtMinute(60);

        var sticky = Edge("host_1", 1, down);
        Assert.Equal(Simulator.Offline, sticky.EdgeState);
        Assert.Equal(sticky.EdgeState, Edge("host_1", 1, stillDown).EdgeState);
        Assert.All(sticky.Links, link => Assert.Equal(Simulator.Disconnected, link.State));

        var shortBlipDown = Edge("host_1", 4, down);
        var shortBlipUp = Edge("host_1", 4, up);
        Assert.Equal(Simulator.Offline, shortBlipDown.EdgeState);
        Assert.Equal(Simulator.Connected, shortBlipUp.EdgeState);

        var longOutage = Edge("host_1", 7, AtMinute(0));
        Assert.Equal(Simulator.Offline, longOutage.EdgeState);
        Assert.Equal(Simulator.Connected, Edge("host_1", 7, AtMinute(8)).EdgeState);

        var shortLink = Edge("host_2", 10, down);
        Assert.Equal(Simulator.Connected, shortLink.EdgeState);
        Assert.Equal(Simulator.Disconnected, Link(shortLink, "GE1").State);
        Assert.Equal(Simulator.Stable, Link(shortLink, "GE2").State);
        Assert.Equal(Simulator.Stable, Link(Edge("host_2", 10, AtMinute(6)), "GE1").State);

        var longLink = Edge("host_3", 13, AtMinute(0));
        Assert.Equal(Simulator.Disconnected, Link(longLink, "GE1").State);
        Assert.Equal(Simulator.Stable, Link(Edge("host_3", 13, AtMinute(14)), "GE1").State);

        var stuckLink = Edge("host_1", 16, up);
        Assert.Equal(Simulator.Connected, stuckLink.EdgeState);
        Assert.Equal(Simulator.Disconnected, Link(stuckLink, "GE1").State);

        var never = Edge("host_1", 20, up);
        Assert.Equal(Simulator.NeverActivated, never.EdgeState);
        Assert.Empty(never.Links);
    }

    [Fact]
    public void Company_inventory_joins_on_serial_and_leaves_gaps()
    {
        var inventory = Simulator.CompanyInventory(DateTimeOffset.Parse("2026-09-28T08:00:00Z"));
        var bySerial = inventory.Devices.ToDictionary(device => device.SerialNumber);

        Assert.Equal(100 + 200 + 1000 - 3 + 5, inventory.Devices.Count);
        Assert.DoesNotContain("SIM-1-00019", bySerial.Keys);
        Assert.DoesNotContain("SIM-2-00019", bySerial.Keys);
        Assert.DoesNotContain("SIM-3-00019", bySerial.Keys);

        var matched = bySerial["SIM-2-00007"];
        Assert.Equal(Simulator.EdgeIdFor(2, 7), matched.EdgeId);
        Assert.Equal(2, matched.Links.Count);
        Assert.Contains(matched.CompanyName, new[]
        {
            "Northwind Clinics",
            "Harbor Schools",
            "Contoso Logistics",
            "Fabrikam Retail",
            "Adventure Works",
            "Datum Research",
        });

        var orphan = bySerial["COMPANY-ONLY-001"];
        Assert.Equal(9_000_001, orphan.EdgeId);
        Assert.Equal("Northwind Clinics", orphan.CompanyName);

        var datum = bySerial["SIM-1-00006"];
        Assert.Equal("Datum Research", datum.CompanyName);
        Assert.Equal(0, datum.UtcOffsetHours);
    }

    private static EdgeDto Edge(string host, int index, DateTimeOffset now) =>
        Simulator.VendorSnapshot(host, now).Edges[index - 1];

    private static LinkDto Link(EdgeDto edge, string interfaceName) =>
        Assert.Single(edge.Links, link => link.Interface == interfaceName);

    private static DateTimeOffset AtMinute(long minute) =>
        DateTimeOffset.FromUnixTimeSeconds(minute * 60);
}
