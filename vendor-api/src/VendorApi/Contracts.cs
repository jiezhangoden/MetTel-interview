using System.Text.Json.Serialization;

namespace VendorApi;

public sealed record HealthResponse(string Status);

public sealed record ApiError(string Error);

public sealed record VendorHost(
    string Host,
    string DevicesPath,
    int DeviceCount,
    int DelayMinutes);

public sealed record VendorDevicesResponse(
    string Host,
    string ObservedAt,
    IReadOnlyList<EdgeDto> Edges);

public sealed record EdgeDto(
    int Id,
    string SerialNumber,
    string Name,
    string ModelNumber,
    string EdgeState,
    string EdgeStateTime,
    string LastContact,
    string ServiceState,
    string LogicalId,
    int VendorSiteId,
    IReadOnlyList<LinkDto> Links);

public sealed record LinkDto(
    int Id,
    int EdgeId,
    [property: JsonPropertyName("interface")] string Interface,
    string DisplayName,
    string IpAddress,
    string State,
    string VpnState,
    string NetworkSide,
    string NetworkType,
    string LastActive);

public sealed record CompanyInventoryResponse(
    string ObservedAt,
    IReadOnlyList<CompanyDeviceDto> Devices);

public sealed record CompanyDeviceDto(
    string SerialNumber,
    int EdgeId,
    string CompanyId,
    string CompanyName,
    string SiteId,
    string SiteName,
    string City,
    string Region,
    int UtcOffsetHours,
    string ManagementStatus,
    string ServiceTier,
    IReadOnlyList<CompanyLinkDto> Links);

public sealed record CompanyLinkDto(
    [property: JsonPropertyName("interface")] string Interface,
    string CircuitId,
    string Carrier,
    string DisplayName);
