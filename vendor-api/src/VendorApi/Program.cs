using Microsoft.OpenApi;
using VendorApi;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.ConfigureKestrel(options =>
{
    options.Limits.KeepAliveTimeout = TimeSpan.FromMinutes(10);
    options.Limits.RequestHeadersTimeout = TimeSpan.FromMinutes(10);
});

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "SD-WAN exercise API",
        Version = "v1",
        Description = "Fake vendor edge snapshots and company inventory. A vendor device call waits before it responds."
    });
});

var app = builder.Build();
var scale = app.Configuration.GetValue("VENDOR_DELAY_SCALE", 1d);

app.UseSwagger();
app.UseSwaggerUI(options =>
{
    options.SwaggerEndpoint("/swagger/v1/swagger.json", "SD-WAN exercise API");
    options.DocumentTitle = "SD-WAN exercise API";
});

app.MapGet("/health", () => Results.Ok(new HealthResponse("ok")))
    .WithName("Health")
    .WithTags("Health")
    .WithSummary("Liveness check.")
    .Produces<HealthResponse>();

app.MapGet("/vendor/hosts", () => Results.Ok(Simulator.HostCatalog()))
    .WithName("ListVendorHosts")
    .WithTags("Vendor")
    .WithSummary("Lists the three vendor hosts, their device counts, and their wait times.")
    .Produces<IReadOnlyList<VendorHost>>();

app.MapGet("/vendor/{host}/devices", async (string host, CancellationToken cancellationToken) =>
{
    if (!Simulator.TryParseHost(host, out var hostNumber, out _))
    {
        return Results.NotFound(new ApiError($"Unknown host '{host}'. Call GET /vendor/hosts."));
    }

    var delay = Simulator.ResponseDelay(hostNumber, scale);
    app.Logger.LogInformation(
        "Vendor host {Host} will answer in {DelaySeconds} seconds",
        host,
        delay.TotalSeconds);
    if (delay > TimeSpan.Zero)
        await Task.Delay(delay, cancellationToken);

    var snapshot = Simulator.VendorSnapshot(host, DateTimeOffset.UtcNow);
    app.Logger.LogInformation(
        "Vendor host {Host} answered with {EdgeCount} edges",
        host,
        snapshot.Edges.Count);
    return Results.Ok(snapshot);
})
    .WithName("GetVendorDevices")
    .WithTags("Vendor")
    .WithSummary("Edge snapshot for one vendor host.")
    .WithDescription("Waits a number of minutes equal to the host number before responding. host_1 waits 1 minute, host_2 waits 2, host_3 waits 3. observedAt is set after that wait. VENDOR_DELAY_SCALE multiplies the wait.")
    .Produces<VendorDevicesResponse>()
    .Produces<ApiError>(StatusCodes.Status404NotFound);

app.MapGet("/company/inventory", () =>
    Results.Ok(Simulator.CompanyInventory(DateTimeOffset.UtcNow)))
    .WithName("GetCompanyInventory")
    .WithTags("Company")
    .WithSummary("Company, site, and circuit rows. Answers immediately.")
    .Produces<CompanyInventoryResponse>();

app.Run();
