using Microsoft.AspNetCore.Mvc;

namespace DataBridgeAPI.Lambda.Controllers;

/// <summary>
/// Simple liveness check — used by load balancers and monitoring tools.
/// GET /health
/// </summary>
[ApiController]
[Route("health")]
public class HealthController : ControllerBase
{
    private readonly ILogger<HealthController> _logger;

    public HealthController(ILogger<HealthController> logger) => _logger = logger;

    /// <summary>Returns 200 OK when the service is running.</summary>
    [HttpGet]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public IActionResult Get()
    {
        _logger.LogInformation("GET /health");
        return Ok(new
        {
            status = "healthy",
            timestamp = DateTime.UtcNow
        });
    }
}
