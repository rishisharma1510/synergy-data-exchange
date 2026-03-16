using System.Diagnostics;
using Amazon.CDK;

namespace DataBridgeAPI.Cdk;

/// <summary>
/// Attempts to run `dotnet publish` locally before falling back to the Docker bundling image.
/// This is much faster on developer machines where the .NET SDK is already installed.
/// </summary>
public class LocalDotNetBundling : ILocalBundling
{
    private readonly string _projectPath;

    public LocalDotNetBundling(string projectPath)
    {
        _projectPath = projectPath;
    }

    public bool TryBundle(string outputDir, IBundlingOptions options)
    {
        if (!IsDotNetSdkInstalled())
        {
            Console.WriteLine("[CDK] Local .NET SDK not found — falling back to Docker bundling.");
            return false;
        }

        Console.WriteLine($"[CDK] Bundling Lambda locally from {_projectPath}");

        var psi = new ProcessStartInfo
        {
            FileName = "dotnet",
            Arguments = $"publish \"{_projectPath}\" " +
                        $"-c Release " +
                        $"-r linux-x64 " +
                        $"--self-contained false " +
                        $"-o \"{outputDir}\"",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };

        using var process = Process.Start(psi);
        if (process == null) return false;

        // Stream output so progress is visible in `cdk deploy`
        process.OutputDataReceived += (_, e) =>
        {
            if (e.Data != null) Console.WriteLine($"  dotnet: {e.Data}");
        };
        process.ErrorDataReceived += (_, e) =>
        {
            if (e.Data != null) Console.Error.WriteLine($"  dotnet: {e.Data}");
        };

        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        process.WaitForExit();

        if (process.ExitCode != 0)
        {
            Console.Error.WriteLine($"[CDK] dotnet publish failed with exit code {process.ExitCode}.");
            return false;
        }

        Console.WriteLine("[CDK] Local bundling succeeded.");
        return true;
    }

    private static bool IsDotNetSdkInstalled()
    {
        try
        {
            using var p = Process.Start(new ProcessStartInfo
            {
                FileName = "dotnet",
                Arguments = "--version",
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            });
            p?.WaitForExit();
            return p?.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }
}
