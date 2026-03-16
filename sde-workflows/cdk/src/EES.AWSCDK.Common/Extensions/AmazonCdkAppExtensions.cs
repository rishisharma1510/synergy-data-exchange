using Amazon.CDK;

namespace EES.AWSCDK.Common.Extensions;

/// <summary>
/// Extension methods for Amazon CDK App
/// </summary>
public static class AmazonCdkAppExtensions
{
    /// <summary>
    /// Check if a context key is present and has a truthy value
    /// </summary>
    public static bool IsContextPresent(this App app, string contextKey)
    {
        var contextValue = app.Node.TryGetContext(contextKey);
        
        if (contextValue == null)
        {
            return false;
        }

        // Check for boolean true
        if (contextValue is bool boolValue)
        {
            return boolValue;
        }

        // Check for string "true"
        if (contextValue is string stringValue)
        {
            return stringValue.Equals("true", StringComparison.OrdinalIgnoreCase) ||
                   stringValue.Equals("1", StringComparison.Ordinal) ||
                   stringValue.Equals("yes", StringComparison.OrdinalIgnoreCase);
        }

        // Any other non-null value is considered present
        return true;
    }

    /// <summary>
    /// Get context value as string, or return default
    /// </summary>
    public static string GetContextString(this App app, string contextKey, string defaultValue = "")
    {
        var contextValue = app.Node.TryGetContext(contextKey);
        return contextValue?.ToString() ?? defaultValue;
    }

    /// <summary>
    /// Get context value as int, or return default
    /// </summary>
    public static int GetContextInt(this App app, string contextKey, int defaultValue = 0)
    {
        var contextValue = app.Node.TryGetContext(contextKey);
        
        if (contextValue == null)
        {
            return defaultValue;
        }

        if (contextValue is int intValue)
        {
            return intValue;
        }

        if (int.TryParse(contextValue.ToString(), out var parsed))
        {
            return parsed;
        }

        return defaultValue;
    }
}
