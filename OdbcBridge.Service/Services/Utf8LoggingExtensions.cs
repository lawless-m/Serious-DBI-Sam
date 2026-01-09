using System.Text;
using Microsoft.Extensions.Logging;

namespace OdbcBridge.Services;

/// <summary>
/// A utility class for writing logs directly to a UTF-8 encoded file without using the logging framework.
/// </summary>
public static class UTF8Writer
{
    private static readonly object _lock = new object();
    private static string _logFileTemplate = string.Empty;

    /// <summary>
    /// Initializes the UTF8Writer with the specified log file template.
    /// The template should contain {0} where the date will be inserted (e.g., "logs/app_{0}.log").
    /// If no {0} is found, "_{0}" will be inserted before the file extension.
    /// If the path appears to be a directory (no extension and exists or ends with separator),
    /// the directory name will be used as the log file base name within that directory.
    /// </summary>
    /// <param name="filenameTemplate">The path template for log files</param>
    /// <param name="useConsoleAsFallback">Whether to output to console on error</param>
    public static void Init(string filenameTemplate, bool useConsoleAsFallback = false)
    {
        // If no {0} placeholder found, insert it before the extension
        if (!filenameTemplate.Contains("{0}"))
        {
            var extension = Path.GetExtension(filenameTemplate);

            // Check if this looks like a directory path (no extension and ends with separator, or directory exists)
            if (string.IsNullOrEmpty(extension) &&
                (filenameTemplate.EndsWith(Path.DirectorySeparatorChar.ToString()) ||
                 filenameTemplate.EndsWith(Path.AltDirectorySeparatorChar.ToString()) ||
                 Directory.Exists(filenameTemplate)))
            {
                // Treat as directory - use directory name as the log file base name
                var dirPath = filenameTemplate.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
                var dirName = Path.GetFileName(dirPath);
                _logFileTemplate = Path.Combine(dirPath, $"{dirName}_{{{0}}}.log");
            }
            else
            {
                // Treat as file path
                var directory = Path.GetDirectoryName(filenameTemplate);
                var nameWithoutExt = Path.GetFileNameWithoutExtension(filenameTemplate);

                // Default to .log extension if none provided
                if (string.IsNullOrEmpty(extension))
                {
                    extension = ".log";
                }

                if (string.IsNullOrEmpty(directory))
                    _logFileTemplate = $"{nameWithoutExt}_{{{0}}}{extension}";
                else
                    _logFileTemplate = Path.Combine(directory, $"{nameWithoutExt}_{{{0}}}{extension}");
            }
        }
        else
        {
            _logFileTemplate = filenameTemplate;
        }

        // Ensure directory exists
        var logDir = Path.GetDirectoryName(GetCurrentLogFile());
        if (!string.IsNullOrEmpty(logDir) && !Directory.Exists(logDir))
        {
            Directory.CreateDirectory(logDir);
        }
    }

    /// <summary>
    /// Gets the current log file path based on today's date (YYYY_MM_DD format).
    /// </summary>
    private static string GetCurrentLogFile()
    {
        var dateString = DateTime.Now.ToString("yyyy_MM_dd");
        return string.Format(_logFileTemplate, dateString);
    }

    /// <summary>
    /// Writes a log message to the UTF-8 encoded log file.
    /// </summary>
    /// <param name="message">The message to log</param>
    /// <param name="useConsoleAsFallback">Whether to use console as a fallback when no log file is specified</param>
    public static void Log(string message, bool useConsoleAsFallback = false)
    {
        try
        {
            if (string.IsNullOrEmpty(_logFileTemplate))
            {
                if (useConsoleAsFallback)
                {
                    Console.WriteLine(message);
                }
                return;
            }

            var currentLogFile = GetCurrentLogFile();

            lock (_lock)
            {
                using var stream = new FileStream(currentLogFile, FileMode.Append, FileAccess.Write, FileShare.Read);
                using var writer = new StreamWriter(stream, new UTF8Encoding(false));
                writer.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} {message}");
            }
        }
        catch (Exception ex)
        {
            if (useConsoleAsFallback)
            {
                Console.WriteLine($"Error writing to log file: {ex.Message}");
            }
        }
    }
}

/// <summary>
/// A helper class for getting string representations of log levels.
/// </summary>
public static class LogLevelHelper
{
    /// <summary>
    /// Gets a string representation of a log level.
    /// </summary>
    public static string GetLogLevelString(LogLevel logLevel)
    {
        return logLevel switch
        {
            LogLevel.Trace => "TRACE",
            LogLevel.Debug => "DEBUG",
            LogLevel.Information => "INFO",
            LogLevel.Warning => "WARN",
            LogLevel.Error => "ERROR",
            LogLevel.Critical => "CRIT",
            _ => "NONE"
        };
    }
}

/// <summary>
/// A custom UTF-8 file logger provider that ensures all logs are properly encoded in UTF-8.
/// </summary>
public class Utf8FileLoggerProvider : ILoggerProvider
{
    private readonly bool _useConsoleAsFallback;

    public Utf8FileLoggerProvider(bool useConsoleAsFallback = false)
    {
        _useConsoleAsFallback = useConsoleAsFallback;
    }

    public ILogger CreateLogger(string categoryName)
    {
        return new Utf8FileLogger(categoryName, _useConsoleAsFallback);
    }

    public void Dispose() { }

    private class Utf8FileLogger : ILogger
    {
        private readonly string _categoryName;
        private readonly bool _useConsoleAsFallback;

        public Utf8FileLogger(string categoryName, bool useConsoleAsFallback)
        {
            _categoryName = categoryName;
            _useConsoleAsFallback = useConsoleAsFallback;
        }

        // Explicit interface implementation to avoid nullability warnings
        IDisposable ILogger.BeginScope<TState>(TState state) => default!;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel)) return;

            var message = formatter(state, exception);
            var logLevelString = LogLevelHelper.GetLogLevelString(logLevel);

            UTF8Writer.Log($"[{_categoryName}] {logLevelString}: {message}", _useConsoleAsFallback);

            if (exception != null)
            {
                UTF8Writer.Log($"[{_categoryName}] Exception: {exception}", _useConsoleAsFallback);
            }
        }
    }
}

/// <summary>
/// Extension methods for configuring UTF-8 logging.
/// </summary>
public static class Utf8LoggingExtensions
{
    /// <summary>
    /// Adds UTF-8 file logging to the logging builder.
    /// </summary>
    /// <param name="builder">The logging builder to configure</param>
    /// <param name="logFilePath">Log file path or directory. If directory, uses directory name as log file base.</param>
    /// <returns>The logging builder for chaining</returns>
    public static ILoggingBuilder AddUtf8FileLogger(this ILoggingBuilder builder, string logFilePath)
    {
        UTF8Writer.Init(logFilePath);
        return builder.AddProvider(new Utf8FileLoggerProvider(false));
    }
}
