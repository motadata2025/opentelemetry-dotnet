// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using System.Collections;
using System.Diagnostics;
using System.Net;
using System.Text;
using Newtonsoft.Json.Linq;
using OpenTelemetry.Exporter.OpenTelemetryProtocol.Implementation;
using OpenTelemetry.Exporter.OpenTelemetryProtocol.Implementation.Transmission;
using OtlpCollector = OpenTelemetry.Proto.Collector.Trace.V1;
using OtlpResource = OpenTelemetry.Proto.Resource.V1;

namespace OpenTelemetry.Exporter;

/// <summary>
/// Exporter consuming <see cref="Activity"/> and exporting the data using
/// the OpenTelemetry protocol (OTLP).
/// </summary>
public class OtlpTraceExporter : BaseExporter<Activity>
{
    private readonly SdkLimitOptions sdkLimitOptions;
    private readonly OtlpExporterTransmissionHandler<OtlpCollector.ExportTraceServiceRequest> transmissionHandler;

    private OtlpResource.Resource processResource;

    private static readonly string PATH_SEPARATOR = Path.DirectorySeparatorChar.ToString();

    private static readonly string CONFIG_DIR = "config";

    private static readonly string AGENT_CONFIG_FILENAME = "agent.json";

    private static readonly String AGENT_RUNNING_STATUS_PATH = "/agent/agent.service.status";

    private static readonly String AGENT_STATE_PATH = "/agent/agent.state";

    private static readonly String TRACE_AGENT_STATE_PATH = "/agent/trace.agent.status";

    private static readonly string AGENT_INSTALL_DIR = GetAgentDirectory();

    private static string DEFAULT_SERVICE_NAME = "unknown_service";

    private static string TRACE_FILE_FORMAT = "trace-{0}-{1}.cache";

    private string serviceName;

    private static readonly string MOTADATA_TRACE_SERVICE_CHECK_TIME = "MOTADATA_TRACE_SERVICE_CHECK_TIME_SEC";

    private static readonly int SERVICE_CHECK_INTERVAL = GetIntervalToCheckConfiguration();

    private Timer timer;

    private volatile bool isShutdown;

    public static readonly string DATA_DIR = AGENT_INSTALL_DIR + "cache" + PATH_SEPARATOR;

    /// <summary>
    /// Initializes a new instance of the <see cref="OtlpTraceExporter"/> class.
    /// </summary>
    /// <param name="options">Configuration options for the export.</param>
    public OtlpTraceExporter(OtlpExporterOptions options)
        : this(options, sdkLimitOptions: new(), experimentalOptions: new(), transmissionHandler: null)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="OtlpTraceExporter"/> class.
    /// </summary>
    /// <param name="exporterOptions"><see cref="OtlpExporterOptions"/>.</param>
    /// <param name="sdkLimitOptions"><see cref="SdkLimitOptions"/>.</param>
    /// <param name="experimentalOptions"><see cref="ExperimentalOptions"/>.</param>
    /// <param name="transmissionHandler"><see cref="OtlpExporterTransmissionHandler{T}"/>.</param>
    internal OtlpTraceExporter(
        OtlpExporterOptions exporterOptions,
        SdkLimitOptions sdkLimitOptions,
        ExperimentalOptions experimentalOptions,
        OtlpExporterTransmissionHandler<OtlpCollector.ExportTraceServiceRequest> transmissionHandler = null)
    {
        Debug.Assert(exporterOptions != null, "exporterOptions was null");
        Debug.Assert(sdkLimitOptions != null, "sdkLimitOptions was null");

        this.sdkLimitOptions = sdkLimitOptions;

        this.transmissionHandler = transmissionHandler ?? exporterOptions.GetTraceExportTransmissionHandler(experimentalOptions);
    }

    internal OtlpResource.Resource ProcessResource => this.processResource ??= this.ParentProvider.GetResource().ToOtlpResource();

    /// <inheritdoc/>
    public override ExportResult Export(in Batch<Activity> activityBatch)
    {
        Console.WriteLine("printing the path separator : " + PATH_SEPARATOR);
        // Prevents the exporter's gRPC and HTTP operations from being instrumented.
        using var scope = SuppressInstrumentationScope.Begin();

        var request = new OtlpCollector.ExportTraceServiceRequest();

        request.AddBatch(this.sdkLimitOptions, this.ProcessResource, activityBatch);

        if (string.IsNullOrEmpty(serviceName))
        {
            SetServiceName();
            Console.WriteLine("service name : " + serviceName);
            this.ScheduleJobToCheckConfiguration();
        }

        if (!isShutdown)
        {
            try
            {
                Console.WriteLine("--------------Printing Start -------------");
                Console.WriteLine(request.ToString());
                Console.WriteLine("---------------Printing End --------------");

                Console.WriteLine("Exporting...");
                byte[] jsonBytes = Encoding.UTF8.GetBytes(request.ToString());
                Console.WriteLine($"Size before Compression : {jsonBytes.Length}");

                byte[] compressData = IronSnappy.Snappy.Encode(jsonBytes);

                Console.WriteLine($"size after compression: {compressData.Length}");


                byte[] reverseData = IronSnappy.Snappy.Decode(compressData);
                string json = Encoding.UTF8.GetString(reverseData);
                Console.WriteLine("After reverse engineering");
                Console.WriteLine(json);
                long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                string fileName = string.Format(TRACE_FILE_FORMAT, this.serviceName, timestamp);
                string filePath = Path.Combine(DATA_DIR, fileName);

                // Write the compressed trace data to file.
                File.WriteAllBytes(filePath, compressData);

                Console.WriteLine($"Trace written to file: {filePath}");


                if (!this.transmissionHandler.TrySubmitRequest(request))
                {
                    return ExportResult.Failure;
                }
            }
            catch (Exception ex)
            {
                OpenTelemetryProtocolExporterEventSource.Log.ExportMethodException(ex);
                return ExportResult.Failure;
            }
            finally
            {
                request.Return();
            }
        }

        return ExportResult.Success;
    }

    /// <inheritdoc />
    protected override bool OnShutdown(int timeoutMilliseconds)
    {
        return this.transmissionHandler.Shutdown(timeoutMilliseconds);
    }

    private static string GetAgentDirectory()
    {
        var otelfolder = GetEnvironmentVariable("OTEL_DOTNET_AUTO_HOME");

        var dirInfo = new DirectoryInfo(otelfolder);

        if (dirInfo.Parent == null)
        {
            throw new InvalidOperationException("Unable to determine the directory of the MotadataAgent.");
        }

        Console.WriteLine("Motadata Agent Path  : " + dirInfo.Parent.FullName + PATH_SEPARATOR);
        return dirInfo.Parent.FullName + PATH_SEPARATOR;
    }

    private void SetServiceName()
    {
        var name = GetEnvironmentVariable("OTEL_SERVICE_NAME");
        this.serviceName = string.IsNullOrEmpty(name) ? DEFAULT_SERVICE_NAME : name;
        Console.WriteLine("Service name : " + this.serviceName);
    }


    private static int GetIntervalToCheckConfiguration()
    {
        var value = Environment.GetEnvironmentVariable(MOTADATA_TRACE_SERVICE_CHECK_TIME);
        if (string.IsNullOrEmpty(value))
        {
            return 30;
        }

        var parsed = int.TryParse(value, out int interval);

        if (!parsed)
        {
            throw new InvalidCastException("Invalid MOTADATA_TRACE_SERVICE_CHECK_TIME");
        }

        return Math.Min(Math.Max(interval, 30), 120);
    }

    private void ScheduleJobToCheckConfiguration()
    {
        timer = new Timer(UpdateExportFlag, null, 0, SERVICE_CHECK_INTERVAL * 1000);
    }

    private void UpdateExportFlag(object state)
    {
        Console.WriteLine("Updating export flag..........................");
        string configPath = AGENT_INSTALL_DIR + CONFIG_DIR + PATH_SEPARATOR + AGENT_CONFIG_FILENAME;
        Console.WriteLine("config path for agent json: " + configPath);

        try
        {
            // Read the JSON configuration file.
            string jsonText = File.ReadAllText(configPath);
            JObject rootNode = JObject.Parse(jsonText);

            // Retrieve values using SelectToken.
            string agentRunningStatus = (string)rootNode.SelectToken("agent['agent.service.status']") ?? "";
            string agentState = (string)rootNode.SelectToken("agent['agent.state']") ?? "";
            string traceAgentState = (string)rootNode.SelectToken("agent['trace.agent.status']") ?? "";
            string serviceTraceState = (string)rootNode.SelectToken( $"['trace.agent']['{serviceName}']['service.trace.state']") ?? "";


            // Determine if the agent is running based on multiple conditions.
            bool isAgentRunning =
                agentRunningStatus.Equals("running", StringComparison.OrdinalIgnoreCase) &&
                agentState.Equals("enable", StringComparison.OrdinalIgnoreCase) &&
                traceAgentState.Equals("yes", StringComparison.OrdinalIgnoreCase) &&
                serviceTraceState.Equals("yes", StringComparison.OrdinalIgnoreCase);

            // Log values (using Console.WriteLine for simplicity)
            Console.WriteLine($"{AGENT_RUNNING_STATUS_PATH} : {agentRunningStatus}");
            Console.WriteLine($"{AGENT_STATE_PATH} : {agentState}");
            Console.WriteLine($"{TRACE_AGENT_STATE_PATH} : {traceAgentState}");
            Console.WriteLine($"{serviceTraceState} : {serviceTraceState}");
            Console.WriteLine("Agent running status : " + isAgentRunning);
            Console.WriteLine("Agent dir " + AGENT_INSTALL_DIR);

            // Update shutdown status (set isShutdown to the opposite of isAgentRunning)
            isShutdown = !isAgentRunning;
        }
        catch (Exception ex)
        {
            // Log the exception message as a warning.
            Console.WriteLine("Warning: " + ex.Message);
        }
    }


    private static string GetEnvironmentVariable(string variable, bool throwIfNotFound = false)
    {
        var value = Environment.GetEnvironmentVariable(variable);

        if (string.IsNullOrEmpty(value) && throwIfNotFound)
        {
            throw new InvalidOperationException($"Unable to determine the environment variable : {variable}");
        }

        return value;
    }
}
