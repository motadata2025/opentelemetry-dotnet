// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using System.Buffers.Binary;
using System.Diagnostics;
using System.Text;
using OpenTelemetry.Exporter.OpenTelemetryProtocol;
using OpenTelemetry.Exporter.OpenTelemetryProtocol.Implementation;
using OpenTelemetry.Exporter.OpenTelemetryProtocol.Implementation.Serializer;
using OpenTelemetry.Exporter.OpenTelemetryProtocol.Implementation.Transmission;
using OpenTelemetry.Resources;

namespace OpenTelemetry.Exporter;

/// <summary>
/// Exporter consuming <see cref="Activity"/> and exporting the data using
/// the OpenTelemetry protocol (OTLP).
/// </summary>
public class OtlpTraceExporter : BaseExporter<Activity>
{
    private const int GrpcStartWritePosition = 5;
    private readonly SdkLimitOptions sdkLimitOptions;
    private readonly OtlpExporterTransmissionHandler transmissionHandler;
    private readonly int startWritePosition;

    private Resource? resource;

    // Initial buffer size set to ~732KB.
    // This choice allows us to gradually grow the buffer while targeting a final capacity of around 100 MB,
    // by the 7th doubling to maintain efficient allocation without frequent resizing.
    private byte[] buffer = new byte[750000];

    private AgentConfigurationProvider agentConfigurationProvider;

    private static string DEFAULT_SERVICE_NAME = "unknown_service";

    private string serviceName;

    private static readonly int SERVICE_CHECK_INTERVAL = GetIntervalToCheckConfiguration();

    private Timer timer;

    private volatile bool isShutdown;

    // vars for Path/Dir
    private static readonly string PATH_SEPARATOR = Path.DirectorySeparatorChar.ToString();

    private static readonly string CONFIG_DIR = "config";

    private static readonly string AGENT_INSTALL_DIR = GetAgentDirectory();

    private static readonly string DATA_DIR = AGENT_INSTALL_DIR + "cache" + PATH_SEPARATOR;

    private static string TRACE_FILE_FORMAT = "trace-{0}-{1}.cache";

    // Constants for ENV var
    private static readonly string ENV_MOTADATA_TRACE_SERVICE_CHECK_TIME = "MOTADATA_TRACE_SERVICE_CHECK_TIME_SEC";


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
    /// <param name="transmissionHandler"><see cref="OtlpExporterTransmissionHandler"/>.</param>
    internal OtlpTraceExporter(
        OtlpExporterOptions exporterOptions,
        SdkLimitOptions sdkLimitOptions,
        ExperimentalOptions experimentalOptions,
        OtlpExporterTransmissionHandler? transmissionHandler = null)
    {
        Debug.Assert(exporterOptions != null, "exporterOptions was null");
        Debug.Assert(sdkLimitOptions != null, "sdkLimitOptions was null");

        this.agentConfigurationProvider = new AgentConfigurationProvider();

        this.sdkLimitOptions = sdkLimitOptions!;
#if NET462_OR_GREATER || NETSTANDARD2_0
        this.startWritePosition = 0;
#else
        this.startWritePosition = exporterOptions!.Protocol == OtlpExportProtocol.Grpc ? GrpcStartWritePosition : 0;
#endif
        this.transmissionHandler = transmissionHandler ?? exporterOptions!.GetExportTransmissionHandler(experimentalOptions, OtlpSignalType.Traces);
    }

    internal Resource Resource => this.resource ??= this.ParentProvider.GetResource();

    /// <inheritdoc/>
#pragma warning disable CA1725 // Parameter names should match base declaration
    public override ExportResult Export(in Batch<Activity> activityBatch)
#pragma warning restore CA1725 // Parameter names should match base declaration
    {
        // Prevents the exporter's gRPC and HTTP operations from being instrumented.
        using var scope = SuppressInstrumentationScope.Begin();

        if (string.IsNullOrEmpty(serviceName))
        {
            SetServiceName();
            Console.WriteLine("service name : " + serviceName);
            this.ScheduleJobToCheckConfiguration();
        }

        try
        {
            int writePosition = ProtobufOtlpTraceSerializer.WriteTraceData(ref this.buffer, this.startWritePosition, this.sdkLimitOptions, this.Resource, activityBatch);

            Console.WriteLine("This is writeLine position");
            Console.WriteLine(writePosition);

            if (this.startWritePosition == GrpcStartWritePosition)
            {
                Console.WriteLine("This is GrpcStartWritePosition");
                // Grpc payload consists of 3 parts
                // byte 0 - Specifying if the payload is compressed.
                // 1-4 byte - Specifies the length of payload in big endian format.
                // 5 and above -  Protobuf serialized data.
                Span<byte> data = new Span<byte>(this.buffer, 1, 4);
                var dataLength = writePosition - GrpcStartWritePosition;
                BinaryPrimitives.WriteUInt32BigEndian(data, (uint)dataLength);
            }

            string dataBeforeSerlization = Encoding.UTF8.GetString(this.buffer, 0, writePosition);

            Console.WriteLine("Data before the deserlization");
            Console.WriteLine(dataBeforeSerlization);
            Console.WriteLine("--------------Printing Start -------------");
            byte[] serializedData = new byte[writePosition];
            Buffer.BlockCopy(this.buffer, 0, serializedData, 0, writePosition);

            string text = Encoding.UTF8.GetString(serializedData, 0, writePosition);
            Console.WriteLine(text);

            Console.WriteLine("Size before Compression : " + serializedData.Length);
            var compressData = IronSnappy.Snappy.Encode(serializedData);
            Console.WriteLine("Size after Compression : " +compressData.Length);

            long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            string fileName = string.Format(TRACE_FILE_FORMAT, this.serviceName, timestamp);
            string filePath = Path.Combine(DATA_DIR, fileName);

            // Write the compressed trace data to file.
            File.WriteAllBytes(filePath, compressData);

            Console.WriteLine($"Trace written to file: {filePath}");

            if (!this.transmissionHandler.TrySubmitRequest(this.buffer, writePosition))
            {
                return ExportResult.Failure;
            }
        }
        catch (Exception ex)
        {
            OpenTelemetryProtocolExporterEventSource.Log.ExportMethodException(ex);
            return ExportResult.Failure;
        }

        return ExportResult.Success;
    }

    /// <inheritdoc />
    protected override bool OnShutdown(int timeoutMilliseconds)
    {
        this.timer?.Dispose();
        return this.transmissionHandler.Shutdown(timeoutMilliseconds);
    }

    private static string GetAgentDirectory()
    {
        var otelfolder = GetEnvironmentVariable("MOTADATA_INSTALLATION_PATH", true);
        Console.WriteLine("Agent Directory : " + otelfolder);
       return otelfolder + PATH_SEPARATOR;
    }

    private void SetServiceName()
    {
        var name = GetEnvironmentVariable("OTEL_SERVICE_NAME");
        this.serviceName = string.IsNullOrEmpty(name) ? DEFAULT_SERVICE_NAME : name;
        Console.WriteLine("Service name : " + this.serviceName);
    }


    private static int GetIntervalToCheckConfiguration()
    {
        var value = GetEnvironmentVariable(ENV_MOTADATA_TRACE_SERVICE_CHECK_TIME);
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
        string configPath = AGENT_INSTALL_DIR +CONFIG_DIR + PATH_SEPARATOR + "agent.json";
        Console.WriteLine("config path for agent json: " + configPath);

        var agentConfig = new AgentConfigurationProvider().GetConfiguration(configPath, serviceName);

        bool isAgentRunning =
                agentConfig.AgentServiceStatus.Equals("running", StringComparison.OrdinalIgnoreCase) &&
                agentConfig.AgentState.Equals("enable", StringComparison.OrdinalIgnoreCase) &&
                agentConfig.TraceAgentStatus.Equals("yes", StringComparison.OrdinalIgnoreCase) &&
                agentConfig.ServiceTraceState.Equals("yes", StringComparison.OrdinalIgnoreCase);

        Console.WriteLine("Agent configuration status: " + isAgentRunning);
        Console.WriteLine("Agent configuration service status: " + agentConfig.AgentServiceStatus);
        Console.WriteLine("Agent configuration status: " + agentConfig.AgentState);
        Console.WriteLine("Agent Trace status: " + agentConfig.TraceAgentStatus);
        Console.WriteLine("Service Trace state: " + agentConfig.ServiceTraceState);


        isShutdown = !isAgentRunning;
    }


    private static string GetEnvironmentVariable(string variable, bool throwIfNotFound = false)
    {
        string envValue = string.Empty;
        try
        {
            envValue = Environment.GetEnvironmentVariable(variable);
        }
        catch (Exception ex)
        {
            if (throwIfNotFound && string.IsNullOrEmpty(envValue))
            {
                Console.Error.WriteLine($"Environment variable {variable} is not defined.");
                throw new InvalidOperationException($"Unable to determine the environment variable : {variable}");
            }
        }
        return envValue;
    }
}
