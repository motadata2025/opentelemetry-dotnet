// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using System.Buffers.Binary;
using System.Collections;
using System.Diagnostics;
using System.Net;
using System.Text;
using OpenTelemetry.Exporter.OpenTelemetryProtocol;
using OpenTelemetry.Exporter.OpenTelemetryProtocol.Implementation;
using OpenTelemetry.Exporter.OpenTelemetryProtocol.Implementation.Serializer;
using OpenTelemetry.Exporter.OpenTelemetryProtocol.Implementation.Transmission;
using OpenTelemetry.Resources;
using OtlpCollector = OpenTelemetry.Proto.Collector.Trace.V1;

namespace OpenTelemetry.Exporter;

/// <summary>
/// Exporter consuming <see cref="Activity"/> and exporting the data using
/// the OpenTelemetry protocol (OTLP).
/// </summary>
public class OtlpTraceExporter : BaseExporter<Activity>
{
    private const int GrpcStartWritePosition = 5;
    private readonly SdkLimitOptions sdkLimitOptions;

    private readonly OtlpExporterTransmissionHandler<OtlpCollector.ExportTraceServiceRequest> transmissionHandler;

    private readonly int startWritePosition;

    private Resource? resource;

    // Initial buffer size set to ~732KB.
    // This choice allows us to gradually grow the buffer while targeting a final capacity of around 100 MB,
    // by the 7th doubling to maintain efficient allocation without frequent resizing.
    private byte[] buffer = new byte[750000];

    private AgentConfigurationProvider agentConfigurationProvider;

    private static string defaultServiceName = "unknown_service";

    private string serviceName;

    private static readonly int serviceCheckInterval = GetIntervalToCheckConfiguration();

    private Timer timer;

    private volatile bool isShutdown;

    // vars for Path/Dir
    private static readonly string pathSeparator = Path.DirectorySeparatorChar.ToString();

    private static readonly string configDir = "config";

    private static readonly string agentInstallDir = GetAgentDirectory();

    private static readonly string dataDir = agentInstallDir + "cache" + pathSeparator;

    private static string traceFileFormat = "trace-{0}-{1}.cache";

    // Constants for ENV var
    private static readonly string envMotadataTraceServiceCheckTime = "MOTADATA_TRACE_SERVICE_CHECK_TIME_SEC";

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

        this.agentConfigurationProvider = new AgentConfigurationProvider();
#if NET462_OR_GREATER || NETSTANDARD2_0
        this.startWritePosition = 0;
#else
        this.startWritePosition = exporterOptions!.Protocol == OtlpExportProtocol.Grpc ? GrpcStartWritePosition : 0;
#endif
    }

    internal Resource Resource => this.resource ??= this.ParentProvider.GetResource();

    /// <inheritdoc/>
    public override ExportResult Export(in Batch<Activity> activityBatch)
    {
        Console.WriteLine("printing the path separator : " + pathSeparator);
        // Prevents the exporter's gRPC and HTTP operations from being instrumented.
        using var scope = SuppressInstrumentationScope.Begin();

        //var request = new OtlpCollector.ExportTraceServiceRequest();

        //request.AddBatch(this.sdkLimitOptions, this.ProcessResource, activityBatch);

        if (string.IsNullOrEmpty(serviceName))
        {
            SetServiceName();
            Console.WriteLine("service name : " + serviceName);
            this.ScheduleJobToCheckConfiguration();
        }

        try
        {
            int writePosition = ProtobufOtlpTraceSerializer.WriteTraceData(ref this.buffer, this.startWritePosition, this.sdkLimitOptions, this.Resource, activityBatch);
            Console.WriteLine("--------------Printing Start -------------");
            Console.WriteLine(writePosition);
            Console.WriteLine("---------------Printing End --------------");

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

            Console.WriteLine("Exporting...");
            string dataBeforeSerlization = Encoding.UTF8.GetString(this.buffer, 0, writePosition);

            byte[] serializedData = new byte[writePosition];
            Console.WriteLine($"Size before Compression : {serializedData.Length}");
            Buffer.BlockCopy(this.buffer, 0, serializedData, 0, writePosition);

            string text = Encoding.UTF8.GetString(serializedData, 0, writePosition);
            Console.WriteLine(text);

            byte[] compressData = IronSnappy.Snappy.Encode(serializedData);
            Console.WriteLine($"size after compression: {compressData.Length}");

            Console.WriteLine("compression through OpenTelemetry 1.9 protobuf");
            long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            string fileName = string.Format(traceFileFormat, this.serviceName, timestamp);
            string filePath = Path.Combine(dataDir, fileName);

            // Write the compressed trace data to file.
            File.WriteAllBytes(filePath, compressData);

            Console.WriteLine($"Trace written to file: {filePath}");

            // if (!this.transmissionHandler.TrySubmitRequest(request))
            // {
            //     return ExportResult.Failure;
            // }
        }
        catch (Exception ex)
        {
            OpenTelemetryProtocolExporterEventSource.Log.ExportMethodException(ex);
            return ExportResult.Failure;
        }
        // finally
        // {
        //     request.Return();
        // }

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
       return otelfolder + pathSeparator;
    }

    private void SetServiceName()
    {
        var name = GetEnvironmentVariable("OTEL_SERVICE_NAME");
        this.serviceName = string.IsNullOrEmpty(name) ? defaultServiceName : name;
        Console.WriteLine("Service name : " + this.serviceName);
    }


    private static int GetIntervalToCheckConfiguration()
    {
        var value = GetEnvironmentVariable(envMotadataTraceServiceCheckTime);
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
        timer = new Timer(UpdateExportFlag, null, 0, serviceCheckInterval * 1000);
    }

    private void UpdateExportFlag(object state)
    {
        Console.WriteLine("Updating export flag..........................");
        string configPath = agentInstallDir +configDir + pathSeparator + "agent.json";
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
            Console.WriteLine(ex.Message.ToString());
        }
        return envValue;
    }
}
