// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0


using System.Text.Json;

namespace OpenTelemetry.Exporter.OpenTelemetryProtocol;

/// <summary>
/// Loads and returns the agent’s configuration from json file.
/// </summary>
public class AgentConfigurationProvider
{
    /// <summary>
    /// Reads the configuration file at <paramref name="configPath"/> for the service
    /// identified by <paramref name="serviceName"/>.
    /// </summary>
    public AgentConfig GetConfiguration(string configPath, string serviceName)
    {
        if (!File.Exists(configPath))
        {
            throw new FileNotFoundException($"Configuration file not found: {configPath}");
        }

        string jsonContent = File.ReadAllText(configPath);
        using JsonDocument doc = JsonDocument.Parse(jsonContent);
        JsonElement root = doc.RootElement;

        if (!root.TryGetProperty("agent", out JsonElement agentNode))
        {
            throw new InvalidOperationException("'agent' node not found in configuration");
        }

        var config = new AgentConfig
        {
            AgentState = agentNode.GetProperty("agent.state").GetString() ?? string.Empty,
            AgentServiceStatus = agentNode.GetProperty("agent.service.status").GetString() ?? string.Empty,
            TraceAgentStatus = agentNode.GetProperty("trace.agent.status").GetString() ?? string.Empty,
            ServiceTraceState = "no" // Default value
        };

        if (!string.IsNullOrEmpty(serviceName) &&
            root.TryGetProperty("trace.agent", out JsonElement traceAgentNode) &&
            traceAgentNode.TryGetProperty(serviceName, out JsonElement serviceNode))
        {
            config.ServiceTraceState = serviceNode.GetProperty("service.trace.state").GetString() ?? "no";
        }

        return config;
    }
}

/// <summary>
/// Represents the configuration settings for the trace agent.
/// </summary>
public class AgentConfig
{
    /// <summary>
    /// The current state of the agent process.
    /// </summary>
    public string AgentState { get; set; } = string.Empty;

    /// <summary>
    /// The desired state for the agent’s service (e.g. Running, Stopped).
    /// </summary>
    public string AgentServiceStatus { get; set; } = string.Empty;

    /// <summary>
    /// The status of the trace‐agent connection.
    /// </summary>
    public string TraceAgentStatus { get; set; } = string.Empty;

    /// <summary>
    /// The service’s trace‐collection state (e.g. Enabled, Disabled).
    /// </summary>
    public string ServiceTraceState { get; set; } = string.Empty;
}
