// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0


using System.Text.Json;

namespace OpenTelemetry.Exporter.OpenTelemetryProtocol;

public class AgentConfigurationProvider
{
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

public class AgentConfig
{
    public string AgentState { get; set; } = string.Empty;
    public string AgentServiceStatus { get; set; } = string.Empty;
    public string TraceAgentStatus { get; set; } = string.Empty;
    public string ServiceTraceState { get; set; } = string.Empty;
}
