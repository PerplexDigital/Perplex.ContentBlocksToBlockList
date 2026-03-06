using System.Text.Json.Nodes;
using Umbraco.Cms.Core.Notifications;

namespace Perplex.ContentBlocksToBlockList;

/// <summary>
/// Notification sent after a ContentBlocks property value has been migrated to BlockList format.
/// </summary>
public class MigratedContentBlocksPropertyValueNotification
(
    JsonNode contentBlocksValue,
    JsonNode blockListValue
) : INotification
{

    /// <summary>
    /// The original ContentBlocks JSON value before migration.
    /// </summary>
    public JsonNode ContentBlocksValue { get; } = contentBlocksValue;

    /// <summary>
    /// The migrated BlockList JSON value.
    /// </summary>
    public JsonNode BlockListValue { get; } = blockListValue;
}
