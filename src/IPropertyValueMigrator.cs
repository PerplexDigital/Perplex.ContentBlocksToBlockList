using System.Text.Json.Nodes;
using Umbraco.Cms.Core.Models;

namespace Perplex.ContentBlocksToBlockList;

/// <summary>
/// Migrates an individual property value during the ContentBlocks-to-BlockList migration.
/// Implement this interface to override the default migration for specific property values,
/// for example when using a custom property editor whose values need special handling.
/// </summary>
public interface IPropertyValueMigrator
{
    /// <summary>
    /// Migrates a property value from the old format to BlockList format.
    /// Return <c>true</c> and set <paramref name="migratedValue"/> to the new value to override the default migration.
    /// Return <c>false</c> and set <paramref name="migratedValue"/> to <c>null</c> if you did not handle this property.
    /// Do not modify <paramref name="originalValue"/>.
    /// </summary>
    bool MigratePropertyValue(JsonNode? originalValue, IPropertyType propertyType, IContentType contentType, out JsonNode? migratedValue);
}
