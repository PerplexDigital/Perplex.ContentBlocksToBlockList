using Microsoft.Extensions.Logging;
using NPoco;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Nodes;
using Umbraco.Cms.Core;
using Umbraco.Cms.Core.Composing;
using Umbraco.Cms.Core.DependencyInjection;
using Umbraco.Cms.Core.Events;
using Umbraco.Cms.Core.Migrations;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Core.Notifications;
using Umbraco.Cms.Core.Scoping;
using Umbraco.Cms.Core.Services;
using Umbraco.Cms.Infrastructure.Migrations;
using Umbraco.Cms.Infrastructure.Migrations.Upgrade;
using static Umbraco.Cms.Core.Constants.PropertyEditors;

namespace Perplex.ContentBlocksToBlockList;

public class ContentBlocksToBlockListMigrationsComponent
(
    IMigrationPlanExecutor migrationPlanExecutor,
    ICoreScopeProvider coreScopeProvider,
    IKeyValueService keyValueService,
    IRuntimeState runtimeState,
    ILogger<ContentBlocksToBlockListMigrationsComponent> logger
) : INotificationAsyncHandler<RuntimePremigrationsUpgradeNotification>
{
    public async Task HandleAsync(RuntimePremigrationsUpgradeNotification notification, CancellationToken cancellationToken)
    {
        if (runtimeState.Level < RuntimeLevel.Upgrade)
        {
            logger.LogInformation("Skipping Perplex.ContentBlocksToBlockList migrations in runtime level {RuntimeLevel}", runtimeState.Level);
            return;
        }

        if (notification.UpgradeResult == RuntimePremigrationsUpgradeNotification.PremigrationUpgradeResult.HasErrors)
        {
            logger.LogWarning("Skipping Perplex.ContentBlocksToBlockList migrations due errors in the Umbraco premigration step.");
            return;
        }

        var plan = new MigrationPlan("ContentBlocksToBlockList");

        plan.From(string.Empty)
            .To<MigrateFromContentBlocksToBlockList>("MigrateFromContentBlocksToBlockList");

        var upgrader = new Upgrader(plan);
        await upgrader.ExecuteAsync(migrationPlanExecutor, coreScopeProvider, keyValueService);
    }

    private class MigrateFromContentBlocksToBlockList
    (
        IMigrationContext context,
        IContentTypeService contentTypeService,
        ILogger<MigrateFromContentBlocksToBlockList> logger
    ) : AsyncMigrationBase(context)
    {
        private const string _blockListUiEditorAlias = "Umb.PropertyEditorUi.BlockList";
        private const int _numberOfContentVersionsToMigrate = 8;

        private static readonly JsonSerializerOptions _jsonOptions = new() { WriteIndented = false };
        private readonly Dictionary<string, IContentType> _cachedContentTypeByAlias = new(StringComparer.OrdinalIgnoreCase);

        protected override Task MigrateAsync()
        {
            MigrateContentBlocksPropertyData();

            return Task.CompletedTask;
        }

        /// <summary>
        /// Migrates the property data of Perplex.ContentBlocks from Nested Content to Block List format.
        /// This also migrates the Nested Content data types used within Perplex.ContentBlocks to use the Block List property editor.
        /// </summary>
        private void MigrateContentBlocksPropertyData()
        {
            var sw = Stopwatch.StartNew();

            logger.LogInformation("[Perplex.ContentBlocksToBlockList] Reading property data from database ...");
            var propertyData = GetPropertyData();
            logger.LogInformation("[Perplex.ContentBlocksToBlockList] Property data rows found: {count}", propertyData.Length);

            var nestedContentDataTypes = GetDataTypes("Umbraco.NestedContent").ToDictionary(dt => dt.NodeId);
            var contentBlockElementTypeKeys = new HashSet<Guid>();

            var migratedDataTypes = new Dictionary<int, UmbracoDataType>();

            logger.LogInformation("[Perplex.ContentBlocksToBlockList] Migrating property data ...");
            var migratedPropertyData = MigratePropertyData(propertyData);

            MigrateContentBlocksDataTypes();

            logger.LogInformation("[Perplex.ContentBlocksToBlockList] Property data rows migrated: {count}", migratedPropertyData.Length);
            logger.LogInformation("[Perplex.ContentBlocksToBlockList] Data types migrated: {count}", migratedDataTypes.Values.Count);

            if (migratedPropertyData.Length > 0)
            {
                logger.LogInformation("[Perplex.ContentBlocksToBlockList] Writing migrated property data to database ...");
                PersistPropertyData(migratedPropertyData);
            }

            if (migratedDataTypes.Values.Count > 0)
            {
                logger.LogInformation("[Perplex.ContentBlocksToBlockList] Writing migrated data types to database ...");
                PersistDataTypes(migratedDataTypes.Values);
            }

            sw.Stop();

            var elapsed = GetHumanReadableElapsed(sw.Elapsed);
            logger.LogInformation("[Perplex.ContentBlocksToBlockList] Migration completed in {elapsed}", elapsed);

            UmbracoPropertyData[] GetPropertyData()
            {
                // Note: due to an NPoco bug the query MUST START with ";" as the first character.
                // Do not add whitespace before it then it will break again because NPoco will then
                // insert a SELECT statement before the CTE.
                var query = @";WITH RankedVersions AS (
                    SELECT pd.*, ROW_NUMBER() OVER (
                        PARTITION BY v.nodeId, pd.propertyTypeId, pd.languageId, pd.segment
                        ORDER BY pd.versionId DESC
                    ) versionRank
                    FROM umbracoPropertyData pd
                    JOIN umbracoContentVersion v
                    ON pd.versionId = v.id
                    JOIN cmsPropertyType pt
                    ON pd.propertyTypeId = pt.id
                    JOIN umbracoDataType dt
                    ON pt.dataTypeId = dt.nodeId
                    WHERE dt.propertyEditorAlias = @propertyEditorAlias
                )

                SELECT id, versionId, propertyTypeId, languageId, segment, intValue, decimalValue, dateValue, varcharValue, textValue
                FROM RankedVersions
                WHERE VersionRank <= @versions";

                var args = new
                {
                    propertyEditorAlias = "Perplex.ContentBlocks",
                    versions = _numberOfContentVersionsToMigrate,
                };

                return [.. Database.Fetch<UmbracoPropertyData>(query, args)];
            }

            UmbracoPropertyData[] MigratePropertyData(UmbracoPropertyData[] propertyDatas)
            {
                if (propertyDatas.Length == 0)
                {
                    return [];
                }

                var migrated = new List<UmbracoPropertyData>();

                foreach (var propertyData in propertyDatas)
                {
                    if (!TryParseJson(propertyData.TextValue, out var node))
                    {
                        continue;
                    }

                    if (!MigrateContentBlocks(node, out JsonNode? value))
                    {
                        continue;
                    }

                    propertyData.TextValue = value.ToJsonString(_jsonOptions);

                    migrated.Add(propertyData);
                }

                return [.. migrated];
            }

            bool MigrateContentBlocks(JsonNode node, [NotNullWhen(true)] out JsonNode? value)
            {
                const int MaxVersionInNestedContentFormat = 3;

                value = null;

                var version = node["version"]?.GetValue<int>();
                if (version is null)
                {
                    return false;
                }

                var isNestedContentFormat = version <= MaxVersionInNestedContentFormat;

                if (!isNestedContentFormat)
                {
                    // The values are already in BlockList format ("contentData")
                    // We just move them into the correct JSON structure and record
                    // the used element types.

                    var layouts = new JsonArray();
                    var contentDatas = new JsonArray();
                    var exposes = new JsonArray();

                    if (node["header"]?["content"] is JsonObject headerContent)
                    {
                        MigrateBlockData_v4(headerContent);
                    }

                    if (node["blocks"] is JsonArray blocks)
                    {
                        foreach (var block in blocks.OfType<JsonObject>())
                        {
                            if (block["content"] is JsonObject blockContent)
                            {
                                MigrateBlockData_v4(blockContent);
                            }
                        }
                    }

                    // BlockList format
                    value = new JsonObject
                    {
                        ["contentData"] = contentDatas,

                        ["settingsData"] = new JsonArray(),

                        ["expose"] = exposes,

                        ["Layout"] = new JsonObject
                        {
                            ["Umbraco.BlockList"] = layouts,
                        },
                    };

                    return true;

                    void MigrateBlockData_v4(JsonObject content)
                    {
                        if (content["contentTypeKey"]?.GetValue<Guid>() is Guid contentTypeKey &&
                            contentTypeKey != default)
                        {
                            RegisterContentBlockElementTypeKey(contentTypeKey);
                        }

                        var key = content["key"]?.ToString();

                        layouts.Add(new JsonObject
                        {
                            ["contentKey"] = key,
                        });

                        // The values are already migrated by the v4 migration, we just copy them here.
                        contentDatas.Add(content.DeepClone());

                        exposes.Add(new JsonObject
                        {
                            ["contentKey"] = key,
                            ["culture"] = null,
                            ["segment"] = null
                        });
                    }
                }

                // Nested Content format; re-use existing code to migrate to BlockList structure.
                var ncArray = new JsonArray();

                if (node["header"] is JsonObject header &&
                    GetNcContent(header) is JsonObject headerNcContent)
                {
                    ncArray.Add(headerNcContent.DeepClone());
                }

                if (node["blocks"] is JsonArray ncBlocks)
                {
                    foreach (var block in ncBlocks.OfType<JsonObject>())
                    {
                        if (GetNcContent(block) is JsonObject blockNcContent)
                        {
                            ncArray.Add(blockNcContent.DeepClone());
                        }
                    }
                }

                value = MigrateNestedContentToBlockList(ncArray, RegisterContentBlockElementTypeKey);
                return true;

                void RegisterContentBlockElementTypeKey(Guid contentTypeKey)
                {
                    contentBlockElementTypeKeys.Add(contentTypeKey);
                }
            }

            static JsonObject? GetNcContent(JsonObject block)
            {
                if (block["content"] is not JsonArray contentArray ||
                    contentArray.Count <= 0 ||
                    contentArray[0] is not JsonObject content)
                {
                    return null;
                }

                return content;
            }

            JsonArray MigrateValues(JsonObject content, IContentType contentType)
            {
                var values = new JsonArray();

                foreach (var property in contentType.CompositionPropertyTypes)
                {
                    if (!content.ContainsKey(property.Alias))
                    {
                        continue;
                    }

                    if (property.PropertyEditorAlias == "Umbraco.NestedContent" &&
                        !migratedDataTypes.ContainsKey(property.DataTypeId) &&
                        nestedContentDataTypes.TryGetValue(property.DataTypeId, out var ncDataType) &&
                        MigrateNestedContentDataType(ncDataType) is UmbracoDataType migratedDataType)
                    {
                        // Migrate the Nested Content data type to Block List format
                        migratedDataTypes[property.DataTypeId] = migratedDataType;
                    }

                    var oldValue = content[property.Alias];
                    var newValue = MigrateValue(oldValue, property.PropertyEditorAlias);

                    var value = new JsonObject
                    {
                        ["editorAlias"] = property.PropertyEditorAlias,
                        ["culture"] = null,
                        ["segment"] = null,
                        ["alias"] = property.Alias,
                        ["value"] = newValue,
                    };

                    values.Add(value);
                }

                return values;
            }

            JsonNode? MigrateValue(JsonNode? node, string propertyEditorAlias)
            {
                if (propertyEditorAlias != "Umbraco.NestedContent")
                {
                    // Return the original value
                    return node?.DeepClone();
                }

                if (!TryParseJson(node?.ToString(), out JsonNode? value))
                {
                    // In NestedContent everything is stored as a string even if it's actually an Array or Object
                    // so we always first try to parse it as a complex object and use that if possible.
                    // BlockList stores the value as a proper complex object so this is necessary to make
                    // many values that we do not migrate work correctly.
                    // For example "[]" should be a real JsonArray and not a string.
                    value = node;
                }

                return MigrateNestedContentPropertyValue(value);
            }

            JsonNode? MigrateNestedContentPropertyValue(JsonNode? node)
            {
                if (node is not JsonArray ncArray ||
                    ncArray.Count == 0)
                {
                    // Cannot determine content type used by this block, ignore.
                    return node?.DeepClone();
                }

                return MigrateNestedContentToBlockList(ncArray);
            }

            JsonObject MigrateNestedContentToBlockList(JsonArray ncArray, Action<Guid>? contentTypeKeyCallback = null)
            {
                var layouts = new JsonArray();
                var contentDatas = new JsonArray();
                var exposes = new JsonArray();

                foreach (var ncContent in ncArray.OfType<JsonObject>())
                {
                    if (ncContent["ncContentTypeAlias"]?.ToString() is not string contentTypeAlias ||
                        GetContentType(contentTypeAlias) is not IContentType contentType)
                    {
                        // Cannot determine content type used by this block, ignore.
                        continue;
                    }

                    contentTypeKeyCallback?.Invoke(contentType.Key);

                    var values = MigrateValues(ncContent, contentType);

                    var key = ncContent["key"]?.ToString();

                    layouts.Add(new JsonObject
                    {
                        ["contentKey"] = key,
                    });

                    contentDatas.Add(new JsonObject
                    {
                        ["key"] = key,
                        ["contentTypeKey"] = contentType.Key,
                        ["values"] = values
                    });

                    exposes.Add(new JsonObject
                    {
                        ["contentKey"] = key,
                        ["culture"] = null,
                        ["segment"] = null
                    });
                }

                // BlockList format
                return new JsonObject
                {
                    ["contentData"] = contentDatas,

                    ["settingsData"] = new JsonArray(),

                    ["expose"] = exposes,

                    ["Layout"] = new JsonObject
                    {
                        ["Umbraco.BlockList"] = layouts,
                    },
                };
            }

            void PersistPropertyData(IEnumerable<UmbracoPropertyData> propertyDatas)
            {
                if (DatabaseType == DatabaseType.SQLite)
                {
                    // SQLite does not support true bulk insert and the UPDATE .. JOIN syntax so
                    // we will use the simple but slow method there.
                    ForEachUpdate();
                    return;
                }

                // Non SQLite database types should be SQL Server which can do bulk insert + update
                BulkUpdate();

                void ForEachUpdate()
                {
                    foreach (var propertyData in propertyDatas)
                    {
                        var query = @"
                        UPDATE umbracoPropertyData
                        SET textValue = @textValue
                        WHERE id = @id";

                        var args = new
                        {
                            textValue = propertyData.TextValue,
                            id = propertyData.Id
                        };

                        Database.Execute(query, args);
                    }
                }

                void BulkUpdate()
                {
                    Database.BeginTransaction();

                    Database.Execute(@"
                    CREATE TABLE #ContentBlocksToBlockListMigration (
                        id INT NOT NULL,
                        textValue NVARCHAR(MAX) NULL)");

                    var migratedValues = propertyDatas.Select(pd => new MigratedPropertyValue
                    {
                        Id = pd.Id,
                        TextValue = pd.TextValue,
                    }).ToArray();

                    Database.InsertBulk(migratedValues);

                    Database.OneTimeCommandTimeout = 600; // 10 minutes

                    Database.Execute(@"
                    UPDATE target
                    SET target.textValue = src.textValue
                    FROM umbracoPropertyData target
                    INNER JOIN #ContentBlocksToBlockListMigration src ON target.id = src.id");

                    Database.Execute("DROP TABLE #ContentBlocksToBlockListMigration");

                    Database.CompleteTransaction();
                }
            }

            UmbracoDataType? MigrateNestedContentDataType(UmbracoDataType dataType)
            {
                var config = MapNestedContentConfig(dataType.Config);

                return new UmbracoDataType
                {
                    NodeId = dataType.NodeId,
                    PropertyEditorAlias = Aliases.BlockList,
                    PropertyEditorUiAlias = _blockListUiEditorAlias,
                    DbType = dataType.DbType,
                    Config = config,
                };
            }

            void MigrateContentBlocksDataTypes()
            {
                var contentBlockDataTypeIds = GetDataTypes("Perplex.ContentBlocks").Select(dt => dt.NodeId).ToArray();

                foreach (var dataTypeId in contentBlockDataTypeIds)
                {
                    var blockListConfig = new JsonObject
                    {
                        // ContentBlocks only has inline editing so let's enable that for the Block List version as well.
                        ["useInlineEditingAsDefault"] = true
                    };

                    var blocks = new JsonArray();

                    foreach (var elementTypeKey in contentBlockElementTypeKeys)
                    {
                        blocks.Add(new JsonObject
                        {
                            ["contentElementTypeKey"] = elementTypeKey
                        });
                    }

                    if (blocks.Count > 0)
                    {
                        blockListConfig["blocks"] = blocks;
                    }

                    var config = blockListConfig.ToJsonString(_jsonOptions);

                    migratedDataTypes[dataTypeId] = new UmbracoDataType
                    {
                        NodeId = dataTypeId,
                        PropertyEditorAlias = Aliases.BlockList,
                        PropertyEditorUiAlias = _blockListUiEditorAlias,
                        DbType = "Ntext",
                        Config = config,
                    };
                }
            }

            string? MapNestedContentConfig(string? configStr)
            {
                if (!TryParseJson(configStr, out var config))
                {
                    // Cannot parse, just return the original config
                    return configStr;
                }

                var blockListConfig = new JsonObject
                {
                    // NestedContent only has inline editing so let's enable that for the Block List version as well.
                    ["useInlineEditingAsDefault"] = true
                };

                var min = config["minItems"]?.GetValue<int?>();
                var max = config["maxItems"]?.GetValue<int?>();

                if (min > 0 || max > 0)
                {
                    var validationLimit = blockListConfig["validationLimit"] = new JsonObject();

                    if (min > 0)
                    {
                        validationLimit["min"] = min.Value;
                    }

                    if (max > 0)
                    {
                        validationLimit["max"] = max.Value;
                    }
                }

                var blocks = new JsonArray();

                if (config["contentTypes"] is JsonArray ncContentTypes &&
                    ncContentTypes.Count > 0)
                {
                    foreach (var ncContentType in ncContentTypes.OfType<JsonObject>())
                    {
                        if (ncContentType["ncAlias"]?.ToString() is not string contentTypeAlias ||
                            GetContentType(contentTypeAlias) is not IContentType contentType)
                        {
                            continue;
                        }

                        blocks.Add(new JsonObject
                        {
                            ["contentElementTypeKey"] = contentType.Key
                        });
                    }

                    if (blocks.Count > 0)
                    {
                        blockListConfig["blocks"] = blocks;
                    }
                }

                if (min == 1 && max == 1 && blocks.Count == 1)
                {
                    blockListConfig["useSingleBlockMode"] = true;
                }

                return blockListConfig.ToJsonString(_jsonOptions);
            }

            void PersistDataTypes(IEnumerable<UmbracoDataType> dataTypes)
            {
                foreach (var dataType in dataTypes)
                {
                    var query = @"
                    UPDATE umbracoDataType
                    SET propertyEditorAlias = @propertyEditorAlias,
                        config = @config,
                        dbType = @dbType,
                        propertyEditorUiAlias = @propertyEditorUiAlias
                    WHERE nodeId = @nodeId";

                    var args = new
                    {
                        nodeId = dataType.NodeId,
                        dbType = dataType.DbType,
                        propertyEditorAlias = dataType.PropertyEditorAlias,
                        propertyEditorUiAlias = dataType.PropertyEditorUiAlias,
                        config = dataType.Config,
                    };

                    Database.Execute(query, args);
                }
            }
        }

        private IContentType? GetContentType(string contentTypeAlias)
        {
            if (string.IsNullOrWhiteSpace(contentTypeAlias))
            {
                return null;
            }

            if (!_cachedContentTypeByAlias.TryGetValue(contentTypeAlias, out var cachedContentType))
            {
                var contentType = contentTypeService.Get(contentTypeAlias);
                if (contentType is null)
                {
                    return null;
                }

                _cachedContentTypeByAlias[contentTypeAlias] = cachedContentType = contentType;
            }

            return cachedContentType;
        }

        private UmbracoDataType[] GetDataTypes(string propertyEditorAlias)
        {
            var query = @"
            SELECT *
            FROM umbracoDataType
            WHERE propertyEditorAlias = @propertyEditorAlias";

            return [.. Database.Fetch<UmbracoDataType>(query, new { propertyEditorAlias })];
        }

        private static string GetHumanReadableElapsed(TimeSpan ts)
        {
            int amount;
            string unit;

            if (ts.TotalSeconds < 10)
            {
                amount = (int)Math.Round(ts.TotalMilliseconds);
                if (amount == 0) amount = 1; // handle very small times
                unit = amount == 1 ? "millisecond" : "milliseconds";
            }

            else if (ts.TotalSeconds < 600)
            {
                amount = (int)Math.Round(ts.TotalSeconds);
                unit = amount == 1 ? "second" : "seconds";
            }

            else if (ts.TotalMinutes < 600)
            {
                amount = (int)Math.Round(ts.TotalMinutes);
                unit = amount == 1 ? "minute" : "minutes";
            }

            else
            {
                amount = (int)Math.Round(ts.TotalHours);
                unit = amount == 1 ? "hour" : "hours";
            }

            return $"{amount} {unit}";
        }

        /// <summary>
        /// Attempts to parse a JSON string into a <see cref="JsonNode"/>.
        /// </summary>
        /// <param name="json">The JSON string to parse. Can be null or whitespace.</param>
        /// <param name="node">When this method returns, contains the parsed <see cref="JsonNode"/> if parsing succeeded; otherwise, null.</param>
        /// <returns>True if the JSON string was successfully parsed into a <see cref="JsonNode"/>; otherwise, false.</returns>
        private static bool TryParseJson(string? json, [NotNullWhen(true)] out JsonNode? node)
        {
            node = null;

            if (string.IsNullOrWhiteSpace(json))
            {
                return false;
            }

            try
            {
                node = JsonNode.Parse(json);
                return node is not null;
            }
            catch
            {
                return false;
            }
        }

        // Copied from Umbraco.Cms.Infrastructure.Persistence.Dtos.PropertyDataDto and dropped the PropertyTypeDto + Value properties
        internal class UmbracoPropertyData
        {
            public int Id { get; set; }
            public int VersionId { get; set; }
            public int PropertyTypeId { get; set; }
            public int? LanguageId { get; set; }
            public string? Segment { get; set; }
            public int? IntValue { get; set; }
            public decimal? DecimalValue { get; set; }
            public DateTime? DateValue { get; set; }
            public string? VarcharValue { get; set; }
            public string? TextValue { get; set; }
        }

        // Copied from Umbraco.Cms.Infrastructure.Persistence.Dtos and dropped the NodeDto property
        internal class UmbracoDataType
        {
            public int NodeId { get; set; }
            public string PropertyEditorAlias { get; set; } = null!;
            public string? PropertyEditorUiAlias { get; set; }
            public string DbType { get; set; } = null!;
            public string? Config { get; set; }
        }

        [TableName("#ContentBlocksToBlockListMigration")]
        [PrimaryKey(nameof(Id), AutoIncrement = false)]
        internal class MigratedPropertyValue
        {
            [Column("id")]
            public int Id { get; set; }

            [Column("textValue")]
            public string? TextValue { get; set; }
        }
    }
}

public class ContentBlocksToBlockListMigrationsComponentComposer : IComposer
{
    public void Compose(IUmbracoBuilder builder)
    {
        builder.AddNotificationAsyncHandler<RuntimePremigrationsUpgradeNotification, ContentBlocksToBlockListMigrationsComponent>();
    }
}
