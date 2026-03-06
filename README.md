# Perplex.ContentBlocksToBlockList

Migrates Perplex.ContentBlocks data types and property data to Umbraco.BlockList.

## Installation

`dotnet add package Perplex.ContentBlocksToBlockList`

## Usage

This package contains a migration that will run automatically on startup. There is nothing you need to do other than run the website once, and all Perplex.ContentBlocks data types and property data will be migrated to Umbraco.BlockList format.

When the migration is completed, Perplex.ContentBlocks will no longer be used in your website.

NOTE: You do not need to have Perplex.ContentBlocks installed to run this package; it does not depend on it. In fact, it is recommended to uninstall Perplex.ContentBlocks, since there is no reason to keep it installed.

### NotificationHandler

If you want to perform some custom migration actions during the migration you can handle the `MigratedContentBlocksPropertyValueNotification` that is sent after we migrate a Perplex.ContentBlocks property value to Umbraco.BlockList. The notification contains the raw `JsonNode` values of the ContentBlocks property value and the migrated BlockList value. This notification is sent before the BlockList property value is persisted to the database so you can update its value directly in your notification handler to make changes if desired.

Note this is quite a low-level notification giving you the raw `JsonNode` instances so you will need to perform some parsing yourself. See the example below:

```csharp
public class MigratedContentBlocksPropertyValueNotificationHandlerComposer : IComposer
{
    public void Compose(IUmbracoBuilder builder)
    {
        builder.AddNotificationHandler<MigratedContentBlocksPropertyValueNotification, MigratedContentBlocksPropertyValueNotificationHandler>();
    }
}

public class MigratedContentBlocksPropertyValueNotificationHandler : INotificationHandler<MigratedContentBlocksPropertyValueNotification>
{
    public class MigratedContentBlocksPropertyValueNotificationHandler : INotificationHandler<MigratedContentBlocksPropertyValueNotification>
{
    public void Handle(MigratedContentBlocksPropertyValueNotification notification)
    {
        // Add your custom migration logic here
        // Updates to notification.BlockListValue will be persisted to the migrated BlockList property value.

        // Basic sample:

        var contentData = notification.BlockListValue["contentData"]?.AsArray();
        var settingsData = notification.BlockListValue["settingsData"]?.AsArray();

        if (contentData is null || contentData.Count == 0)
        {
            return;
        }

        JsonNode?[] contentBlocks =
        [
            notification.ContentBlocksValue["header"]?.AsObject(),
            .. notification.ContentBlocksValue["blocks"]?.AsArray() ?? []
        ];

        if (contentBlocks is null || contentBlocks.Length == 0)
        {
            return;
        }

        foreach (var block in contentBlocks)
        {
            if (block is null) continue;

            var definitionId = block["definitionId"]?.GetValue<Guid>();
            var layoutId = block["layoutId"]?.GetValue<Guid>();
            var isDisabled = block["isDisabled"]?.GetValue<bool>();

            // The original NestedContent key
            var key = block["content"]?.AsArray()?.FirstOrDefault()?["key"]?.GetValue<Guid>();
            if (key is null) continue;

            var blockListBlock = contentData.FirstOrDefault(x => x?["key"]?.GetValue<Guid>() == key);
            if (blockListBlock is null)
            {
                continue;
            }

            // Update blockListBlock as needed or update settingsData for this block
        }
    }
}

```

## Limitations

The package has a few known limitations, and some manual actions are required. There are no plans to resolve these issues, as the required manual actions are limited to the Umbraco.BlockList data type configuration and require minimal time to complete. This can be done locally and synced with, for example, uSync to other environments.

1. Only blocks that are used in the property data are added to the Available Blocks in the Block List data types.
    - The migration is based only on the property data that is present in the database. If a certain ContentBlock is never used in the content, we will not know about its existence, and it will not be set as an available block in the Umbraco.BlockList data types that are created.
    - **Fix**: Manually check the created Umbraco.BlockList data types and ensure all available blocks are listed.
2. Block name templates like `{{title}}` are not migrated to Umbraco.BlockList.
    - **Fix**: Manually configure the new name template in the Umbraco.BlockList configuration under `Block appearance > Label`. Make sure to use the new [Umbraco Flavored Markdown](https://docs.umbraco.com/umbraco-cms/reference/umbraco-flavored-markdown); for example, `{{title}}` would become `{umbValue: title}` or `{= title}`.
3. Features unique to Perplex.ContentBlocks that are not present in Umbraco.BlockList like layouts or the ability to hide blocks are not migrated. You can implement a notification handler for `MigratedContentBlocksPropertyValueNotification` to migrate these in a way that suits your use case.
