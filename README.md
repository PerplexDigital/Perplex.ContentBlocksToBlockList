# Perplex.ContentBlocksToBlockList

Migrates Perplex.ContentBlocks data types and property data to Umbraco.BlockList.

## Installation

`dotnet add package Perplex.ContentBlocksToBlockList`

## Usage

This package contains a migration that will run automatically on startup. There is nothing you need to do other than run the website once, and all Perplex.ContentBlocks data types and property data will be migrated to Umbraco.BlockList format.

When the migration is completed, Perplex.ContentBlocks will no longer be used in your website.

NOTE: You do not need to have Perplex.ContentBlocks installed to run this package; it does not depend on it. In fact, it is recommended to uninstall Perplex.ContentBlocks, since there is no reason to keep it installed.

## Limitations

The package has a few known limitations, and some manual actions are required. There are no plans to resolve these issues, as the required manual actions are limited to the Umbraco.BlockList data type configuration and require minimal time to complete. This can be done locally and synced with, for example, uSync to other environments.

1. Only blocks that are used in the property data are added to the Available Blocks in the Block List data types.
    - The migration is based only on the property data that is present in the database. If a certain ContentBlock is never used in the content, we will not know about its existence, and it will not be set as an available block in the Umbraco.BlockList data types that are created.
    - **Fix**: Manually check the created Umbraco.BlockList data types and ensure all available blocks are listed.
2. Block name templates like `{{title}}` are not migrated to Umbraco.BlockList.
    - **Fix**: Manually configure the new name template in the Umbraco.BlockList configuration under `Block appearance > Label`. Make sure to use the new [Umbraco Flavored Markdown](https://docs.umbraco.com/umbraco-cms/reference/umbraco-flavored-markdown); for example, `{{title}}` would become `{umbValue: title}` or `{= headline}`.
