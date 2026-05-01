# Plan: Add IPropertyValueMigrator hook

## Task

Add a hook that allows calling code to customize migration of individual property values during the ContentBlocks-to-BlockList migration. Modeled after the `IContentBlocksPropertyValueMigrator` interface in `Perplex.ContentBlocks.Core`.

---

## Context

Currently the migration in `ContentBlocksToBlockListMigrationsComponent.cs` migrates property values with no way for consumers to override or customize individual property value migrations. The only extension point is `MigratedContentBlocksPropertyValueNotification`, which is read-only and fires after the entire ContentBlocks value is migrated — not per-property.

The reference implementation in `Perplex.ContentBlocks.Core` has `IContentBlocksPropertyValueMigrator` which is injected as `IEnumerable<IContentBlocksPropertyValueMigrator>` and called per-property after the default migration runs. The last migrator that returns `true` wins.

## Plan

### 1. Create `IPropertyValueMigrator` interface

**File:** `src/IPropertyValueMigrator.cs`

```csharp
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
    /// Return <c>true</c> and set <paramref name="migratedValue"/> to override the default migration.
    /// Return <c>false</c> and set <paramref name="migratedValue"/> to <c>null</c> if you did not handle this property.
    /// Do not modify <paramref name="originalValue"/>.
    /// </summary>
    bool MigratePropertyValue(
        JsonNode? originalValue,
        IPropertyType propertyType,
        IContentType contentType,
        out JsonNode? migratedValue);
}
```

### 2. Inject migrators into `MigrateFromContentBlocksToBlockList`

The inner `MigrateFromContentBlocksToBlockList` class needs `IEnumerable<IPropertyValueMigrator>` added to its constructor.

This requires the composer to register the migrators. Since the migration class is resolved by Umbraco's migration infrastructure via DI, the `IEnumerable<IPropertyValueMigrator>` will be automatically injected if any implementations are registered.

### 3. Call migrators in `MigrateValues` after the default `MigrateValue`

In the `MigrateValues` local function (line ~309), after `MigrateValue` produces `newValue` (line 330), iterate over the injected migrators:

```csharp
var newValue = MigrateValue(oldValue, property.PropertyEditorAlias);

// Allow custom migrators to override the default migration
foreach (var migrator in propertyValueMigrators)
{
    if (migrator.MigratePropertyValue(oldValue, property, contentType, out var migratedValue))
    {
        newValue = migratedValue;
    }
}
```

This matches the reference implementation's behavior: the last migrator returning `true` wins.

### 4. Also call migrators for v4 format property values

The v4 path (`MigrateBlockData_v4`, line ~238) copies content data directly without per-property migration. The migrator hook should also apply there. However, v4 data has already been migrated to BlockList format by the previous ContentBlocks internal migration, so property values are already in their final shape.

**Decision needed:** Should the hook apply to v4 format values as well? The v4 path doesn't call `MigrateValues` at all — it just deep-clones the content object. Adding per-property migration there would require iterating over the content type's properties and resolving them from the v4 content JSON structure, which is more involved.

**Recommendation:** Skip v4 for now. The v4 format means the site already ran the ContentBlocks v4 migration which had its own `IContentBlocksPropertyValueMigrator` hook. Document this limitation.

### 5. Register in the composer

No explicit registration of the interface collection is needed beyond what DI provides — but to make it discoverable, add a collection builder or use `builder.Services` in the composer:

```csharp
// In ContentBlocksToBlockListMigrationsComponentComposer.Compose:
builder.Services.AddSingleton<IEnumerable<IPropertyValueMigrator>>(
    sp => sp.GetServices<IPropertyValueMigrator>());
```

Actually, `IEnumerable<T>` is resolved automatically by the Microsoft DI container. Consumers just need to register their implementations:

```csharp
builder.Services.AddSingleton<IPropertyValueMigrator, MyCustomMigrator>();
```

So no extra registration is needed in the composer. Just ensure the constructor injection works.

---

## Summary of changes

| File | Change |
|------|--------|
| `src/IPropertyValueMigrator.cs` | **New** — Interface definition |
| `src/ContentBlocksToBlockListMigrationsComponent.cs` | Add `IEnumerable<IPropertyValueMigrator>` to `MigrateFromContentBlocksToBlockList` constructor; call migrators after default `MigrateValue` in `MigrateValues` |

## Files NOT changed

- Composer — no changes needed, MS DI resolves `IEnumerable<T>` automatically
- `MigratedContentBlocksPropertyValueNotification` — unchanged, still fires as before

## Open question

- Should the hook also apply to v4-format data? (Recommendation: no, see step 4)
