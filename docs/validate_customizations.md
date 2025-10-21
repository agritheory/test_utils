# Frappe Customization Validation Documentation

The `validate_customizations` hook is designed to keep Custom Fields and Property Setters with their respective apps. This has become increasingly necessary as the number of applications used in a typical ERPNext deployment increases.

## Validation Checks

The validation system performs several checks to ensure customizations are properly organized and maintained:

1. **Custom Permissions Check** (`validate_no_custom_perms`)
	- Ensures no custom permissions are defined in customization files
	- Custom permissions should not be included in customizations
	- Changes to permissions through customizations create significant operational friction:
		* Changes must go through version control and CI pipelines, causing unnecessary delays
		* Custom permissions are reset during `bench migrate`, nullifying any manual changes made by the customer or system administrator
		* Permission changes often need to be immediate for business operations
	- Instead, manage permissions directly through the UI's Role Permission Manager

2. **Module Attribution** (`validate_module`)
	- Verifies that all Custom Fields and Property Setters have a valid module key
	- Ensures Property Setters are associated with the correct module and application

3. **System Generation Status** (`validate_system_generated`)
	- Identifies and flags system-generated Custom Fields that may be managed by internal Frappe/ERPNext APIs
		- Examples include barcode visibility in Stock Settings
		- The management of Accounting Dimension or Inventory Dimensions fields can be overridden if subsequent customization is required 

4. **Own DocType Customization** (`validate_customizations_on_own_doctypes`)
	- Prevents customizing DocTypes that are already defined within the same app, ensuring modifications to an app's DocTypes are made in the source rather than through customizations

5. **Duplicate Customizations** (`validate_duplicate_customizations`)
	- Detects duplicate Custom Fields  and Property Setters across different modules
	- Prevents conflicting customizations between apps

5. **Replace email literals** (`validate_email_literals`)
	- Checks @ email literals in Custom Fields and Property Setters JSON file in the "modified" or "owner" key
	- Replace it with "Administrator"


## Adding New Customizations

Customizations can be added to the system through several methods:

1. **Via Customize Form**
	- Navigate to the "Customize Form" page
	- Make desired modifications
	- Use the "Export Customizations" button to save all changes
		- This may include customizations made by ERPNext, HRMS or other apps, so editing the resulting JSON file may be appropriate
		- This is not usually the best choice for adding a new Custom Field or Property Setter to an existing customization file

2. **Individual Field/Property Export**
	- Access the Custom Field or Property Setter list
	- Use the 'Copy to Clipboard' option to collect a JSON serialization of the record
	- Paste into your app's customization JSON file in the appropriate section

3. **Programmatic Export**
	- Write scripts to export customizations
	- Ensure proper module attribution
	- Place files in the correct app directory structure

## File Structure

Customizations should be organized in the following directory structure so that they are loaded automatically by Frappe.
```
your_app/
├── your_app/
│   ├── modules.txt
│   └── module_name/
│       └── custom/
│           └── name_of_customized_doctype.json
```

## Why To Not Use Fixtures

While Frappe provides a fixtures feature that can export and import DocTypes including Custom Fields and Property Setters, it has several drawbacks that make it unsuitable for managing customizations:

1. **No Separation of Concerns**
  - Fixtures don't maintain any reference to their source app; all Custom Fields and Property Setters are included
  - Fixtures are organized into a single file for Custom Fields and separate, single for Property Setters, without any organization by doctype or module

2. **All-or-Nothing Import**
  - Fixtures must be imported in their entirety
  - Makes it difficult to manage incremental changes

3. **Version Control Challenges**
  - Changes to fixtures create large diffs in version control, even for trivial changes because the default export is ordered by the modified datetime field
  - Hard to review and understand what actually changed
  - Can obscure important changes among routine updates

4. **Migration Reliability**
  - No built-in validation for duplicate or conflicting customizations; the last application in the hooks resolution order will apply its customizations and override applications ordered before it
  - Can lead to inconsistent states across environments

## Pre-commit Integration

To ensure customization validation runs automatically during development, add the validation hook to your pre-commit configuration:

```yaml
- repo: https://github.com/agritheory/test_utils
  rev: v0.17.0
  hooks:
    - id: validate_customizations
      args: ['--app', 'your_app_name']
```

This will:
  - Run validation checks before each commit
  - Prevent commits that would introduce invalid customizations
  - Help maintain consistent customization standards across your development team

## Clean Customized Files

The `clean_customized_doctypes` pre-commit hook helps maintain clean and consistent customization files by removing unnecessary null values and normalizing JSON structure. This is particularly useful because the Frappe framework's UI can sometimes generate customization files with extraneous null fields that don't affect functionality.

### What the Cleaner Does

1. **Null Value Cleanup**
  - Removes fields with `null` values from customization objects
  - Preserves `null` values for specific fields that require them:
    * `default`
    * `value`
  - Processes both top-level fields and nested objects within arrays

2. **File Processing**
  - Scans all JSON files in the app's custom directories
  - Creates temporary files for safe processing
  - Only replaces files that have actually been modified
  - Preserves original file if no changes are needed


### Pre-commit Integration

Add to your pre-commit config:
```yaml
- repo: https://github.com/agritheory/test_utils
  rev: v0.17.0
  hooks:
    - id: clean_customized_doctypes
      args: ['--app', 'your_app_name']
```

The hook will:
  - Run automatically before each commit
  - Clean all customization files in the specified app
  - Report which files were modified
  - Exit with code 0 to allow the commit to proceed

### Example

Before cleaning:
```json
{
  "custom_fields": [
    {
      "fieldname": "custom_field",
      "label": "Custom Field",
      "hidden": null,
      "default": null,
      "description": null
    }
  ]
}
```

After cleaning:
```json
{
  "custom_fields": [
    {
      "fieldname": "custom_field",
      "label": "Custom Field",
      "default": null
    }
  ]
}
```

Note that `default` retains its null value as it's in the preservation list, while other null fields are removed.
