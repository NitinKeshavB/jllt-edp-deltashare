# Excel Template - IP Management Fields

## Recipients Sheet Columns

The Recipients sheet now supports **both approaches** for IP management:

### Complete Column List

| Column # | Column Name | Required | Description |
|----------|-------------|----------|-------------|
| 1 | `name` | ✅ Yes | Recipient name |
| 2 | `type` | ✅ Yes | D2D or D2O |
| 3 | `recipient` | ✅ Yes | Contact email |
| 4 | `recipient_databricks_org` | D2D only | Metastore ID for D2D |
| 5 | `recipient_ips` | **Option 1** | Declarative: Complete list of IPs |
| 6 | `token_expiry` | Optional | Days until token expires (D2O) |
| 7 | `token_rotation` | Optional | Enable token rotation (D2O) |
| 8 | `description` | Optional | Recipient description |
| 9 | `recipient_ips_to_add` | **Option 2a** | Explicit: IPs to add |
| 10 | `recipient_ips_to_remove` | **Option 2b** | Explicit: IPs to remove |

## Two Approaches

### Approach 1: Declarative (Use Column 5)

**When to use:** You know all IPs that should exist

| name | type | recipient_ips | recipient_ips_to_add | recipient_ips_to_remove |
|------|------|---------------|----------------------|-------------------------|
| my-recipient | D2O | 192.168.1.0/24,10.0.0.50 | | |

- Specify complete list in `recipient_ips` column
- Leave `recipient_ips_to_add` and `recipient_ips_to_remove` empty

### Approach 2: Explicit (Use Columns 9 & 10)

**When to use:** You only want to add/remove specific IPs (easier!)

| name | type | recipient_ips | recipient_ips_to_add | recipient_ips_to_remove |
|------|------|---------------|----------------------|-------------------------|
| my-recipient | D2O | | 10.0.0.100,172.16.0.0/16 | 192.168.2.0/24 |

- Leave `recipient_ips` column empty
- Use `recipient_ips_to_add` for IPs to add
- Use `recipient_ips_to_remove` for IPs to remove
- You can use just one or both

## Examples

### Example 1: Add One IP (Explicit)
| name | type | recipient_ips_to_add | recipient_ips_to_remove |
|------|------|----------------------|-------------------------|
| prod-recipient | D2O | 10.0.0.200 | |

✅ **Result:** Adds 10.0.0.200, keeps all existing IPs

### Example 2: Remove One IP (Explicit)
| name | type | recipient_ips_to_add | recipient_ips_to_remove |
|------|------|----------------------|-------------------------|
| prod-recipient | D2O | | 192.168.1.50 |

✅ **Result:** Removes 192.168.1.50, keeps all other IPs

### Example 3: Add and Remove (Explicit)
| name | type | recipient_ips_to_add | recipient_ips_to_remove |
|------|------|----------------------|-------------------------|
| prod-recipient | D2O | 172.20.0.0/16 | 10.0.0.50 |

✅ **Result:** Adds 172.20.0.0/16, removes 10.0.0.50, keeps everything else

### Example 4: Replace All IPs (Declarative)
| name | type | recipient_ips |
|------|------|---------------|
| prod-recipient | D2O | 172.16.0.0/16,172.17.0.0/16 |

✅ **Result:** Removes all old IPs, sets exactly these two

## IP Format

### Single IP
```
10.0.0.50
```

### Multiple IPs (Comma-Separated)
```
192.168.1.0/24,10.0.0.50,172.16.0.0/16
```

### CIDR Notation Supported
```
192.168.1.0/24,10.0.0.0/16
```

## Important Notes

⚠️ **Do NOT mix approaches!**
- If `recipient_ips` has a value, the explicit columns are ignored
- Use EITHER declarative OR explicit, not both

✅ **Safe to leave empty**
- If all three columns are empty, IPs remain unchanged

✅ **Idempotent**
- Adding existing IP → Skipped silently
- Removing non-existent IP → Skipped silently

## Excel Parser Support

The Excel parser automatically handles both approaches:
- Recognizes column headers (case-insensitive)
- Splits comma-separated values into lists
- Trims whitespace from IPs
- Passes to provisioning logic

## Migration from Old Template

Old template only had `recipient_ips` column - **no changes needed!**

Your existing Excel files continue to work with the declarative approach.

New columns (`recipient_ips_to_add`, `recipient_ips_to_remove`) are optional and provide an easier way to manage IPs incrementally.

## Sample Row

Here's a complete example row showing all columns:

| name | type | recipient | recipient_databricks_org | recipient_ips | token_expiry | token_rotation | description | recipient_ips_to_add | recipient_ips_to_remove |
|------|------|-----------|-------------------------|---------------|--------------|----------------|-------------|----------------------|-------------------------|
| prod-recipient | D2O | prod@example.com | | | 30 | false | Production analytics | 10.0.0.200 | |

This recipient will:
- Be named "prod-recipient" (D2O type)
- Have existing IPs unchanged
- Add 10.0.0.200 to the IP allowlist
- Keep all other existing IPs

## Questions?

**Q: Which approach should I use?**
A: Use **explicit** (`recipient_ips_to_add` / `recipient_ips_to_remove`) for easier, safer updates!

**Q: Can I switch between approaches?**
A: Yes! Each update can use whichever approach makes sense for that operation.

**Q: What happens if I accidentally use both?**
A: Declarative takes precedence - the explicit columns are ignored if `recipient_ips` has a value.
