# IP Address Management for D2O Recipients

## Overview

The system supports **TWO approaches** for managing IP addresses for D2O (Databricks-to-Open) recipients during UPDATE strategy:

1. **Declarative Approach** - Specify complete desired state
2. **Explicit Approach** - Specify only changes (easier for users)

## Approach 1: Declarative (Complete State)

Specify the **complete list** of IPs that should exist. The system calculates what to add and remove.

### YAML Example
```yaml
recipient:
  - name: my-recipient
    type: D2O
    recipient_ips:  # Complete list of desired IPs
      - 192.168.1.0/24
      - 10.0.0.50
```

### Excel Example
| name | type | recipient_ips |
|------|------|---------------|
| my-recipient | D2O | 192.168.1.0/24,10.0.0.50 |

### How It Works
```
Current IPs in Databricks: [192.168.1.0/24, 172.16.0.0/16]
Desired IPs in YAML:       [192.168.1.0/24, 10.0.0.50]

→ System adds:    [10.0.0.50]           (in YAML, not in Databricks)
→ System removes: [172.16.0.0/16]      (in Databricks, not in YAML)
```

### When to Use
- ✅ You know all IPs that should exist
- ✅ You want idempotent, predictable state
- ✅ Initial recipient creation (NEW strategy)

## Approach 2: Explicit (Incremental Changes)

Specify **only the changes** - which IPs to add and/or which to remove.

### YAML Example
```yaml
recipient:
  - name: my-recipient
    type: D2O
    recipient_ips_to_add:      # Only add these
      - 10.0.0.100
      - 172.16.0.0/16
    recipient_ips_to_remove:   # Only remove these
      - 192.168.2.0/24
```

### Excel Example
| name | type | recipient_ips_to_add | recipient_ips_to_remove |
|------|------|----------------------|-------------------------|
| my-recipient | D2O | 10.0.0.100,172.16.0.0/16 | 192.168.2.0/24 |

### How It Works
```
Current IPs in Databricks: [192.168.1.0/24, 192.168.2.0/24]
IPs to add from YAML:      [10.0.0.100, 172.16.0.0/16]
IPs to remove from YAML:   [192.168.2.0/24]

→ System adds:    [10.0.0.100, 172.16.0.0/16]
→ System removes: [192.168.2.0/24]
→ Keeps:          [192.168.1.0/24]          (not mentioned, so unchanged)
```

### When to Use
- ✅ You DON'T know all existing IPs
- ✅ You only want to add/remove specific IPs
- ✅ Incremental updates (easier for users!)
- ✅ You want to avoid accidentally removing IPs

### Flexibility
You can use:
- `recipient_ips_to_add` **alone** - only add IPs
- `recipient_ips_to_remove` **alone** - only remove IPs
- **Both together** - add some and remove others in one operation

## Comparison

| Feature | Declarative | Explicit |
|---------|-------------|----------|
| **Fields** | `recipient_ips` | `recipient_ips_to_add`<br>`recipient_ips_to_remove` |
| **User provides** | Complete final state | Only changes |
| **User needs to know** | All IPs that should exist | Only IPs to add/remove |
| **Idempotent** | ✅ Yes - always converges to same state | ✅ Yes - skips duplicates/non-existent |
| **Risk of accident** | ⚠️ Medium - typo removes all others | ✅ Low - only affects specified IPs |
| **Ease of use** | ⚠️ Must list all IPs | ✅ Only list changes |
| **Best for** | Initial creation, full sync | Incremental updates |

## Examples

### Example 1: Add One IP (Explicit - Easier!)
```yaml
recipient:
  - name: prod-recipient
    type: D2O
    recipient_ips_to_add:
      - 10.0.0.200  # Just add this one IP
```
**Result:** Adds 10.0.0.200, keeps all existing IPs

### Example 2: Remove One IP (Explicit - Easier!)
```yaml
recipient:
  - name: prod-recipient
    type: D2O
    recipient_ips_to_remove:
      - 192.168.1.50  # Just remove this one IP
```
**Result:** Removes 192.168.1.50, keeps all other IPs

### Example 3: Replace All IPs (Declarative)
```yaml
recipient:
  - name: prod-recipient
    type: D2O
    recipient_ips:  # New complete list
      - 172.16.0.0/16
      - 172.17.0.0/16
```
**Result:** Removes all old IPs, adds only these two

### Example 4: Add and Remove Together (Explicit)
```yaml
recipient:
  - name: prod-recipient
    type: D2O
    recipient_ips_to_add:
      - 172.20.0.0/16    # Add new datacenter
    recipient_ips_to_remove:
      - 10.0.0.50         # Remove old office IP
```
**Result:** Adds 172.20.0.0/16, removes 10.0.0.50, keeps everything else

## Error Handling

Both approaches are **idempotent** and safe:

### Declarative
```yaml
recipient_ips: [10.0.0.50]  # Already exists in Databricks
```
→ No error - system detects it's already there, no API call made

### Explicit
```yaml
recipient_ips_to_add: [10.0.0.50]      # Already exists
recipient_ips_to_remove: [192.168.1.1] # Doesn't exist
```
→ No error - system:
  - Skips adding 10.0.0.50 (already exists)
  - Skips removing 192.168.1.1 (doesn't exist)
  - Logs debug message for both

## Recommendations

### For Users
- ✅ **Use Explicit approach** for day-to-day updates (easier!)
- Use Declarative approach when you want full control over final state

### For Initial Creation (NEW Strategy)
- Only `recipient_ips` is supported (declarative)
- Explicit fields only work in UPDATE strategy

### For Safety
- Both approaches log all changes before executing
- Both approaches are idempotent (safe to run multiple times)

## Excel Template Updates

The Excel parser automatically supports both approaches - just add the column headers:

**Option 1: Declarative**
| name | type | recipient_ips |
|------|------|---------------|

**Option 2: Explicit**
| name | type | recipient_ips_to_add | recipient_ips_to_remove |
|------|------|----------------------|-------------------------|

Use comma-separated values for multiple IPs: `192.168.1.0/24,10.0.0.50`

## Logging

The system logs which approach is being used:

```
INFO: Using DECLARATIVE approach for prod-recipient IP management
DEBUG: Desired IPs from YAML: {192.168.1.0/24, 10.0.0.50}
DEBUG: Current IPs in Databricks: {192.168.1.0/24, 172.16.0.0/16}
INFO: Adding 1 IP(s): [10.0.0.50]
INFO: Removing 1 IP(s): [172.16.0.0/16]
```

or

```
INFO: Using EXPLICIT approach for prod-recipient IP management
DEBUG: Explicit IPs to add: [10.0.0.200]
DEBUG: Explicit IPs to remove: []
INFO: Adding 1 IP(s): [10.0.0.200]
```

## Questions?

**Q: Can I use both approaches together?**
A: No - if `recipient_ips` is present, it takes precedence and the explicit fields are ignored.

**Q: What if I specify an IP to add that already exists?**
A: No problem! The system checks and skips it (idempotent).

**Q: What if I specify an IP to remove that doesn't exist?**
A: No problem! The system checks and skips it (idempotent).

**Q: Which approach is better?**
A: **Explicit is easier for users** - you don't need to know all existing IPs, just specify what to add/remove.
