####################################
# --- Request/response schemas --- #
####################################

import re
from datetime import datetime
from typing import Dict
from typing import List
from typing import Optional

from databricks.sdk.service.sharing import RecipientInfo
from databricks.sdk.service.sharing import ShareInfo
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator


# read (cRud)
class RecipientMetadata(BaseModel):
    """Metadata for a recipient."""

    name: str
    auth_type: str
    created_at: datetime


# read (cRud)
class GetRecipientsResponse(BaseModel):
    """Response model for listing recipients."""

    Message: str
    Recipient: List[RecipientInfo]


class GetSharesResponse(BaseModel):
    """Response model for listing shares."""

    Message: str
    Share: List[ShareInfo]


# read (cRud)
class GetRecipientsQueryParams(BaseModel):
    """Query parameters for listing recipients."""

    prefix: Optional[str] = None
    page_size: Optional[int] = 100

    @field_validator("page_size")
    @classmethod
    def validate_page_size(cls, v):
        """Validate that page_size is greater than 0."""
        if v is not None and v <= 0:
            raise ValueError("page_size must be greater than 0")
        return v


class GetSharesQueryParams(BaseModel):
    """Query parameters for listing shares."""

    prefix: Optional[str] = None
    page_size: Optional[int] = 100

    @field_validator("page_size")
    @classmethod
    def validate_page_size(cls, v):
        """Validate that page_size is greater than 0."""
        if v is not None and v <= 0:
            raise ValueError("page_size must be greater than 0")
        return v


class GetPipelinesQueryParams(BaseModel):
    """Query parameters for listing pipelines."""

    search_string: Optional[str] = None
    page_size: Optional[int] = 100

    @field_validator("page_size")
    @classmethod
    def validate_page_size(cls, v):
        """Validate that page_size is greater than 0."""
        if v is not None and v <= 0:
            raise ValueError("page_size must be greater than 0")
        return v


class AddDataObjectsRequest(BaseModel):
    """Request model for adding data objects to a share."""

    tables: Optional[List[str]] = []
    views: Optional[List[str]] = []
    schemas: Optional[List[str]] = []

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "tables": ["catalog.schema.table1", "catalog.schema.table2"],
                "views": ["catalog.schema.view1"],
                "schemas": ["catalog.schema"],
            }
        }
    )


# delete (cruD)
class DeleteRecipientResponse(BaseModel):
    """Response model for deleting a recipient."""

    message: str
    status_code: int


class PipelineConfigurationModel(BaseModel):
    """
    Validation model for DLT pipeline configuration.

    This model ensures that the configuration dictionary contains all required keys
    and automatically sets fixed defaults for sequence_by and delete_expr.

    Required Keys (user-provided):
        pipelines.source_table: Source table in format catalog.schema.table
        pipelines.keys: Primary key column(s)
        pipelines.target_table: Target table name
        pipelines.scd_type: SCD type (valid values: "1", "2")

    Fixed Keys (auto-set):
        pipelines.sequence_by: Fixed to "_commit_version"
        pipelines.delete_expr: Fixed to "_change_type = 'delete'"
    """

    source_table: str = Field(..., alias="pipelines.source_table", description="Source table (catalog.schema.table)")
    keys: str = Field(..., alias="pipelines.keys", description="Primary key column(s)")
    target_table: str = Field(..., alias="pipelines.target_table", description="Target table name")
    scd_type: str = Field(..., alias="pipelines.scd_type", description="SCD type (1 or 2)")
    sequence_by: str = Field(
        default="_commit_version", alias="pipelines.sequence_by", description="Sequence column (fixed)"
    )
    delete_expr: str = Field(
        default="_change_type = 'delete'", alias="pipelines.delete_expr", description="Delete expression (fixed)"
    )

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "example": {
                "pipelines.source_table": "dltshr_prod.01_bronze.jc_citibike_01",
                "pipelines.keys": "ride_id",
                "pipelines.target_table": "jc_citibike_01_type2",  # Note: use underscores, not hyphens
                "pipelines.scd_type": "2",
            }
        },
    )

    @field_validator("source_table")
    @classmethod
    def validate_source_table(cls, v: str) -> str:
        """
        Validate source table format (catalog.schema.table).

        Each part (catalog, schema, table) must contain only valid characters:
        - Alphanumeric characters (a-z, A-Z, 0-9)
        - Underscores (_)
        """
        if not v or not v.strip():
            raise ValueError("source_table cannot be empty")
        parts = v.split(".")
        if len(parts) != 3:
            raise ValueError("source_table must be in format: catalog.schema.table")

        catalog, schema, table = parts

        # Validate each part
        for part_name, part_value in [("catalog", catalog), ("schema", schema), ("table", table)]:
            if not part_value.strip():
                raise ValueError(f"{part_name} name cannot be empty in source_table")

            # Check for valid characters
            if not re.match(r"^[a-zA-Z0-9_]+$", part_value.strip()):
                raise ValueError(
                    f"{part_name} '{part_value}' contains invalid characters. "
                    "Names can only contain alphanumeric characters and underscores (a-z, A-Z, 0-9, _). "
                    "Hyphens (-) and other special characters are not allowed."
                )

        return v.strip()

    @field_validator("keys")
    @classmethod
    def validate_keys(cls, v: str) -> str:
        """Validate that keys is not empty."""
        if not v or not v.strip():
            raise ValueError("keys cannot be empty")
        return v.strip()

    @field_validator("target_table")
    @classmethod
    def validate_target_table(cls, v: str) -> str:
        """
        Validate that target_table is not empty and contains only valid characters.

        Databricks table names can only contain:
        - Alphanumeric characters (a-z, A-Z, 0-9)
        - Underscores (_)

        Hyphens (-) and other special characters are NOT allowed.
        """
        if not v or not v.strip():
            raise ValueError("target_table cannot be empty")

        v_stripped = v.strip()

        # Check for valid characters (alphanumeric and underscore only)
        if not re.match(r"^[a-zA-Z0-9_]+$", v_stripped):
            raise ValueError(
                f"target_table '{v_stripped}' contains invalid characters. "
                "Table names can only contain alphanumeric characters and underscores (a-z, A-Z, 0-9, _). "
                "Hyphens (-) and other special characters are not allowed."
            )

        return v_stripped

    @field_validator("scd_type")
    @classmethod
    def validate_scd_type(cls, v: str) -> str:
        """Validate SCD type is either 1 or 2."""
        if v not in ["1", "2"]:
            raise ValueError("scd_type must be either '1' or '2'")
        return v

    @model_validator(mode="after")
    def ensure_fixed_defaults(self) -> "PipelineConfigurationModel":
        """Ensure fixed fields have the correct default values."""
        self.sequence_by = "_commit_version"
        self.delete_expr = "_change_type = 'delete'"
        return self


class CreatePipelineRequest(BaseModel):
    """
    Request model for creating a DLT pipeline.

    Args:
        target_catalog_name: Target catalog name
        target_schema_name: Target schema name
        configuration: Pipeline configuration with required keys (includes target_table)
        notifications_list: List of email addresses or AD group names for notifications
        tags: Dictionary of tags for the pipeline
        serverless: Whether to use serverless compute (default: False)
    """

    target_catalog_name: str = Field(..., description="Target catalog name")
    target_schema_name: str = Field(..., description="Target schema name")
    configuration: PipelineConfigurationModel = Field(..., description="Pipeline configuration")
    notifications_list: List[str] = Field(
        default_factory=list, description="Email addresses or AD group names for notifications"
    )
    tags: Dict[str, str] = Field(default_factory=dict, description="Pipeline tags")
    serverless: bool = Field(default=False, description="Use serverless compute")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "target_catalog_name": "dltshr_prod",
                "target_schema_name": "02_silver",
                "configuration": {
                    "pipelines.source_table": "dltshr_prod.01_bronze.jc_citibike_01",
                    "pipelines.keys": "ride_id",
                    "pipelines.target_table": "jc_citibike_01_type2",
                    "pipelines.scd_type": "2",
                },
                "notifications_list": ["user@example.com", "data-engineering-team", "admin_group"],
                "tags": {"env": "prod", "team": "data-engineering"},
                "serverless": True,
            }
        }
    )

    @field_validator("target_catalog_name", "target_schema_name")
    @classmethod
    def validate_identifier(cls, v: str) -> str:
        """Validate catalog/schema names."""
        if not v or not v.strip():
            raise ValueError("Identifier cannot be empty")
        # Basic validation: no spaces, must contain only valid characters
        if " " in v:
            raise ValueError("Identifier cannot contain spaces")
        if not re.match(r"^[a-zA-Z0-9_]+$", v):
            raise ValueError("Identifier can only contain alphanumeric characters and underscores")
        return v.strip()

    @field_validator("notifications_list")
    @classmethod
    def validate_notifications(cls, v: List[str]) -> List[str]:
        """
        Validate notification recipients (email addresses or AD group names).

        Accepts:
            - Email addresses (e.g., user@example.com)
            - AD group names (e.g., data-engineering-team, admin_group)

        Raises:
            ValueError: If a recipient is neither a valid email nor a valid AD group name
        """
        if not v:
            return v

        email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        # AD group names: alphanumeric, hyphens, underscores, periods, no spaces
        ad_group_pattern = re.compile(r"^[a-zA-Z0-9._-]+$")

        for recipient in v:
            recipient_stripped = recipient.strip()
            if not recipient_stripped:
                raise ValueError("Notification recipient cannot be empty")

            # Check if it's a valid email
            is_email = email_pattern.match(recipient_stripped)
            # Check if it's a valid AD group name
            is_ad_group = ad_group_pattern.match(recipient_stripped) and "@" not in recipient_stripped

            if not (is_email or is_ad_group):
                raise ValueError(
                    f"Invalid notification recipient: {recipient}. "
                    "Must be a valid email address or AD group name (alphanumeric with .-_ allowed)"
                )

        return v


class UpdatePipelineConfigurationModel(BaseModel):
    """
    Validation model for updating DLT pipeline configuration.

    This model allows partial updates - only keys and target_table can be updated.
    At least one field must be provided. Fixed defaults (sequence_by, delete_expr) are automatically set.

    Updateable Keys (user can provide either or both):
        pipelines.keys: Primary key column(s)
        pipelines.target_table: Target table name

    Fixed/Non-updateable (cannot be changed after creation):
        pipelines.source_table: Source table (set during creation, immutable)
        pipelines.scd_type: SCD type (set during creation, immutable)

    Fixed Keys (auto-set):
        pipelines.sequence_by: Fixed to "_commit_version"
        pipelines.delete_expr: Fixed to "_change_type = 'delete'"
    """

    keys: Optional[str] = Field(None, alias="pipelines.keys", description="Primary key column(s)")
    target_table: Optional[str] = Field(None, alias="pipelines.target_table", description="Target table name")
    sequence_by: str = Field(
        default="_commit_version", alias="pipelines.sequence_by", description="Sequence column (fixed)"
    )
    delete_expr: str = Field(
        default="_change_type = 'delete'", alias="pipelines.delete_expr", description="Delete expression (fixed)"
    )

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {"pipelines.keys": "ride_id"},
                {"pipelines.target_table": "new_table_name"},
                {"pipelines.keys": "ride_id,trip_id", "pipelines.target_table": "new_table_name"},
            ]
        },
    )

    @field_validator("keys")
    @classmethod
    def validate_keys(cls, v: Optional[str]) -> Optional[str]:
        """Validate that keys is not empty if provided."""
        if v is None:
            return v
        if not v or not v.strip():
            raise ValueError("keys cannot be empty")
        return v.strip()

    @field_validator("target_table")
    @classmethod
    def validate_target_table(cls, v: Optional[str]) -> Optional[str]:
        """
        Validate that target_table is not empty and contains only valid characters.

        Databricks table names can only contain:
        - Alphanumeric characters (a-z, A-Z, 0-9)
        - Underscores (_)

        Hyphens (-) and other special characters are NOT allowed.
        """
        if v is None:
            return v
        if not v or not v.strip():
            raise ValueError("target_table cannot be empty")

        v_stripped = v.strip()

        # Check for valid characters (alphanumeric and underscore only)
        if not re.match(r"^[a-zA-Z0-9_]+$", v_stripped):
            raise ValueError(
                f"target_table '{v_stripped}' contains invalid characters. "
                "Table names can only contain alphanumeric characters and underscores (a-z, A-Z, 0-9, _). "
                "Hyphens (-) and other special characters are not allowed."
            )

        return v_stripped

    @model_validator(mode="after")
    def validate_at_least_one_field(self) -> "UpdatePipelineConfigurationModel":
        """Ensure at least one configuration field is provided and set fixed defaults."""
        # Check if at least one optional field is provided
        if not any([self.keys, self.target_table]):
            raise ValueError(
                "At least one configuration field must be provided: pipelines.keys or pipelines.target_table"
            )

        # Ensure fixed fields have the correct default values
        self.sequence_by = "_commit_version"
        self.delete_expr = "_change_type = 'delete'"
        return self


class UpdatePipelineLibrariesModel(BaseModel):
    """
    Model for updating DLT pipeline libraries and/or root folder path.

    At least one field must be provided (library_path OR root_path, or both).
    """

    library_path: Optional[str] = Field(
        None,
        alias="library_path",
        description="Workspace path to the notebook file (e.g., '/Workspace/Shared/.bundle/.../pattern-load.py')",
    )
    root_path: Optional[str] = Field(
        None,
        alias="root_path",
        description="Root folder path for pipeline files (e.g., '/Workspace/Shared/.bundle/.../pattern')",
    )

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "library_path": "/Workspace/Shared/.bundle/dab_project/prod/files/citibike_etl/dlt/pattern/pattern-load.py"
                },
                {"root_path": "/Workspace/Shared/.bundle/dab_project/prod/files/citibike_etl/dlt/pattern"},
                {
                    "library_path": "/Workspace/Shared/.bundle/dab_project/prod/files/citibike_etl/dlt/pattern/pattern-load.py",
                    "root_path": "/Workspace/Shared/.bundle/dab_project/prod/files/citibike_etl/dlt/pattern",
                },
            ]
        },
    )

    @field_validator("library_path")
    @classmethod
    def validate_library_path(cls, v: Optional[str]) -> Optional[str]:
        """
        Validate library path is not empty and starts with /Workspace or /Repos.

        Library paths must:
        - Start with /Workspace or /Repos
        - Not be empty
        - End with .py (Python notebook)
        - Be a valid workspace path
        """
        if v is None:
            return v
        if not v or not v.strip():
            raise ValueError("library_path cannot be empty")

        v_stripped = v.strip()

        # Check if it starts with valid workspace prefix
        if not (v_stripped.startswith("/Workspace/") or v_stripped.startswith("/Repos/")):
            raise ValueError(
                f"library_path '{v_stripped}' must start with '/Workspace/' or '/Repos/'. "
                "Library paths must be valid workspace paths."
            )

        # Check if it ends with .py (Python notebook/file)
        if not v_stripped.endswith(".py"):
            raise ValueError(
                f"library_path '{v_stripped}' must end with '.py'. " "DLT pipelines require Python notebook files."
            )

        return v_stripped

    @field_validator("root_path")
    @classmethod
    def validate_root_path(cls, v: Optional[str]) -> Optional[str]:
        """
        Validate root path is not empty and starts with /Workspace or /Repos.

        Root paths must:
        - Start with /Workspace or /Repos
        - Not be empty
        - Be a valid workspace directory path
        """
        if v is None:
            return v
        if not v or not v.strip():
            raise ValueError("root_path cannot be empty")

        v_stripped = v.strip()

        # Check if it starts with valid workspace prefix
        if not (v_stripped.startswith("/Workspace/") or v_stripped.startswith("/Repos/")):
            raise ValueError(
                f"root_path '{v_stripped}' must start with '/Workspace/' or '/Repos/'. "
                "Root paths must be valid workspace paths."
            )

        # Root path should be a directory, not a file (shouldn't end with .py, .sql, etc.)
        if v_stripped.endswith((".py", ".sql", ".scala", ".r")):
            raise ValueError(
                f"root_path '{v_stripped}' appears to be a file path, not a directory. "
                "Root path should be a folder/directory path."
            )

        return v_stripped

    @model_validator(mode="after")
    def validate_at_least_one_field(self) -> "UpdatePipelineLibrariesModel":
        """Ensure at least one field is provided (library_path or root_path)."""
        if not any([self.library_path, self.root_path]):
            raise ValueError("At least one field must be provided: library_path or root_path")
        return self


class UpdatePipelineNotificationsModel(BaseModel):
    """
    Model for updating DLT pipeline notification recipients.

    Allows updating the list of email addresses and/or AD group names to receive pipeline notifications.
    Used for both adding and removing notifications.
    """

    notifications_list: List[str] = Field(
        ...,
        alias="notifications_list",
        description="List of email addresses or AD group names for notifications",
        min_length=1,
    )

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {"notifications_list": ["user@example.com", "data-engineering-team"]},
                {
                    "notifications_list": [
                        "admin@company.com",
                        "data_team",
                        "monitoring-alerts",
                        "devops@company.com",
                    ]
                },
            ]
        },
    )

    @field_validator("notifications_list")
    @classmethod
    def validate_notifications(cls, v: List[str]) -> List[str]:
        """
        Validate notification recipients (email addresses or AD group names).

        Accepts:
            - Email addresses (e.g., user@example.com)
            - AD group names (e.g., data-engineering-team, admin_group)

        Raises:
            ValueError: If a recipient is neither a valid email nor a valid AD group name
        """
        if not v:
            raise ValueError("notifications_list cannot be empty")

        validated_list = []
        for recipient in v:
            recipient_stripped = recipient.strip()
            if not recipient_stripped:
                raise ValueError("Notification recipient cannot be empty")

            # Check if it's a valid email or AD group name
            # Email regex pattern
            email_pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
            # AD group name pattern (alphanumeric, underscores, hyphens)
            ad_group_pattern = r"^[a-zA-Z0-9_-]+$"

            is_email = re.match(email_pattern, recipient_stripped)
            is_ad_group = re.match(ad_group_pattern, recipient_stripped)

            if not (is_email or is_ad_group):
                raise ValueError(
                    f"Invalid notification recipient '{recipient_stripped}'. "
                    "Must be either a valid email address (user@example.com) or "
                    "an AD group name (alphanumeric with underscores/hyphens)."
                )

            validated_list.append(recipient_stripped)

        return validated_list


class UpdatePipelineTagsModel(BaseModel):
    """
    Model for updating DLT pipeline tags.

    Allows adding or updating tags (key-value pairs) for a pipeline.
    Tags are used for organization, cost tracking, and resource management.
    """

    tags: Dict[str, str] = Field(
        ...,
        alias="tags",
        description="Dictionary of tag key-value pairs",
        min_length=1,
    )

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {"tags": {"env": "prod", "team": "data-engineering"}},
                {
                    "tags": {
                        "environment": "production",
                        "team": "data-engineering",
                        "cost_center": "analytics",
                        "owner": "admin@example.com",
                        "project": "deltashare",
                    }
                },
            ]
        },
    )

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, v: Dict[str, str]) -> Dict[str, str]:
        """
        Validate tag key-value pairs.

        Rules:
        - Keys and values cannot be empty
        - Keys should be alphanumeric with underscores, hyphens, or dots
        - Values can contain more characters but should be reasonable

        Raises:
            ValueError: If any tag key or value is invalid
        """
        if not v:
            raise ValueError("tags dictionary cannot be empty")

        validated_tags = {}
        for key, value in v.items():
            # Validate key
            key_stripped = key.strip()
            if not key_stripped:
                raise ValueError("Tag key cannot be empty")

            # Tag key pattern: alphanumeric, underscores, hyphens, dots
            if not re.match(r"^[a-zA-Z0-9_.-]+$", key_stripped):
                raise ValueError(
                    f"Invalid tag key '{key_stripped}'. "
                    "Keys can only contain alphanumeric characters, underscores, hyphens, and dots (a-z, A-Z, 0-9, _, -, .)."
                )

            # Validate value
            value_stripped = value.strip() if value else ""
            if not value_stripped:
                raise ValueError(f"Tag value for key '{key_stripped}' cannot be empty")

            # Tag value can be more flexible but should be reasonable
            if len(value_stripped) > 256:
                raise ValueError(f"Tag value for key '{key_stripped}' is too long (max 256 characters)")

            validated_tags[key_stripped] = value_stripped

        return validated_tags


class UpdatePipelineTagsRemoveModel(BaseModel):
    """
    Model for removing tags from DLT pipelines.

    Allows removing specific tag keys from the pipeline.
    """

    tag_keys: List[str] = Field(
        ...,
        alias="tag_keys",
        description="List of tag keys to remove",
        min_length=1,
    )

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {"tag_keys": ["old_env", "deprecated_tag"]},
                {"tag_keys": ["cost_center", "owner", "billing_group"]},
            ]
        },
    )

    @field_validator("tag_keys")
    @classmethod
    def validate_tag_keys(cls, v: List[str]) -> List[str]:
        """
        Validate tag keys.

        Rules:
        - Keys cannot be empty
        - Keys should be alphanumeric with underscores, hyphens, or dots

        Raises:
            ValueError: If any tag key is invalid
        """
        if not v:
            raise ValueError("tag_keys list cannot be empty")

        validated_keys = []
        for key in v:
            key_stripped = key.strip()
            if not key_stripped:
                raise ValueError("Tag key cannot be empty")

            # Tag key pattern: alphanumeric, underscores, hyphens, dots
            if not re.match(r"^[a-zA-Z0-9_.-]+$", key_stripped):
                raise ValueError(
                    f"Invalid tag key '{key_stripped}'. "
                    "Keys can only contain alphanumeric characters, underscores, hyphens, and dots (a-z, A-Z, 0-9, _, -, .)."
                )

            validated_keys.append(key_stripped)

        return validated_keys


class UpdatePipelineContinuousModel(BaseModel):
    """
    Model for updating DLT pipeline continuous mode.

    Continuous mode determines whether the pipeline runs continuously or in triggered mode.
    - continuous=True: Pipeline processes data continuously as it arrives
    - continuous=False: Pipeline runs in triggered mode (manual or scheduled)
    """

    continuous: bool = Field(
        ...,
        description="Whether the pipeline should run in continuous mode (True) or triggered mode (False)",
    )

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {"continuous": True},
                {"continuous": False},
            ]
        },
    )
