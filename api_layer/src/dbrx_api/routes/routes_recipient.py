import ipaddress
from typing import Optional

from databricks.sdk.service.sharing import AuthenticationType
from databricks.sdk.service.sharing import RecipientInfo
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from fastapi import Response
from fastapi import status
from fastapi.responses import JSONResponse
from loguru import logger

from dbrx_api.dependencies import get_workspace_url
from dbrx_api.dltshr.recipient import add_recipient_ip
from dbrx_api.dltshr.recipient import create_recipient_d2d as create_recipient_for_d2d
from dbrx_api.dltshr.recipient import create_recipient_d2o as create_recipient_for_d2o
from dbrx_api.dltshr.recipient import delete_recipient
from dbrx_api.dltshr.recipient import get_recipients as get_recipient_by_name
from dbrx_api.dltshr.recipient import list_recipients
from dbrx_api.dltshr.recipient import revoke_recipient_ip
from dbrx_api.dltshr.recipient import rotate_recipient_token
from dbrx_api.dltshr.recipient import update_recipient_description
from dbrx_api.dltshr.recipient import update_recipient_expiration_time
from dbrx_api.schemas.schemas import GetRecipientsQueryParams
from dbrx_api.schemas.schemas import GetRecipientsResponse

ROUTER_RECIPIENT = APIRouter(tags=["Recipients"])


@ROUTER_RECIPIENT.get(
    "/recipients/{recipient_name}",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Recipient not found",
            "content": {"application/json": {"example": {"detail": "Recipient not found"}}},
        },
    },
)
async def get_recipients(
    request: Request,
    recipient_name: str,
    response: Response,
    workspace_url: str = Depends(get_workspace_url),
) -> RecipientInfo:
    """Get a specific recipient by name."""
    logger.info(
        "Getting recipient by name",
        recipient_name=recipient_name,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )
    recipient = get_recipient_by_name(recipient_name, workspace_url)

    if recipient is None:
        logger.warning(
            "Recipient not found",
            recipient_name=recipient_name,
            http_status=404,
            http_method=request.method,
            url_path=str(request.url.path),
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Recipient not found: {recipient_name}",
        )

    if recipient:
        response.status_code = status.HTTP_200_OK

    logger.info(
        "Recipient retrieved successfully",
        recipient_name=recipient_name,
        auth_type=str(recipient.authentication_type),
        owner=recipient.owner,
    )
    return recipient


##########################


@ROUTER_RECIPIENT.get(
    "/recipients",
    responses={
        status.HTTP_200_OK: {
            "description": "Recipients fetched successfully",
            "content": {
                "application/json": {
                    "example": {
                        "Message": "Fetched 5 recipients!",
                        "Recipient": [],
                    }
                }
            },
        }
    },
)
async def list_recipients_all(
    request: Request,
    response: Response,
    query_params: GetRecipientsQueryParams = Depends(),
    workspace_url: str = Depends(get_workspace_url),
):
    """List all recipients or with optional prefix filtering."""
    logger.info(
        "Listing recipients",
        prefix=query_params.prefix,
        page_size=query_params.page_size,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    recipients = list_recipients(
        dltshr_workspace_url=workspace_url,
        prefix=query_params.prefix,
        max_results=query_params.page_size,
    )

    if len(recipients) == 0:
        logger.info("No recipients found", prefix=query_params.prefix)
        return JSONResponse(
            status_code=status.HTTP_200_OK, content={"detail": "No recipients found for search criteria."}
        )

    response.status_code = status.HTTP_200_OK
    message = f"Fetched {len(recipients)} recipients!"
    logger.info("Recipients retrieved successfully", count=len(recipients), prefix=query_params.prefix)
    return GetRecipientsResponse(Message=message, Recipient=recipients)


##########################


@ROUTER_RECIPIENT.delete(
    "/recipients/{recipient_name}",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Recipient not found",
            "content": {"application/json": {"example": {"detail": "Recipient not found"}}},
        },
        status.HTTP_200_OK: {
            "description": "Deleted Recipient successfully!",
            "content": {"application/json": {"example": {"detail": "Deleted Recipient successfully!"}}},
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "Permission denied to delete recipient",
            "content": {
                "application/json": {
                    "example": {"detail": "Permission denied to delete recipient as user is not the owner"}
                }
            },
        },
    },
)
async def delete_recipient_by_name(
    request: Request,
    recipient_name: str,
    workspace_url: str = Depends(get_workspace_url),
):
    """Delete a Recipient."""
    logger.info(
        "Deleting recipient",
        recipient_name=recipient_name,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )
    recipient = get_recipient_by_name(recipient_name, workspace_url)
    if recipient:
        response = delete_recipient(recipient_name, workspace_url)
        if response == "User is not an owner of Recipient":
            logger.warning("Permission denied to delete recipient", recipient_name=recipient_name, error=response)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission denied to delete recipient as user is not the owner: {recipient_name}",
            )
        logger.info("Recipient deleted successfully", recipient_name=recipient_name, status_code=status.HTTP_200_OK)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"message": "Deleted Recipient successfully!"},
        )

    logger.warning(
        "Recipient not found for deletion",
        recipient_name=recipient_name,
        http_status=404,
        http_method=request.method,
        url_path=str(request.url.path),
    )
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Recipient not found: {recipient_name}",
    )


##########################


@ROUTER_RECIPIENT.post(
    "/recipients/d2d/{recipient_name}",
    responses={
        status.HTTP_201_CREATED: {
            "description": "Recipients created successfully",
            "content": {"application/json": {"example": {"Message": "Recipient created successfully!"}}},
        },
        status.HTTP_409_CONFLICT: {
            "description": "Recipient already exists",
            "content": {"application/json": {"example": {"Message": "Recipient already exists"}}},
        },
    },
)
async def create_recipient_databricks_to_databricks(
    request: Request,
    response: Response,
    recipient_name: str,
    recipient_identifier: str,
    description: str,
    sharing_code: Optional[str] = None,
    workspace_url: str = Depends(get_workspace_url),
) -> RecipientInfo:
    """Create a recipient for Databricks to Databricks sharing."""
    logger.info(
        "Creating D2D recipient",
        recipient_name=recipient_name,
        recipient_identifier=recipient_identifier,
        description=description,
        sharing_code=sharing_code,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )
    recipient = get_recipient_by_name(recipient_name, workspace_url)

    if recipient:
        logger.warning("Recipient already exists", recipient_name=recipient_name)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Recipient already exists: {recipient_name}",
        )

    recipient = create_recipient_for_d2d(
        recipient_name=recipient_name,
        recipient_identifier=recipient_identifier,
        description=description,
        sharing_code=sharing_code,
        dltshr_workspace_url=workspace_url,
    )

    if isinstance(recipient, str) and recipient.startswith("Invalid recipient_identifier"):
        logger.error("Invalid recipient identifier", recipient_name=recipient_name, error=recipient)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=recipient,
        )

    if isinstance(recipient, str) and "already exists with same sharing identifier" in recipient:
        logger.warning("Recipient with same identifier already exists", recipient_name=recipient_name, error=recipient)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=recipient,
        )

    if recipient:
        response.status_code = status.HTTP_201_CREATED
        logger.info("D2D recipient created successfully", recipient_name=recipient_name, owner=recipient.owner)
    return recipient


##########################


@ROUTER_RECIPIENT.post(
    "/recipients/d2o/{recipient_name}",
    responses={
        status.HTTP_201_CREATED: {
            "description": "Recipients created successfully",
            "content": {"application/json": {"example": {"Message": "Recipient created successfully!"}}},
        },
        status.HTTP_409_CONFLICT: {
            "description": "Recipient already exists",
            "content": {"application/json": {"example": {"Message": "Recipient already exists"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Invalid IP addresses or CIDR blocks",
            "content": {"application/json": {"example": {"Message": "Invalid IP addresses or CIDR blocks"}}},
        },
    },
)
async def create_recipient_databricks_to_opensharing(
    request: Request,
    response: Response,
    recipient_name: str,
    description: str,
    ip_access_list: Optional[str] = Query(
        default=None,
        description="Comma-delimited list of IP addresses or CIDR blocks (e.g., '192.168.1.1,10.0.0.0/24')",
    ),
    workspace_url: str = Depends(get_workspace_url),
) -> RecipientInfo:
    """Create a recipient for Databricks to Databricks sharing."""
    # Parse comma-delimited IP access list
    parsed_ip_list = None
    if ip_access_list:
        parsed_ip_list = [ip.strip() for ip in ip_access_list.split(",") if ip.strip()]

    logger.info(
        "Creating D2O recipient",
        recipient_name=recipient_name,
        description=description,
        ip_access_list=parsed_ip_list,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )
    recipient = get_recipient_by_name(recipient_name, workspace_url)

    if recipient:
        logger.warning("Recipient already exists", recipient_name=recipient_name)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Recipient already exists: {recipient_name}",
        )

    # Validate IP access list if provided
    if parsed_ip_list and len(parsed_ip_list) > 0:
        invalid_ips = []
        for ip_str in parsed_ip_list:
            try:
                # Try parsing as network (supports both single IPs and CIDR)
                ipaddress.ip_network(ip_str.strip(), strict=False)
            except ValueError:
                invalid_ips.append(ip_str)

        if invalid_ips:
            logger.warning("Invalid IP addresses provided", recipient_name=recipient_name, invalid_ips=invalid_ips)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(f"Invalid IP addresses or CIDR blocks: " f"{', '.join(invalid_ips)}"),
            )

    recipient = create_recipient_for_d2o(
        recipient_name=recipient_name,
        description=description,
        ip_access_list=parsed_ip_list,
        dltshr_workspace_url=workspace_url,
    )

    if recipient:
        response.status_code = status.HTTP_201_CREATED
        logger.info("D2O recipient created successfully", recipient_name=recipient_name, owner=recipient.owner)
    return recipient


##########################


@ROUTER_RECIPIENT.put(
    "/recipients/{recipient_name}/tokens/rotate",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Recipient not found",
            "content": {"application/json": {"example": {"Message": "Recipient not found"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "expire_in_seconds must be a non-negative integer",
            "content": {
                "application/json": {"example": {"Message": "Iexpire_in_seconds must be a non-negative integer"}}
            },
        },
    },
)
async def rotate_recipient_tokens(
    request: Request,
    response: Response,
    recipient_name: str,
    expire_in_seconds: int = 0,
    workspace_url: str = Depends(get_workspace_url),
) -> RecipientInfo:
    """Rotate a recipient token for Databricks to opensharing protocol."""
    logger.info(
        "Rotating recipient token",
        recipient_name=recipient_name,
        expire_in_seconds=expire_in_seconds,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )
    if expire_in_seconds < 0:
        logger.warning(
            "Invalid expire_in_seconds value", recipient_name=recipient_name, expire_in_seconds=expire_in_seconds
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="expire_in_seconds must be a non-negative integer",
        )

    recipient = get_recipient_by_name(recipient_name, workspace_url)

    if not recipient:
        logger.warning(
            "Recipient not found for token rotation",
            recipient_name=recipient_name,
            http_status=404,
            http_method=request.method,
            url_path=str(request.url.path),
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Recipient not found: {recipient_name}",
        )

    recipient = rotate_recipient_token(
        recipient_name=recipient_name,
        expire_in_seconds=expire_in_seconds,
        dltshr_workspace_url=workspace_url,
    )

    if isinstance(recipient, str) and "Cannot extend the token expiration time" in recipient:
        logger.error("Cannot extend token expiration time", recipient_name=recipient_name, error=recipient)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=recipient,
        )
    elif isinstance(recipient, str) and "Recipient already has maximum number of active tokens" in recipient:
        logger.warning("Recipient has maximum active tokens", recipient_name=recipient_name, error=recipient)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=recipient,
        )
    elif isinstance(recipient, str) and "Permission denied" in recipient:
        logger.warning("Permission denied to rotate token", recipient_name=recipient_name, error=recipient)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=recipient,
        )
    elif isinstance(recipient, str) and "non-TOKEN type recipient" in recipient:
        logger.warning("Cannot rotate token for non-TOKEN recipient", recipient_name=recipient_name, error=recipient)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=recipient,
        )
    else:
        response.status_code = status.HTTP_200_OK
        logger.info("Recipient token rotated successfully", recipient_name=recipient_name)
        return recipient


##########################


@ROUTER_RECIPIENT.put(
    "/recipients/{recipient_name}/ipaddress/add",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Recipient not found",
            "content": {"application/json": {"example": {"Message": "Recipient not found"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "IP access list cannot be empty",
            "content": {"application/json": {"example": {"Message": "IP access list cannot be empty"}}},
        },
    },
)
async def add_client_ip_to_databricks_opensharing(
    request: Request,
    response: Response,
    recipient_name: str,
    ip_access_list: str = Query(
        ...,
        description="Comma-delimited list of IP addresses or CIDR blocks to add (e.g., '192.168.1.1,10.0.0.0/24')",
    ),
    workspace_url: str = Depends(get_workspace_url),
):
    """Add IP to access list for Databricks to opensharing protocol."""
    # Parse comma-delimited IP access list
    parsed_ip_list = [ip.strip() for ip in ip_access_list.split(",") if ip.strip()]

    logger.info(
        "Adding IP addresses to recipient",
        recipient_name=recipient_name,
        ip_access_list=parsed_ip_list,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    recipient = get_recipient_by_name(recipient_name, workspace_url)

    if not recipient:
        logger.warning(
            "Recipient not found for IP addition",
            recipient_name=recipient_name,
            http_status=404,
            http_method=request.method,
            url_path=str(request.url.path),
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Recipient not found: {recipient_name}",
        )

    if recipient.authentication_type == AuthenticationType.DATABRICKS:
        logger.warning(
            "Cannot add IPs to D2D recipient",
            recipient_name=recipient_name,
            auth_type=str(recipient.authentication_type),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot add IP addresses for DATABRICKS to DATABRICKS type recipient. IP access lists only work with TOKEN authentication.",
        )

    if not parsed_ip_list or len(parsed_ip_list) == 0:
        logger.warning("Empty IP access list provided", recipient_name=recipient_name)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IP access list cannot be empty",
        )

    # Validate each IP address or CIDR block
    invalid_ips = []
    for ip_str in parsed_ip_list:
        try:
            # Try parsing as network (supports both single IPs and CIDR)
            ipaddress.ip_network(ip_str.strip(), strict=False)
        except ValueError:
            invalid_ips.append(ip_str)

    if invalid_ips:
        logger.warning(
            "Invalid IP addresses provided for addition", recipient_name=recipient_name, invalid_ips=invalid_ips
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(f"Invalid IP addresses or CIDR blocks: " f"{', '.join(invalid_ips)}"),
        )

    recipient = add_recipient_ip(recipient_name, parsed_ip_list, workspace_url)

    if isinstance(recipient, str) and "Permission denied" in recipient:
        logger.warning("Permission denied to add IPs", recipient_name=recipient_name, error=recipient)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=recipient,
        )
    else:
        response.status_code = status.HTTP_200_OK
        logger.info("IP addresses added successfully to recipient", recipient_name=recipient_name)
    return recipient


@ROUTER_RECIPIENT.put(
    "/recipients/{recipient_name}/ipaddress/revoke",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Recipient not found",
            "content": {"application/json": {"example": {"Message": "Recipient not found"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "IP access list cannot be empty",
            "content": {"application/json": {"example": {"Message": "IP access list cannot be empty"}}},
        },
    },
)
async def revoke_client_ip_from_databricks_opensharing(
    request: Request,
    response: Response,
    recipient_name: str,
    ip_access_list: str = Query(
        ...,
        description="Comma-delimited list of IP addresses or CIDR blocks to revoke (e.g., '192.168.1.1,10.0.0.0/24')",
    ),
    workspace_url: str = Depends(get_workspace_url),
) -> RecipientInfo:
    """revoke IP to access list for Databricks to opensharing protocol."""
    # Parse comma-delimited IP access list
    parsed_ip_list = [ip.strip() for ip in ip_access_list.split(",") if ip.strip()]

    logger.info(
        "Revoking IP addresses from recipient",
        recipient_name=recipient_name,
        ip_access_list=parsed_ip_list,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    recipient = get_recipient_by_name(recipient_name, workspace_url)

    if not recipient:
        logger.warning(
            "Recipient not found for IP revocation",
            recipient_name=recipient_name,
            http_status=404,
            http_method=request.method,
            url_path=str(request.url.path),
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Recipient not found: {recipient_name}",
        )

    if recipient.authentication_type == AuthenticationType.DATABRICKS:
        logger.warning(
            "Cannot revoke IPs from D2D recipient",
            recipient_name=recipient_name,
            auth_type=str(recipient.authentication_type),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot revoke IP addresses for DATABRICKS to DATABRICKS type recipient. IP access lists only work with TOKEN authentication.",
        )

    if not parsed_ip_list or len(parsed_ip_list) == 0:
        logger.warning("Empty IP access list provided for revocation", recipient_name=recipient_name)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="IP access list cannot be empty",
        )

    # Validate each IP address or CIDR block
    invalid_ips = []
    for ip_str in parsed_ip_list:
        try:
            # Try parsing as network (supports both single IPs and CIDR)
            ipaddress.ip_network(ip_str.strip(), strict=False)
        except ValueError:
            invalid_ips.append(ip_str)

    if invalid_ips:
        logger.warning(
            "Invalid IP addresses provided for revocation", recipient_name=recipient_name, invalid_ips=invalid_ips
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(f"Invalid IP addresses or CIDR blocks: " f"{', '.join(invalid_ips)}"),
        )

    # Check which IPs are not present in the recipient's current IP list
    current_ips = []
    if recipient.ip_access_list and recipient.ip_access_list.allowed_ip_addresses:
        current_ips = recipient.ip_access_list.allowed_ip_addresses

    ips_not_present = [ip for ip in parsed_ip_list if ip.strip() not in current_ips]

    if ips_not_present:
        logger.warning(
            "IPs not present in recipient's access list",
            recipient_name=recipient_name,
            ips_not_present=ips_not_present,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"The following IP addresses are not present in the recipient's "
                f"IP access list and cannot be revoked: {', '.join(ips_not_present)}"
            ),
        )

    recipient = revoke_recipient_ip(recipient_name, parsed_ip_list, workspace_url)

    if isinstance(recipient, str) and "Permission denied" in recipient:
        logger.warning("Permission denied to revoke IPs", recipient_name=recipient_name, error=recipient)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=recipient,
        )
    else:
        response.status_code = status.HTTP_200_OK
        logger.info("IP addresses revoked successfully from recipient", recipient_name=recipient_name)
    return recipient


@ROUTER_RECIPIENT.put(
    "/recipients/{recipient_name}/description/update",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Recipient not found",
            "content": {"application/json": {"example": {"Message": "Recipient not found"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Description cannot be empty",
            "content": {"application/json": {"example": {"Message": "Description cannot be empty"}}},
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "Permission denied to update description of recipient as user is not the owner",
            "content": {
                "application/json": {
                    "example": {
                        "Message": "Permission denied to update description of recipient as user is not the owner"
                    }
                }
            },
        },
    },
)
async def update_recipients_description(
    request: Request,
    recipient_name: str,
    description: str,
    response: Response,
    workspace_url: str = Depends(get_workspace_url),
) -> RecipientInfo:
    """Rotate a recipient token for Databricks to opensharing protocol."""
    logger.info(
        "Updating recipient description",
        recipient_name=recipient_name,
        description=description,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # Remove all quotes and spaces to check if description contains actual content
    cleaned_description = description.strip().replace('"', "").replace("'", "").replace(" ", "")

    if not description or not cleaned_description:
        logger.warning("Empty or invalid description provided", recipient_name=recipient_name)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Description cannot be empty or contain only spaces, quotes, or a combination thereof",
        )

    recipient = get_recipient_by_name(recipient_name, workspace_url)

    if not recipient:
        logger.warning(
            "Recipient not found for description update",
            recipient_name=recipient_name,
            http_status=404,
            http_method=request.method,
            url_path=str(request.url.path),
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Recipient not found: {recipient_name}",
        )

    recipient = update_recipient_description(
        recipient_name=recipient_name,
        description=description,
        dltshr_workspace_url=workspace_url,
    )

    if isinstance(recipient, str) and ("Permission denied" in recipient or "not an owner" in recipient):
        logger.warning("Permission denied to update description", recipient_name=recipient_name, error=recipient)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Permission denied to update description of recipient: " f"{recipient_name} as user is not the owner"
            ),
        )
    else:
        response.status_code = status.HTTP_200_OK
        logger.info("Recipient description updated successfully", recipient_name=recipient_name)
        return recipient


@ROUTER_RECIPIENT.put(
    "/recipients/{recipient_name}/expiration_time/update",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Recipient not found",
            "content": {"application/json": {"example": {"Message": "Recipient not found"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Expiration time in days cannot be negative or empty or zero",
            "content": {
                "application/json": {
                    "example": {"Message": "Expiration time in days cannot be negative or empty or zero"}
                }
            },
        },
    },
)
async def update_recipients_expiration_time(
    request: Request,
    recipient_name: str,
    expiration_time_in_days: int,
    response: Response,
    workspace_url: str = Depends(get_workspace_url),
) -> RecipientInfo:
    """Expires a recipient token for Databricks to opensharing protocol."""
    logger.info(
        "Updating recipient expiration time",
        recipient_name=recipient_name,
        expiration_time_in_days=expiration_time_in_days,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    recipient = get_recipient_by_name(recipient_name, workspace_url)

    if not recipient:
        logger.warning(
            "Recipient not found for expiration time update",
            recipient_name=recipient_name,
            http_status=404,
            http_method=request.method,
            url_path=str(request.url.path),
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Recipient not found: {recipient_name}",
        )

    elif recipient.authentication_type == AuthenticationType.DATABRICKS:
        logger.warning(
            "Cannot update expiration time for D2D recipient",
            recipient_name=recipient_name,
            auth_type=str(recipient.authentication_type),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot update expiration time for DATABRICKS to DATABRICKS type recipient. Expiration time only works with TOKEN authentication.",
        )
    elif expiration_time_in_days <= 0 or expiration_time_in_days is None:
        logger.warning(
            "Invalid expiration time provided",
            recipient_name=recipient_name,
            expiration_time_in_days=expiration_time_in_days,
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expiration time in days cannot be negative or empty",
        )
    else:
        recipient = update_recipient_expiration_time(
            recipient_name=recipient_name,
            expiration_time=expiration_time_in_days,
            dltshr_workspace_url=workspace_url,
        )

        if isinstance(recipient, str) and ("Permission denied" in recipient or "not an owner" in recipient):
            logger.warning(
                "Permission denied to update expiration time", recipient_name=recipient_name, error=recipient
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"Permission denied to update expiration time of recipient: "
                    f"{recipient_name} as user is not the owner"
                ),
            )
        else:
            response.status_code = status.HTTP_200_OK
            logger.info(
                "Recipient expiration time updated successfully",
                recipient_name=recipient_name,
                expiration_time_in_days=expiration_time_in_days,
            )
        return recipient
