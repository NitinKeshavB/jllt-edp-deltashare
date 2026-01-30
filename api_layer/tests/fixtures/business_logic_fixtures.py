"""Fixtures for business logic mocks."""

from datetime import datetime
from datetime import timezone
from unittest.mock import patch

import pytest
from databricks.sdk.service.sharing import AuthenticationType
from databricks.sdk.service.sharing import RecipientTokenInfo


@pytest.fixture
def mock_share_business_logic(mock_share_info):
    """Mock all share business logic functions."""
    with (
        patch("dbrx_api.routes.routes_share.list_shares_all") as mock_list,
        patch("dbrx_api.routes.routes_share.get_shares") as mock_get,
        patch("dbrx_api.routes.routes_share.create_share_func") as mock_create,
        patch("dbrx_api.routes.routes_share.delete_share") as mock_delete,
        patch("dbrx_api.routes.routes_share.add_data_object_to_share") as mock_add_objects,
        patch("dbrx_api.routes.routes_share.revoke_data_object_from_share") as mock_revoke_objects,
        patch("dbrx_api.routes.routes_share.adding_recipients_to_share") as mock_add_recipients,
        patch("dbrx_api.routes.routes_share.removing_recipients_from_share") as mock_remove_recipients,
    ):
        # Setup return values
        mock_list.return_value = [mock_share_info(name="share1"), mock_share_info(name="share2")]
        mock_get.return_value = mock_share_info(name="test_share")
        mock_create.return_value = mock_share_info(name="new_share")
        mock_delete.return_value = None
        mock_add_objects.return_value = mock_share_info(name="test_share")
        mock_revoke_objects.return_value = mock_share_info(name="test_share")
        mock_add_recipients.return_value = None
        mock_remove_recipients.return_value = None

        yield {
            "list": mock_list,
            "get": mock_get,
            "create": mock_create,
            "delete": mock_delete,
            "add_objects": mock_add_objects,
            "revoke_objects": mock_revoke_objects,
            "add_recipients": mock_add_recipients,
            "remove_recipients": mock_remove_recipients,
        }


@pytest.fixture
def mock_recipient_business_logic(mock_recipient_info):
    """Mock all recipient business logic functions."""
    with (
        patch("dbrx_api.routes.routes_recipient.list_recipients") as mock_list,
        patch("dbrx_api.routes.routes_recipient.get_recipient_by_name") as mock_get,
        patch("dbrx_api.routes.routes_recipient.create_recipient_for_d2d") as mock_create_d2d,
        patch("dbrx_api.routes.routes_recipient.create_recipient_for_d2o") as mock_create_d2o,
        patch("dbrx_api.routes.routes_recipient.delete_recipient") as mock_delete,
        patch("dbrx_api.routes.routes_recipient.rotate_recipient_token") as mock_rotate,
        patch("dbrx_api.routes.routes_recipient.add_recipient_ip") as mock_add_ip,
        patch("dbrx_api.routes.routes_recipient.revoke_recipient_ip") as mock_revoke_ip,
        patch("dbrx_api.routes.routes_recipient.update_recipient_description") as mock_update_desc,
        patch("dbrx_api.routes.routes_recipient.update_recipient_expiration_time") as mock_update_exp,
    ):
        # Setup return values
        mock_list.return_value = [mock_recipient_info(name="recipient1"), mock_recipient_info(name="recipient2")]
        mock_get.return_value = mock_recipient_info(name="test_recipient")
        mock_create_d2d.return_value = mock_recipient_info(
            name="new_d2d_recipient", auth_type=AuthenticationType.DATABRICKS
        )
        mock_create_d2o.return_value = mock_recipient_info(
            name="new_d2o_recipient", auth_type=AuthenticationType.TOKEN
        )
        mock_delete.return_value = None
        mock_rotate.return_value = mock_recipient_info(
            name="test_recipient",
            tokens=[
                RecipientTokenInfo(
                    id="new_token_id",
                    activation_url="https://test-activation.databricks.com",
                    created_at=int(datetime.now(timezone.utc).timestamp() * 1000),
                )
            ],
        )
        mock_add_ip.return_value = mock_recipient_info(name="test_recipient")
        mock_revoke_ip.return_value = mock_recipient_info(name="test_recipient")
        mock_update_desc.return_value = mock_recipient_info(name="test_recipient", comment="Updated description")
        mock_update_exp.return_value = mock_recipient_info(name="test_recipient")

        yield {
            "list": mock_list,
            "get": mock_get,
            "create_d2d": mock_create_d2d,
            "create_d2o": mock_create_d2o,
            "delete": mock_delete,
            "rotate": mock_rotate,
            "add_ip": mock_add_ip,
            "revoke_ip": mock_revoke_ip,
            "update_desc": mock_update_desc,
            "update_exp": mock_update_exp,
        }
