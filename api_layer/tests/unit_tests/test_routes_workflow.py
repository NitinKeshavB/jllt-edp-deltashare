"""Test suite for Workflow API endpoints."""

from io import BytesIO
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import status
from tests.consts import API_BASE

try:
    import python_multipart  # noqa: F401  # preferred; avoids PendingDeprecationWarning

    MULTIPART_AVAILABLE = True
except ImportError:
    try:
        import multipart  # noqa: F401  # fallback for older python-multipart

        MULTIPART_AVAILABLE = True
    except ImportError:
        MULTIPART_AVAILABLE = False


class TestWorkflowWhenDisabled:
    """When workflow is disabled, workflow routes should return 404."""

    def test_workflow_health_returns_404_when_disabled(self, client):
        """GET /api/workflow/health returns 404 when workflow is not enabled."""
        response = client.get(f"{API_BASE}/workflow/health")

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_workflow_upload_returns_404_when_disabled(self, client):
        """POST /api/workflow/sharepack/upload_and_validate returns 404 when workflow is not enabled."""
        response = client.post(
            f"{API_BASE}/workflow/sharepack/upload_and_validate",
            files={"file": ("pack.yaml", BytesIO(b"key: value"), "application/yaml")},
        )

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_workflow_sharepack_status_returns_404_when_disabled(self, client):
        """GET /api/workflow/sharepack/{id} returns 404 when workflow is not enabled."""
        response = client.get(f"{API_BASE}/workflow/sharepack/00000000-0000-0000-0000-000000000001")

        assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.skipif(not MULTIPART_AVAILABLE, reason="python-multipart required for workflow app")
class TestWorkflowHealthEndpoint:
    """Tests for GET /api/workflow/health when workflow is enabled."""

    def test_workflow_health_success(self, client_with_workflow, mock_domain_db_pool, mock_queue_client):
        """GET /api/workflow/health returns 200 when DB and queue are healthy."""
        response = client_with_workflow.get(f"{API_BASE}/workflow/health")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "Message" in data or "DatabaseConnected" in data
        assert data.get("DatabaseConnected", True) is True

    def test_workflow_health_unhealthy_db(self, client_with_workflow, mock_domain_db_pool, mock_queue_client):
        """GET /api/workflow/health returns 503 when DB is unhealthy."""
        mock_domain_db_pool.health_check = AsyncMock(return_value=False)

        response = client_with_workflow.get(f"{API_BASE}/workflow/health")

        assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE


@pytest.mark.skipif(not MULTIPART_AVAILABLE, reason="python-multipart required for workflow app")
class TestWorkflowSharepackUploadEndpoint:
    """Tests for POST /api/workflow/sharepack/upload_and_validate when workflow is enabled."""

    def test_upload_and_validate_success(
        self,
        client_with_workflow,
    ):
        """POST with valid YAML returns 202 Accepted."""
        with (
            patch("dbrx_api.workflow.parsers.parser_factory.parse_sharepack_file") as mock_parse,
            patch(
                "dbrx_api.workflow.validators.strategy_detector.detect_optimal_strategy", new_callable=AsyncMock
            ) as mock_detect,
            patch("dbrx_api.workflow.db.repository_share_pack.SharePackRepository") as mock_repo_class,
        ):
            mock_parse.return_value = MagicMock(
                metadata=MagicMock(
                    strategy="full",
                    requestor="test@example.com",
                    business_line="test",
                ),
                recipient=[],
                share=[],
                dict=MagicMock(
                    return_value={
                        "metadata": {"strategy": "full", "requestor": "test@example.com", "business_line": "test"},
                        "recipient": [],
                        "share": [],
                    }
                ),
            )
            mock_detect.return_value = MagicMock(
                strategy_changed=False, get_summary=MagicMock(return_value=""), warnings=[]
            )
            mock_repo = MagicMock()
            mock_repo.get_by_name = AsyncMock(return_value=None)
            mock_repo.create_from_config = AsyncMock(return_value=None)
            mock_repo_class.return_value = mock_repo

            response = client_with_workflow.post(
                f"{API_BASE}/workflow/sharepack/upload_and_validate",
                files={
                    "file": (
                        "pack.yaml",
                        BytesIO(b"metadata:\n  strategy: full\nrecipient: []\nshare: []"),
                        "application/yaml",
                    )
                },
            )

            assert response.status_code == status.HTTP_202_ACCEPTED
            data = response.json()
            assert "SharePackId" in data or "Status" in data

    def test_upload_and_validate_invalid_file(self, client_with_workflow):
        """POST with invalid file returns 400."""
        with patch("dbrx_api.workflow.parsers.parser_factory.parse_sharepack_file") as mock_parse:
            mock_parse.side_effect = ValueError("Invalid YAML")

            response = client_with_workflow.post(
                f"{API_BASE}/workflow/sharepack/upload_and_validate",
                files={"file": ("bad.yaml", BytesIO(b"not: valid: yaml: ["), "application/yaml")},
            )

            assert response.status_code == status.HTTP_400_BAD_REQUEST
            assert "Invalid" in response.json()["detail"]


@pytest.mark.skipif(not MULTIPART_AVAILABLE, reason="python-multipart required for workflow app")
class TestWorkflowSharepackStatusEndpoint:
    """Tests for GET /api/workflow/sharepack/{share_pack_id} when workflow is enabled."""

    def test_get_sharepack_status_not_found(self, client_with_workflow):
        """GET sharepack status for non-existent id returns 404."""
        with patch("dbrx_api.workflow.db.repository_share_pack.SharePackRepository") as mock_repo_class:
            mock_repo = MagicMock()
            mock_repo.get_current = AsyncMock(return_value=None)
            mock_repo_class.return_value = mock_repo

            response = client_with_workflow.get(f"{API_BASE}/workflow/sharepack/00000000-0000-0000-0000-000000000001")

            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_get_sharepack_status_success(self, client_with_workflow):
        """GET sharepack status returns 200 with status when found."""
        with patch("dbrx_api.workflow.db.repository_share_pack.SharePackRepository") as mock_repo_class:
            mock_repo = MagicMock()
            mock_repo.get_current = AsyncMock(
                return_value={
                    "share_pack_id": "00000000-0000-0000-0000-000000000001",
                    "share_pack_name": "test-pack",
                    "share_pack_status": "COMPLETED",
                    "strategy": "full",
                    "provisioning_status": "done",
                    "error_message": "",
                    "requested_by": "test@example.com",
                    "effective_from": "2024-01-01T00:00:00Z",
                }
            )
            mock_repo_class.return_value = mock_repo

            response = client_with_workflow.get(f"{API_BASE}/workflow/sharepack/00000000-0000-0000-0000-000000000001")

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data.get("Status") == "COMPLETED" or data.get("SharePackId") is not None
