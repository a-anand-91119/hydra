from __future__ import annotations

from hydra import gitlab


class TestDeleteProject:
    def test_already_marked_for_deletion_is_success(self, requests_mock):
        requests_mock.delete(
            "https://gitlab.com/api/v4/projects/83318931",
            status_code=400,
            json={"message": "Project has already been marked for deletion"},
        )

        gitlab.delete_project(
            host="gitlab",
            base_url="https://gitlab.com",
            token="tok",
            project_id=83318931,
        )
