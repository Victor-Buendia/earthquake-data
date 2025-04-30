import requests
import os
import logging

CLOCKIFY_API_ENDPOINT = (
    "https://api.clockify.me/api/v1/workspaces/{workspaceId}/user/{userId}/time-entries"
)

logger = logging.getLogger(__name__)


class ClockifyInteractor:
    def __init__(self, workspaceId, userId):
        self.workspaceId = workspaceId
        self.userId = userId

        self.session = requests.Session()
        self.session.headers.update(
            {"x-api-key": os.environ.get("CLOCKIFY_API_KEY", "")}
        )

    def get_time_entries(self, start=None, end=None, page_size=1_000):
        url = CLOCKIFY_API_ENDPOINT.format(
            workspaceId=self.workspaceId, userId=self.userId
        )
        params = {}

        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if page_size:
            params["page-size"] = page_size

        logger.info("Fetching time entries from %s", url)
        logger.info("Params: %s", params)

        return self.session.get(url, params=params)


if __name__ == "__main__":
    from dotenv import load_dotenv
    import json

    load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"))

    api = ClockifyInteractor(
        workspaceId=os.environ["CLOCKIFY_WORKSPACE_ID"],
        userId=os.environ["CLOCKIFY_USER_ID"],
    )
    res = api.get_time_entries(start="2020-01-01T00:00:00Z")
    print(json.dumps(json.loads(res.text), indent=2))
