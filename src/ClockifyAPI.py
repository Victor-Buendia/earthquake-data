import requests
import os

CLOCKIFY_API_ENDPOINT = (
    "https://api.clockify.me/api/v1/workspaces/{workspaceId}/user/{userId}/time-entries"
)


class ClockifyInteractor:
    def __init__(self, workspaceId, userId):
        self.workspaceId = workspaceId
        self.userId = userId
        self.session = requests.Session()
        self.session.headers.update(
            {"x-api-key": os.environ.get("CLOCKIFY_API_KEY", "")}
        )

    def get_time_entries(self):
        return self.session.get(
            CLOCKIFY_API_ENDPOINT.format(
                workspaceId=self.workspaceId, userId=self.userId
            )
        )


if __name__ == "__main__":
    from pprint import pprint
    from dotenv import load_dotenv
    import json

    load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"))

    api = ClockifyInteractor(
        workspaceId=os.environ["CLOCKIFY_WORKSPACE_ID"], userId=os.environ["CLOCKIFY_USER_ID"]
    )
    res = api.get_time_entries()
    print(os.environ.get("CLOCKIFY_API_KEY", ""))
    # pprint(res.text)
    print(json.dumps(json.loads(res.text), indent=2))
