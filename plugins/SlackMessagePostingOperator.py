from airflow.models import BaseOperator, Variable
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


class SlackMessagePostingOperator(BaseOperator):
    def __init__(
            self,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def execute(self, context):
        client = WebClient(token=(Variable.get_variable_from_secrets("slack_token")))

        try:
            response = client.chat_postMessage(
                channel="an-variety-of-cutting-edge-frameworks",
                text="Alert from just launched app! :tada:")
            print("message " + response.__str__() + " was sent on a slack channel successfully")

        except SlackApiError as e:
            print("SlackAPI exception occured")
            assert e.response["error"]
        return "success"
