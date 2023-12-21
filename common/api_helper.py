import requests
import logging

logger = logging.getLogger(__name__)

class API_Helper:
    """Helper class for making API requests."""

    def get(self, url, params=None, headers=None):
        """Make a GET request.

        Parameters:
        url (str): The URL for the API endpoint.
        params (dict, optional): A dictionary of URL parameters.
        headers (dict, optional): A dictionary of request headers.

        Returns:
        requests.Response: The server's response to the request.

        Example Usage:
        api_helper = API_Helper()
        response = api_helper.get('https://jsonplaceholder.typicode.com/posts')
        print(response.json())    # Prints the response body as JSON
        """
        response = requests.get(url, params=params, headers=headers)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            logger.error(f'GET request failed. Error: {err}')
            raise
        logger.info(f'GET request to {url} responded with status code {response.status_code}')
        return response

    def post(self, url, data=None, json=None, headers=None):
        """Make a POST request.

        Parameters:      
        url (str): The URL for the API endpoint.
        data (dict, optional): Dictionary, list of tuples, bytes, or file-like object to send in the body of the request.
                                Ideal for form-encoded data (like an HTML form).
        json (dict, optional): A JSON-serializable Python object to send in the body of the request.
        headers (dict, optional): A dictionary of request headers.

        Returns:
        requests.Response: The server's response to the request.

        Example Usage:
        api_helper = API_Helper()
        response = api_helper.post('https://jsonplaceholder.typicode.com/posts', json={'title': 'foo'})
        print(response.json())    # Prints the response body as JSON
        """
        response = requests.post(url, data=data, json=json, headers=headers)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            logger.error(f'POST request failed. Error: {err}')
            raise
        logger.info(f'POST request to {url} responded with status code {response.status_code}')
        return response


# # Instantiate API_Helper class
# api_helper = API_Helper()

# # Example for GET request
# # Let's fetch a list of posts from the JSONPlaceholder API
# get_url = "https://jsonplaceholder.typicode.com/posts"
# response = api_helper.get(get_url)
# print(response.json())  # prints the response JSON content

# # Example for POST request
# # Let's create a new post at JSONPlaceholder API
# post_url = "https://jsonplaceholder.typicode.com/posts"
# post_data = {
#     "title": "foo",
#     "body": "bar",
#     "userId": 1
# }
# response = api_helper.post(url=post_url, json=post_data)
# print(response.json())  # prints the response JSON content

