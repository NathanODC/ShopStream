import google.auth.transport.requests
import google.oauth2.id_token
import requests
import logging

# def call_cloud_function(url: str, body=None):

#         auth_req = google.auth.transport.requests.Request()
#         audience = url
#         id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience)

#         headers = {
#             'Authorization': f'Bearer {id_token}',
#             'Content-Type': 'application/json'
#         }

#         response = requests.post(url, headers=headers, json=body, timeout=3600)
#         print(f"Called {url} - Status Code:", response.status_code)
#         try:
#             print("Response:", response.json())
#             return response.json()
#         except ValueError:
#             print("Response text:", response.text)
#             raise Exception(response.text)

def call_cloud_function(url: str, body=None):
    auth_req = google.auth.transport.requests.Request()
    audience = url
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience)

    headers = {
        'Authorization': f'Bearer {id_token}',
        'Content-Type': 'application/json'
    }

    if body is not None:
        response = requests.post(url, headers=headers, json=body, timeout=3600)
    else:
        response = requests.post(url, headers=headers, timeout=3600)

    print(f"Called {url} - Status Code:", response.status_code)
    try:
        print("Response:", response.json())
        return response.json()
    except ValueError:
        print("Response text:", response.text)
        raise Exception(response.text)

def log_on_dag_failure(context):
    """collect DAG information and send to console.log on failure."""
    
    dag = context.get('dag')

    log_msg = f"""
        Airflow DAG Failure:
            *DAG*: {dag.dag_id}
            *DAG Description*: {dag.description}
            *DAG Tags*: {dag.tags}
            *Context*: {context}
    """

    logging.info(log_msg)

def log_on_task_failure(context):
    """collect task information and send to console.log on failure."""
    
    ti = context.get('task_instance')
    log_msg = f"""
        Airflow Task Failure:
            *DAG*: {ti.dag_id}
            *DAG Run*: {ti.run_id}
            *Task*: {ti.task_id}
            *state*: {ti.state}
            *operator*: {ti.operator}
    """

    logging.info(log_msg)

def log_on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    """collect DAG information and send to console.log on sla miss."""
    
    log_msg = f"""
        Airflow DAG SLA Miss:
            *DAG*: {dag.dag_id}
            *DAG Description*: {dag.description}
            *DAG Tags*: {dag.tags}
            *Task List*: {task_list}
            *Blocking*: {blocking_task_list}
            *slas*: {slas}
            *blocking task ids*: {blocking_tis}
        """

    logging.info(log_msg)

def dag_success_alert(context):
    """collect DAG information and send to console.log on success."""
    
    dag = context.get('dag')

    log_msg = f"""
        Airflow DAG Success: 
            *DAG*: {dag.dag_id} 
            *DAG Description*: {dag.description} 
            *DAG Tags*: {dag.tags} 
            *Context*: {context}
    """

    logging.info(log_msg)

def ms_teams_alert(context, status_message):
    """collect DAG information and send to console.log on success."""

    dag = context.get('dag')
    dag_run = context.get('dag_run')

    if status_message == "Succeded":
        image_url = "https://freepngimg.com/save/18343-success-png-image/800x800"
        status_color = "Good"

    elif status_message == "Failed":
        image_url = "https://icon-library.com/images/error-icon-png/error-icon-png-21.jpg"
        status_color = "Attention"

    ms_teams_template = (
        {
            "type": "message",
            "attachments": [
                {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                    {
                        "type": "TextBlock",
                        "text": "Request sent by: Composer Callback",
                        "wrap": True,
                        "size": "Medium",
                        "weight": "Bolder"
                    },
                    {
                        "type": "Image",
                        "url": image_url,
                        "size": "Large",
                        "horizontalAlignment": "Center"
                    },
                    {
                        "type": "TextBlock",
                        "text": f"Airflow DAG {status_message}:",
                        "wrap": True,
                        "weight": "Bolder",
                        "spacing": "Medium",
                        "color": status_color
                    },
                    {
                        "type": "TextBlock",
                        "text": f"**DAG**: {dag.dag_id}",
                        "wrap": True
                    },
                    {
                        "type": "TextBlock",
                        "text": f"**DAG Description**: {dag.description}",
                        "wrap": True
                    },
                    {
                        "type": "TextBlock",
                        "text": f"**DAG Tags**: {dag.tags}",
                        "wrap": True
                    },
                    {
                        "type": "TextBlock",
                        "text": f"**DAG Run ID**: {dag_run.run_id}",
                        "wrap": True
                    },
                    {
                        "type": "TextBlock",
                        "text": f"**Status**: {dag_run.state}",
                        "wrap": True,
                        "weight": "Bolder"
                    },
                    {
                        "type": "TextBlock",
                        "text": f"**Start Time**: {dag_run.start_date}",
                        "wrap": True
                    },
                    {
                        "type": "TextBlock",
                        "text": f"**End Time**: {dag_run.end_date}",
                        "wrap": True
                    }
                    ]
                }
                }
            ]
        }
    )

    webhook_url = "https://prod-171.westus.logic.azure.com:443/workflows/d66d89625bd94c60b57a2f2c385ff367/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=xj0aV-l-ln-Dmle4C8GitvcNkeATTtv5IQrYbCikTok"

    response = requests.post(webhook_url, json=ms_teams_template)
    logging.info(response.json)
    
    
        
    
