from airflow.policies import hookimpl
from airflow.exceptions import AirflowClusterPolicyViolation

from airflow.models.taskinstance import TaskInstance

def check_api_response(url):
    """
    Get your Astro API token and replace your-api-token with the actual token in line #17 to access the Astro API
    """
    import requests
    import sys
    headers = {
        "Authorization": "Bearer your-api-token"  ##TODO
    }

    try:
        response = requests.get(url, headers=headers)
    except:
        print(response.status_code)
        print(response.json())
        sys.exit(1)

    return response   


def check_using_user_role(user_id):

    """
    My version of this method might be different from the checks you want to place. 
    In this version I am checking if a user has a deployment role called `least_privilege_test`, he/she is not allowed to run the DAG. 
    This is a custom role that I created just for testing. You can create your own role and and change this name or add your logic.

    Also, change the following:
    1. In line #42, replace your-organization-id with your Astro Organization ID
    2. In line #43, replace your-deployment-id with your Astro Deployment ID
    3. In line #59, change the role name as per your testing
    """
    users_endpoint =  'https://api.astronomer.io/iam/v1beta1/organizations/your-organization-id/users/{user_id}' ##TODO
    deployment_id = "your-deployment-id" ##TODO

    ## make the API call to get the roles for the user_id
    response = check_api_response(users_endpoint.format(user_id=user_id))

    ## if any of the roles assigned to the user match the role that is not allowed to perform actions.
    ## this method will return False, which means, stop the execution
    if response.json():
        deployment_roles = response.json()['deploymentRoles']
        print(deployment_roles)
        for depl_role in deployment_roles:
            id = depl_role['deploymentId']
            role = depl_role['role']
            print(id, role)
            if id == deployment_id:
                print("found my deployment")
                if role == 'least_privilege_test':
                    return False ## don't continue
            else:
                continue
        
    return True  ## continue the DAG execution

def check_user(dag_id, run_id, session=None):
    """
    This method uses Airflow's session object to check:
    - if the DAG is manually triggered, get the user_id
      - `trigger` event is generated when a DAG is manually created
    - if the DAG was scheduled run but is being cleared, get the user_id
      - `dagrun_clear` event is generated when a DAG is cleared
      - `clear` event is generated when a Task is cleared
    - if it is indeed a scheduled run, which is not cleared, but a regular run, the execution will continue as usual
    - This method returns
      - user_id, when an exception is found
      - None, if there is no exception
    """
    from airflow.settings import Session
    session = Session()
    print(dag_id, run_id)

    if run_id.startswith('manual'):
        print("manual run...checking user ", run_id)
        query = """
                select dttm, dag_id, run_id, execution_date, owner, event
                from log
                where dag_id = '{dag_id}' and event = 'trigger' 
                order by dttm desc
        """
        results = session.execute(query.format(dag_id=dag_id)).fetchall()
        print(results) 
        if results:
            ## get the most recent entry
            user_id = results[0][4]
            print(f"====> user id is {user_id}")
            return user_id
    else:
        print("scheduled DAG: ", run_id)
        query = """
                select dttm, dag_id, run_id, execution_date, owner, event
                from log
                where dag_id = '{dag_id}' and run_id = '{run_id}' and event in ('clear', 'dagrun_clear')
                order by dttm desc
        """
        results = session.execute(query.format(dag_id=dag_id, run_id=run_id)).fetchall()

        print(results) 
        if results:
            ## get the most recent entry
            user_id = results[0][4]
            print(f"====> user id is {user_id}")
            return user_id
        else:
            # if a new scheduled run
            print("Scheduled run, Run id new, continuing...")
            return None


@hookimpl
def task_instance_mutation_hook(task_instance: TaskInstance):
    """
    This method is the actual mutation hook, that will modify each and every task that runs in an Airflow Deployment where this plugin in installed.
    Currently there is no check, but if we want to apply this to specific TIs, we can do that.
    """
    dag_id = task_instance.dag_id
    run_id = task_instance.run_id
    print("===> in TI mutation hook", dag_id, run_id)
    if run_id is not None:
        user_id = check_user(dag_id, run_id)
        if user_id:
            print("Found the User ID that is executing this DAG. Checking it's permissions...")
            if not check_using_user_role(user_id):
                msg = "Ooops! You do not have the correct permission to run the DAG."
                print(msg)
                raise ValueError(msg) 
        else:
            print("regular scheduled run")
