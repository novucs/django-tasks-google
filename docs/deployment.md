# Deploying to Google Cloud

How to create the Google Cloud resources the `CloudTasksBackend` and `CloudRunJobsBackend`
need. Local development needs none of this; see [Local development](local-development.md).

[Back to README](../README.md)

> The IAM grants below are scoped to exactly the permissions this library calls. Review the
> names, regions, and your organization's policies before running them in production.

## Set up

```bash
PROJECT_ID=your-project-id
REGION=us-central1
SERVICE=your-app
IMAGE=us-central1-docker.pkg.dev/$PROJECT_ID/your-repo/your-app:latest
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')

gcloud services enable \
    run.googleapis.com cloudtasks.googleapis.com cloudscheduler.googleapis.com \
    --project="$PROJECT_ID"
```

## Database

The library coordinates work through your Django database, so it needs:

- **One shared database** for the web service, every Cloud Run Job, and the local worker.
  They hand tasks off through `TaskExecution` rows (lease, heartbeat, status, results).
- **Row-level locking** (`SELECT ... FOR UPDATE`). Use any Django backend that supports it:
  PostgreSQL, MySQL, and Cloud Spanner (via `django-google-spanner`) all do, as do their
  managed forms (Cloud SQL, AlloyDB). SQLite does not, so it is unsafe beyond single-process
  local use.
- **Connectivity from both runtimes.** The service and the job are separate, so configure
  each. For Cloud SQL, use `--add-cloudsql-instances` and grant `roles/cloudsql.client` to
  both service accounts.

This guide assumes the database already exists and your Django `DATABASES` setting points at
it. For Cloud SQL, create the instance and connect over its unix socket
(`/cloudsql/INSTANCE_CONNECTION_NAME`); see
[Connect Cloud Run to Cloud SQL](https://cloud.google.com/sql/docs/postgres/connect-run).

## Deploy the app

Build and push your Django app as a container image (standard Cloud Run; see
[Deploying to Cloud Run](https://cloud.google.com/run/docs/deploying)). `IMAGE` above points at
it. Then deploy:

```bash
gcloud run deploy "$SERVICE" \
    --image="$IMAGE" \
    --add-cloudsql-instances="$PROJECT_ID:$REGION:your-instance" \
    --region="$REGION" --project="$PROJECT_ID"
```

Your `base_url` is the printed URL plus your mounted path, e.g.
`https://your-app-xxxx.run.app/tasks/`. The service runs as `RUNTIME_SA`, a service account
email. Unless you pass `--service-account`, it is the Compute Engine default:

```bash
RUNTIME_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
# or a dedicated account: your-app@${PROJECT_ID}.iam.gserviceaccount.com
```

## Authentication

Google Cloud calls back into your app as an OIDC service account. Create it, let it invoke the
service, and let your app and Cloud Tasks act as it:

```bash
gcloud iam service-accounts create task-invoker --project="$PROJECT_ID"
INVOKER="task-invoker@${PROJECT_ID}.iam.gserviceaccount.com"

# Let the invoker account call your service.
gcloud run services add-iam-policy-binding "$SERVICE" \
    --member="serviceAccount:${INVOKER}" --role="roles/run.invoker" \
    --region="$REGION" --project="$PROJECT_ID"

# Let your app attach an OIDC token for the invoker account when it enqueues or schedules.
gcloud iam service-accounts add-iam-policy-binding "$INVOKER" \
    --member="serviceAccount:${RUNTIME_SA}" --role="roles/iam.serviceAccountUser" \
    --project="$PROJECT_ID"

# Let Cloud Tasks mint that token at delivery time.
gcloud iam service-accounts add-iam-policy-binding "$INVOKER" \
    --member="serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-cloudtasks.iam.gserviceaccount.com" \
    --role="roles/iam.serviceAccountUser" --project="$PROJECT_ID"
```

Set `oidc_service_account` to `task-invoker@...`. Cloud Scheduler's service agent gets this
automatically. If the Cloud Tasks agent does not exist yet, create it with
`gcloud beta services identity create --service=cloudtasks.googleapis.com --project="$PROJECT_ID"`.

## Permissions

`RUNTIME_SA` needs these permissions, granted however fits your setup:

```
cloudtasks.tasks.create        # enqueue a task
cloudtasks.queues.get          # read the queue's max attempts
run.jobs.run                   # start a job execution
run.jobs.get                   # read the job's max retries
run.executions.cancel          # forcefully cancel an execution
cloudscheduler.jobs.create     # create, sync, delete, pause, and resume scheduled tasks
cloudscheduler.jobs.update
cloudscheduler.jobs.get
cloudscheduler.jobs.delete
cloudscheduler.jobs.pause
cloudscheduler.jobs.enable
```

You only need the `cloudscheduler.*` permissions if you schedule tasks, and the `run.*`
permissions if you use the Cloud Run Jobs backend. A custom role bundling them keeps things
least-privilege:

```bash
gcloud iam roles create djangoTasksGoogle --project="$PROJECT_ID" \
    --title="django-tasks-google" \
    --permissions=cloudtasks.tasks.create,cloudtasks.queues.get,run.jobs.run,run.jobs.get,run.executions.cancel,cloudscheduler.jobs.create,cloudscheduler.jobs.update,cloudscheduler.jobs.get,cloudscheduler.jobs.delete,cloudscheduler.jobs.pause,cloudscheduler.jobs.enable

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${RUNTIME_SA}" \
    --role="projects/${PROJECT_ID}/roles/djangoTasksGoogle"
```

## Create the queue and job

Create one Cloud Tasks queue per `CloudTasksBackend` queue, and one Cloud Run Job per
`CloudRunJobsBackend` queue. Name each resource the value your `queue_aliases` points at (or
the `QUEUES` name itself if you set no alias).

```bash
# Cloud Tasks queue. Retries come from --max-attempts (default 100).
gcloud tasks queues create your-cloud-task-queue-name \
    --location="$REGION" --project="$PROJECT_ID"
```

The Cloud Run Job uses the same image and runs `execute_task`. Retries come from
`--max-retries` (default 3):

```bash
JOB_SA="task-runner@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud run jobs create your-cloud-run-job-name \
    --image="$IMAGE" --command="python" --args="manage.py,execute_task" \
    --service-account="$JOB_SA" \
    --add-cloudsql-instances="$PROJECT_ID:$REGION:your-instance" \
    --region="$REGION" --project="$PROJECT_ID"
```

The job runs your task code directly as `JOB_SA`, so `JOB_SA` needs database access, plus the
`djangoTasksGoogle` role only if your tasks themselves enqueue, schedule, or cancel. Creating
the job under `JOB_SA` requires your deploy account to have `roles/iam.serviceAccountUser` on
it. You can reuse `RUNTIME_SA` as `JOB_SA` if it has database access.

## Scheduling

Nothing to set up. `schedule_task(...)` creates and syncs the Cloud Scheduler job; the
permissions and grants above already cover it. See [Scheduling](scheduling.md).

## Check it works

```python
result = send_notification.enqueue(user_id=1)
result.refresh()
print(result.status)
```

If it fails, check: the queue or job exists and matches `queue_aliases`; the IAM grants above
(the custom role and both `actAs` grants); the database is reachable from the job and supports
row locking; and `base_url` matches your mounted path.
