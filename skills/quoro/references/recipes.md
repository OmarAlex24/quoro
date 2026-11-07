# Recipes

Each recipe assumes you ran `quoro whoami` and that your grants cover the calls. The examples name the project and the app on every command with `--project` and `--app`; in a folder linked with `quoro link PROJECT APP` you can leave them out. Replace the example names, images and ids with real ones, and write your own `--reason`.

## Deploy an image

Grants: `app:create` for a new app, `app:deploy` to deploy, `app:read` and `app:logs` to check.

1. Create the app if it does not exist. Creating it records it; nothing runs yet.

   ```sh
   quoro app create web --image ghcr.io/acme/web:1.4.2 --port 8080 --project shop --reason "Add the web front end for the shop"
   ```

2. Deploy the image. The command prints the deployment's id (`dep_...`) and returns.

   ```sh
   quoro deploy --image ghcr.io/acme/web:1.4.2 --project shop --app web --reason "First rollout of web 1.4.2"
   ```

3. Follow the rollout until the deployment is finished, then check the app.

   ```sh
   quoro logs --deployment dep_01JC9Z6Q5W8N3K4T7R2M1XHB0D --project shop --app web
   quoro status --project shop --app web
   ```

A later version is the same `quoro deploy --image` with the new tag. Prefer an immutable tag or a digest (`ghcr.io/acme/web@sha256:...`).

## Deploy from Git and roll back

Grants: `app:create`, `app:deploy`, `app:read`, `app:logs`.

1. Create the app from a public `https://` repository. It builds the repository's Dockerfile.

   ```sh
   quoro app create api --repo https://github.com/acme/api --ref main --dockerfile Dockerfile --port 8080 --project shop --reason "Add the API, built from its repository"
   ```

2. Build and deploy. `quoro up` follows the build log and exits `0` when the build succeeded, which deploys it, and `1` when it failed or was cancelled. `--ref` builds another branch, tag or commit; `--detach` returns at once.

   ```sh
   quoro up --project shop --app api --reason "Deploy the fix for the login timeout"
   ```

3. Roll back to an earlier deployment. `quoro status` lists the last five deployments with their ids. A Git app rebuilds the commit that deployment ran; an image app redeploys its image.

   ```sh
   quoro status --project shop --app api
   quoro deploy --rollback dep_01JC9Z6Q5W8N3K4T7R2M1XHB0D --project shop --app api --reason "Roll back: 5xx since the last deploy"
   ```

A Git app is never given an image: `quoro deploy --image` answers `conflict` (exit `6`).

## Debug a failed deploy

Grants: `app:read`, `app:logs`; `vm:read` and `vm:console` for the console. Read in this order and stop when you find the cause.

1. The app, its instances and its last deployments:

   ```sh
   quoro status --project shop --app api
   ```

2. The failed deployment's progress lines (image pull, start, health check):

   ```sh
   quoro logs --deployment dep_01JC9Z6Q5W8N3K4T7R2M1XHB0D --project shop --app api
   ```

3. For a Git app, the build's log (`--build` alone is the app's newest build; `quoro build list` shows the others):

   ```sh
   quoro logs --build --project shop --app api
   quoro build list --project shop --app api
   ```

4. The app's own output, the last lines or since a time:

   ```sh
   quoro logs -n 500 --project shop --app api
   quoro logs --since 15m --project shop --app api
   ```

5. When the app writes nothing (a crash at boot), the instance's serial console. Find the instance's VM in `quoro vm list`, then follow its console, read-only, until you stop it:

   ```sh
   quoro vm list --project shop
   quoro vm console vm_01JC9Z6Q5W8N3K4T7R2M1XHB0D --reason "Read the boot output of the failing api instance"
   ```

Common causes: the app listens on another port than its `--port`; the health check path answers an error (`quoro app update --health-path /healthz`); a variable is missing (`quoro env list` shows the names, never the values); the instance runs out of memory (`quoro app update --memory 1024`). Fix the cause, then deploy again.

## Act on an incident

An incident wakes you with incident grants that last while it is open. Pass `--incident` on every call for it.

1. Read the incident, the event that opened it and the grants it gave you:

   ```sh
   quoro incident list --status open
   quoro incident show inc_01JC9Z6Q5W8N3K4T7R2M1XHB0D
   ```

2. Diagnose with the reads of the debug recipe above.

3. Act within your grants, naming the incident:

   ```sh
   quoro restart --project shop --app api --incident inc_01JC9Z6Q5W8N3K4T7R2M1XHB0D --reason "Restart after the memory leak alert; instances at 98% memory"
   quoro redeploy --project shop --app api --incident inc_01JC9Z6Q5W8N3K4T7R2M1XHB0D --reason "Roll out the current image again after a node failure"
   ```

4. Check the result, then resolve the incident with what was done:

   ```sh
   quoro incident resolve inc_01JC9Z6Q5W8N3K4T7R2M1XHB0D --note "Restarted api; memory back to 40%" --reason "The app is healthy again"
   ```

Over MCP: `get_incident`, then `restart_app` or `redeploy_app` with `incident_id` and `justification`, then `resolve_incident`. Resolving ends the incident's grants. If the fix needs a destructive call, use the approval loop; an incident grant never holds a destructive scope.

## Create and operate a VM

Grants: `vm:create`, `vm:read`, `vm:start`, `vm:stop`, `vm:console`; `vm:delete` only through an approval.

```sh
quoro status --cluster
quoro vm create builder --image ghcr.io/acme/builder:2 --node node-a --vcpus 2 --memory 2048 --disk 10 --project shop --reason "A build VM for the nightly jobs"
quoro vm show builder --project shop
quoro vm stop builder --project shop --reason "Stop the build VM outside the nightly window"
quoro vm start builder --project shop --reason "Start the build VM for the nightly jobs"
quoro vm console builder --project shop --reason "Check the build VM's boot output"
quoro vm delete builder --project shop --reason "The nightly jobs moved to the api app" --request-approval --wait
```

`quoro status --cluster` lists the nodes to place it on. A name two VMs share is refused; use the id (`vm_...`).

## Scale an app

Grants: `app:update`, `app:read`.

```sh
quoro app update --replicas 3 --project shop --app web --reason "Three instances for the launch traffic"
quoro status --project shop --app web
```

A new replica count adds or removes instances without a deployment. An app runs at most one instance per node, so `--replicas` above the eligible nodes is refused (`validation_failed`), and more than one instance needs a machine with the edge role. An app with a Git source or a volume runs one instance (`conflict`, exit `6`). `--vcpus` and `--memory` size each instance; read `quoro status` after the change.
