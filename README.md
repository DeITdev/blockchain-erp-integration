## Install HRMS into ERPNext

### Step 1: List Running Containers

Run this command to find the frontend and backend containers:

```bash
docker ps
```

Look for containers with names like `erpnext-backend-1` or `erpnext-frontend-1`.

### Step 2: Get HRMS App

Go into each container (frontend and backend) and download the HRMS app:

```bash
# Enter the container (replace {container_name} with actual name)
docker exec -it {container_name} bash

# Download HRMS app
bench get-app hrms
```

**Do this for BOTH frontend and backend containers.**

### Step 3: Install HRMS

After getting the app, install it to your site. Run this in each container:

```bash
# Install HRMS to the site
bench --site frontend install-app hrms

# If you get duplicate entry error, use --force
bench --site frontend install-app hrms --force
```

**Do this on BOTH frontend and backend containers after the get-app command.**

### Step 4: Run Migration

After installation, run migration:

```bash
bench migrate
```

### Step 5: Verify Installation

Check if HRMS is installed:

```bash
# List all installed apps
bench --site frontend list-apps
```

You should see `hrms` in the list.

**DONE!** HRMS is now installed in your ERPNext instance.

---

## Useful Verification Commands

```bash
# Check installed apps
bench --site frontend list-apps

# Check site status
bench --site frontend status

# Clear cache after installation
bench --site frontend clear-cache
```
