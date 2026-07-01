# Gunicorn configuration settings.
import multiprocessing

# Don't start too many workers:
workers = min(multiprocessing.cpu_count() * 2 + 1, 4)

# Give workers an expiry:
max_requests = 2048
max_requests_jitter = 256
preload_app = True
timeout = 600

# Disable access logging.
accesslog = None

# The following settings allow the AKS container to work with a read-only root filesystem.

# Prevent Gunicorn from creating a control socket
control_socket_disable = True

# Use a temporary directory in shared memory for worker processes (instead of trying to write to /tmp)
worker_tmp_dir = "/dev/shm"
