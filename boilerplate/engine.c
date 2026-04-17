/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Full implementation:
 *   - UNIX domain socket control-plane IPC
 *   - Container lifecycle with clone() + namespaces
 *   - Bounded-buffer logging pipeline (producer/consumer)
 *   - SIGCHLD / SIGINT / SIGTERM handling
 *   - ps, logs, start, run, stop commands
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <dirent.h>

#include "monitor_ioctl.h"

#define STACK_SIZE        (1024 * 1024)
#define CONTAINER_ID_LEN  32
#define CONTROL_PATH      "/tmp/mini_runtime.sock"
#define LOG_DIR           "logs"
#define CONTROL_MESSAGE_LEN 512
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE    4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)
#define PS_RESPONSE_MAX   (8192)

/* ---------------------------------------------------------------
 * Types
 * --------------------------------------------------------------- */

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
    /* For CMD_RUN: fd to send back exit status over */
    int run_notify_pipe[2]; /* supervisor fills [1], client reads [0] */
} control_request_t;

typedef struct {
    int status;
    int exit_code;       /* for CMD_RUN */
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

/* For tracking "run" waiters */
typedef struct run_waiter {
    char container_id[CONTAINER_ID_LEN];
    int  notify_fd;   /* write end — supervisor writes exit code here */
    struct run_waiter *next;
} run_waiter_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    volatile int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
    /* list of blocking "run" waiters */
    pthread_mutex_t waiter_lock;
    run_waiter_t *waiters;
} supervisor_ctx_t;

/* Global for signal handler to reach supervisor */
static supervisor_ctx_t *g_ctx = NULL;

/* ---------------------------------------------------------------
 * Usage
 * --------------------------------------------------------------- */

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run   <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

/* ---------------------------------------------------------------
 * Argument Parsing
 * --------------------------------------------------------------- */

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;
    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }
        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }
    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }
    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

/* ---------------------------------------------------------------
 * Bounded Buffer
 * --------------------------------------------------------------- */

static int bounded_buffer_init(bounded_buffer_t *buf)
{
    int rc;
    memset(buf, 0, sizeof(*buf));
    rc = pthread_mutex_init(&buf->mutex, NULL);
    if (rc != 0) return rc;
    rc = pthread_cond_init(&buf->not_empty, NULL);
    if (rc != 0) { pthread_mutex_destroy(&buf->mutex); return rc; }
    rc = pthread_cond_init(&buf->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buf->not_empty);
        pthread_mutex_destroy(&buf->mutex);
        return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buf)
{
    pthread_cond_destroy(&buf->not_full);
    pthread_cond_destroy(&buf->not_empty);
    pthread_mutex_destroy(&buf->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buf)
{
    pthread_mutex_lock(&buf->mutex);
    buf->shutting_down = 1;
    pthread_cond_broadcast(&buf->not_empty);
    pthread_cond_broadcast(&buf->not_full);
    pthread_mutex_unlock(&buf->mutex);
}

/*
 * Push a log item into the buffer.
 * Blocks when full; returns -1 if shutting down.
 */
int bounded_buffer_push(bounded_buffer_t *buf, const log_item_t *item)
{
    pthread_mutex_lock(&buf->mutex);
    while (buf->count == LOG_BUFFER_CAPACITY && !buf->shutting_down)
        pthread_cond_wait(&buf->not_full, &buf->mutex);

    if (buf->shutting_down) {
        pthread_mutex_unlock(&buf->mutex);
        return -1;
    }

    buf->items[buf->tail] = *item;
    buf->tail = (buf->tail + 1) % LOG_BUFFER_CAPACITY;
    buf->count++;

    pthread_cond_signal(&buf->not_empty);
    pthread_mutex_unlock(&buf->mutex);
    return 0;
}

/*
 * Pop a log item from the buffer.
 * Returns 0 on success, 1 if shutdown and empty (caller should drain then exit).
 */
int bounded_buffer_pop(bounded_buffer_t *buf, log_item_t *item)
{
    pthread_mutex_lock(&buf->mutex);
    while (buf->count == 0 && !buf->shutting_down)
        pthread_cond_wait(&buf->not_empty, &buf->mutex);

    if (buf->count == 0) {
        /* shutting_down and empty */
        pthread_mutex_unlock(&buf->mutex);
        return 1;
    }

    *item = buf->items[buf->head];
    buf->head = (buf->head + 1) % LOG_BUFFER_CAPACITY;
    buf->count--;

    pthread_cond_signal(&buf->not_full);
    pthread_mutex_unlock(&buf->mutex);
    return 0;
}

/* ---------------------------------------------------------------
 * Logging Thread
 * --------------------------------------------------------------- */

/*
 * Consumer thread: drains log items and writes them to per-container log files.
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;
    int rc;

    while (1) {
        rc = bounded_buffer_pop(&ctx->log_buffer, &item);
        if (rc != 0) {
            /* shutdown + empty: drain any leftovers then exit */
            break;
        }

        /* find the log path for this container */
        char log_path[PATH_MAX] = {0};
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = ctx->containers;
        while (c) {
            if (strncmp(c->id, item.container_id, CONTAINER_ID_LEN) == 0) {
                strncpy(log_path, c->log_path, PATH_MAX - 1);
                break;
            }
            c = c->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (log_path[0] == '\0') {
            /* container gone — build a fallback path */
            snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, item.container_id);
        }

        int fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd >= 0) {
            size_t written = 0;
            while (written < item.length) {
                ssize_t n = write(fd, item.data + written, item.length - written);
                if (n <= 0) break;
                written += (size_t)n;
            }
            close(fd);
        }
    }

    return NULL;
}

/* ---------------------------------------------------------------
 * Log Reader Thread: reads from a pipe and pushes to bounded buffer
 * --------------------------------------------------------------- */

typedef struct {
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *log_buffer;
} log_reader_arg_t;

static void *log_reader_thread(void *arg)
{
    log_reader_arg_t *lra = (log_reader_arg_t *)arg;
    log_item_t item;
    ssize_t n;

    memset(item.container_id, 0, CONTAINER_ID_LEN);
    strncpy(item.container_id, lra->container_id, CONTAINER_ID_LEN - 1);

    while (1) {
        n = read(lra->read_fd, item.data, LOG_CHUNK_SIZE);
        if (n <= 0) break;
        item.length = (size_t)n;
        bounded_buffer_push(lra->log_buffer, &item);
    }

    close(lra->read_fd);
    free(lra);
    return NULL;
}

/* ---------------------------------------------------------------
 * Container Child Function
 * --------------------------------------------------------------- */

/*
 * Runs inside the new namespace. Sets up /proc, chroots, exec's the command.
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* Redirect stdout and stderr to the log pipe */
    if (cfg->log_write_fd >= 0) {
        dup2(cfg->log_write_fd, STDOUT_FILENO);
        dup2(cfg->log_write_fd, STDERR_FILENO);
        close(cfg->log_write_fd);
    }

    /* Set UTS hostname to container id */
    if (sethostname(cfg->id, strlen(cfg->id)) != 0)
        perror("sethostname");

    /* Mount proc inside rootfs */
    char proc_path[PATH_MAX];
    snprintf(proc_path, sizeof(proc_path), "%s/proc", cfg->rootfs);
    mkdir(proc_path, 0755);

    if (mount("proc", proc_path, "proc", 0, NULL) != 0)
        perror("mount proc");

    /* chroot into the container's rootfs */
    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") != 0) {
        perror("chdir /");
        return 1;
    }

    /* Apply nice value */
    if (cfg->nice_value != 0) {
        errno = 0;
        if (nice(cfg->nice_value) == -1 && errno != 0)
            perror("nice");
    }

    /* Execute the command via shell */
    execl("/bin/sh", "/bin/sh", "-c", cfg->command, NULL);

    /* If exec fails, try direct execution */
    char *argv_exec[] = { cfg->command, NULL };
    execvp(cfg->command, argv_exec);

    perror("exec");
    return 127;
}

/* ---------------------------------------------------------------
 * Monitor ioctl helpers
 * --------------------------------------------------------------- */

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;
    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;
    return 0;
}

/* ---------------------------------------------------------------
 * Spawn a Container
 * --------------------------------------------------------------- */

static container_record_t *spawn_container(supervisor_ctx_t *ctx,
                                            const control_request_t *req)
{
    /* Check for duplicate ID */
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *existing = ctx->containers;
    while (existing) {
        if (strncmp(existing->id, req->container_id, CONTAINER_ID_LEN) == 0
            && (existing->state == CONTAINER_RUNNING
                || existing->state == CONTAINER_STARTING)) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            fprintf(stderr, "Container '%s' already running\n", req->container_id);
            return NULL;
        }
        existing = existing->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Allocate stack for clone */
    char *stack = malloc(STACK_SIZE);
    if (!stack) {
        perror("malloc stack");
        return NULL;
    }
    char *stack_top = stack + STACK_SIZE;

    /* Create log pipe */
    int log_pipe[2];
    if (pipe(log_pipe) < 0) {
        perror("pipe");
        free(stack);
        return NULL;
    }

    /* Set up child config */
    child_config_t *cfg = malloc(sizeof(child_config_t));
    if (!cfg) {
        close(log_pipe[0]); close(log_pipe[1]);
        free(stack);
        return NULL;
    }
    memset(cfg, 0, sizeof(*cfg));
    strncpy(cfg->id, req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(cfg->rootfs, req->rootfs, PATH_MAX - 1);
    strncpy(cfg->command, req->command, CHILD_COMMAND_LEN - 1);
    cfg->nice_value = req->nice_value;
    cfg->log_write_fd = log_pipe[1];

    /* Clone with namespace flags */
    int clone_flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;
    pid_t child_pid = clone(child_fn, stack_top, clone_flags, cfg);
    int clone_errno = errno;

    /* Close write end in parent */
    close(log_pipe[1]);
    free(stack);
    free(cfg);

    if (child_pid < 0) {
        errno = clone_errno;
        perror("clone");
        close(log_pipe[0]);
        return NULL;
    }

    /* Build container record */
    container_record_t *rec = calloc(1, sizeof(container_record_t));
    if (!rec) {
        close(log_pipe[0]);
        kill(child_pid, SIGKILL);
        waitpid(child_pid, NULL, 0);
        return NULL;
    }

    strncpy(rec->id, req->container_id, CONTAINER_ID_LEN - 1);
    rec->host_pid = child_pid;
    rec->started_at = time(NULL);
    rec->state = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->exit_code = -1;
    rec->exit_signal = 0;

    /* Create log directory and path */
    mkdir(LOG_DIR, 0755);
    snprintf(rec->log_path, PATH_MAX, "%s/%s.log", LOG_DIR, req->container_id);

    /* Register with kernel monitor */
    if (ctx->monitor_fd >= 0) {
        if (register_with_monitor(ctx->monitor_fd, rec->id,
                                   child_pid,
                                   rec->soft_limit_bytes,
                                   rec->hard_limit_bytes) < 0) {
            fprintf(stderr, "Warning: could not register with kernel monitor: %s\n",
                    strerror(errno));
        }
    }

    /* Start log reader thread for this container */
    log_reader_arg_t *lra = malloc(sizeof(log_reader_arg_t));
    if (lra) {
        lra->read_fd = log_pipe[0];
        strncpy(lra->container_id, req->container_id, CONTAINER_ID_LEN - 1);
        lra->log_buffer = &ctx->log_buffer;
        pthread_t reader_tid;
        if (pthread_create(&reader_tid, NULL, log_reader_thread, lra) != 0) {
            close(log_pipe[0]);
            free(lra);
        } else {
            pthread_detach(reader_tid);
        }
    } else {
        close(log_pipe[0]);
    }

    /* Insert into container list */
    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    fprintf(stdout, "[supervisor] Started container '%s' pid=%d\n", rec->id, child_pid);
    fflush(stdout);
    return rec;
}

/* ---------------------------------------------------------------
 * SIGCHLD Handler — reap children
 * --------------------------------------------------------------- */

static void sigchld_handler(int sig)
{
    (void)sig;
    if (!g_ctx) return;

    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&g_ctx->metadata_lock);
        container_record_t *c = g_ctx->containers;
        while (c) {
            if (c->host_pid == pid) {
                if (WIFEXITED(status)) {
                    c->state = CONTAINER_EXITED;
                    c->exit_code = WEXITSTATUS(status);
                } else if (WIFSIGNALED(status)) {
                    /* Distinguish SIGTERM-killed (stop) vs SIGKILL (hard limit or forced) */
                    int sig_num = WTERMSIG(status);
                    if (sig_num == SIGTERM || sig_num == SIGKILL) {
                        c->state = CONTAINER_KILLED;
                    } else {
                        c->state = CONTAINER_KILLED;
                    }
                    c->exit_signal = WTERMSIG(status);
                    c->exit_code = 128 + c->exit_signal;
                }
                /* Unregister from monitor */
                if (g_ctx->monitor_fd >= 0)
                    unregister_from_monitor(g_ctx->monitor_fd, c->id, pid);
                break;
            }
            c = c->next;
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);

        /* Notify any run-waiter */
        if (c) {
            pthread_mutex_lock(&g_ctx->waiter_lock);
            run_waiter_t *w = g_ctx->waiters;
            run_waiter_t *prev = NULL;
            while (w) {
                if (strncmp(w->container_id, c->id, CONTAINER_ID_LEN) == 0) {
                    int code = c->exit_code;
                    ssize_t wr = write(w->notify_fd, &code, sizeof(code));
                    (void)wr;
                    close(w->notify_fd);
                    if (prev) prev->next = w->next;
                    else g_ctx->waiters = w->next;
                    free(w);
                    break;
                }
                prev = w;
                w = w->next;
            }
            pthread_mutex_unlock(&g_ctx->waiter_lock);
        }
    }
}

static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx) g_ctx->should_stop = 1;
}

/* ---------------------------------------------------------------
 * Handle a control request from a client
 * --------------------------------------------------------------- */

static void handle_control_request(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t resp;

    memset(&resp, 0, sizeof(resp));

    ssize_t n = recv(client_fd, &req, sizeof(req), 0);
    if (n != (ssize_t)sizeof(req)) {
        resp.status = -1;
        snprintf(resp.message, CONTROL_MESSAGE_LEN, "Invalid request size");
        send(client_fd, &resp, sizeof(resp), 0);
        return;
    }

    switch (req.kind) {

    case CMD_START: {
        container_record_t *rec = spawn_container(ctx, &req);
        if (!rec) {
            resp.status = -1;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "Failed to start container '%s'", req.container_id);
        } else {
            resp.status = 0;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "Started container '%s' pid=%d", rec->id, rec->host_pid);
        }
        send(client_fd, &resp, sizeof(resp), 0);
        break;
    }

    case CMD_RUN: {
        /* Create a pipe so the supervisor can notify client when container exits */
        int notify_pipe[2];
        if (pipe(notify_pipe) < 0) {
            resp.status = -1;
            snprintf(resp.message, CONTROL_MESSAGE_LEN, "pipe() failed: %s", strerror(errno));
            send(client_fd, &resp, sizeof(resp), 0);
            break;
        }

        container_record_t *rec = spawn_container(ctx, &req);
        if (!rec) {
            close(notify_pipe[0]); close(notify_pipe[1]);
            resp.status = -1;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "Failed to start container '%s'", req.container_id);
            send(client_fd, &resp, sizeof(resp), 0);
            break;
        }

        /* Register waiter */
        run_waiter_t *w = calloc(1, sizeof(run_waiter_t));
        if (!w) {
            close(notify_pipe[0]); close(notify_pipe[1]);
            resp.status = -1;
            snprintf(resp.message, CONTROL_MESSAGE_LEN, "OOM");
            send(client_fd, &resp, sizeof(resp), 0);
            break;
        }
        strncpy(w->container_id, req.container_id, CONTAINER_ID_LEN - 1);
        w->notify_fd = notify_pipe[1];
        pthread_mutex_lock(&ctx->waiter_lock);
        w->next = ctx->waiters;
        ctx->waiters = w;
        pthread_mutex_unlock(&ctx->waiter_lock);

        /* Tell client container started */
        resp.status = 0;
        snprintf(resp.message, CONTROL_MESSAGE_LEN, "Running '%s' pid=%d", rec->id, rec->host_pid);
        send(client_fd, &resp, sizeof(resp), 0);

        /* Block until container exits */
        int exit_code = 0;
        ssize_t nr = read(notify_pipe[0], &exit_code, sizeof(exit_code));
        close(notify_pipe[0]);

        /* Send final response with exit code */
        memset(&resp, 0, sizeof(resp));
        resp.status = (nr == sizeof(exit_code)) ? 0 : -1;
        resp.exit_code = exit_code;
        snprintf(resp.message, CONTROL_MESSAGE_LEN,
                 "Container '%s' exited with code %d", req.container_id, exit_code);
        send(client_fd, &resp, sizeof(resp), 0);
        break;
    }

    case CMD_PS: {
        /* Build a text table of containers */
        char *buf = malloc(PS_RESPONSE_MAX);
        if (!buf) {
            resp.status = -1;
            snprintf(resp.message, CONTROL_MESSAGE_LEN, "OOM");
            send(client_fd, &resp, sizeof(resp), 0);
            break;
        }
        int off = 0;
        off += snprintf(buf + off, PS_RESPONSE_MAX - off,
                        "%-16s %-8s %-10s %-10s %-12s %-12s %s\n",
                        "ID", "PID", "STATE", "EXIT",
                        "SOFT(MiB)", "HARD(MiB)", "STARTED");

        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = ctx->containers;
        while (c && off < PS_RESPONSE_MAX - 128) {
            char tmbuf[32];
            struct tm tm_info;
            localtime_r(&c->started_at, &tm_info);
            strftime(tmbuf, sizeof(tmbuf), "%H:%M:%S", &tm_info);

            off += snprintf(buf + off, PS_RESPONSE_MAX - off,
                            "%-16s %-8d %-10s %-10d %-12lu %-12lu %s\n",
                            c->id,
                            c->host_pid,
                            state_to_string(c->state),
                            c->exit_code,
                            c->soft_limit_bytes >> 20,
                            c->hard_limit_bytes >> 20,
                            tmbuf);
            c = c->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        resp.status = 0;
        /* Send the table as the message (truncated if needed) */
        strncpy(resp.message, buf, CONTROL_MESSAGE_LEN - 1);
        free(buf);
        send(client_fd, &resp, sizeof(resp), 0);
        break;
    }

    case CMD_LOGS: {
        char log_path[PATH_MAX] = {0};
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = ctx->containers;
        while (c) {
            if (strncmp(c->id, req.container_id, CONTAINER_ID_LEN) == 0) {
                strncpy(log_path, c->log_path, PATH_MAX - 1);
                break;
            }
            c = c->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (log_path[0] == '\0') {
            resp.status = -1;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "Container '%s' not found", req.container_id);
            send(client_fd, &resp, sizeof(resp), 0);
            break;
        }

        resp.status = 0;
        snprintf(resp.message, CONTROL_MESSAGE_LEN, "%s", log_path);
        send(client_fd, &resp, sizeof(resp), 0);

        /* Stream the log file contents */
        int fd = open(log_path, O_RDONLY);
        if (fd >= 0) {
            char chunk[4096];
            ssize_t nr;
            while ((nr = read(fd, chunk, sizeof(chunk))) > 0)
                send(client_fd, chunk, (size_t)nr, 0);
            close(fd);
        }
        break;
    }

    case CMD_STOP: {
        pid_t pid = -1;
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = ctx->containers;
        while (c) {
            if (strncmp(c->id, req.container_id, CONTAINER_ID_LEN) == 0) {
                if (c->state == CONTAINER_RUNNING || c->state == CONTAINER_STARTING) {
                    pid = c->host_pid;
                    c->state = CONTAINER_STOPPED;
                }
                break;
            }
            c = c->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (pid < 0) {
            resp.status = -1;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "Container '%s' not found or not running", req.container_id);
        } else {
            /* Send SIGTERM first, then SIGKILL after 3s */
            kill(pid, SIGTERM);
            struct timespec ts = {3, 0};
            nanosleep(&ts, NULL);
            /* Check if still alive */
            if (kill(pid, 0) == 0)
                kill(pid, SIGKILL);
            resp.status = 0;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "Sent stop signal to container '%s' pid=%d", req.container_id, pid);
        }
        send(client_fd, &resp, sizeof(resp), 0);
        break;
    }

    default:
        resp.status = -1;
        snprintf(resp.message, CONTROL_MESSAGE_LEN, "Unknown command");
        send(client_fd, &resp, sizeof(resp), 0);
        break;
    }
}

/* ---------------------------------------------------------------
 * Supervisor Main Loop
 * --------------------------------------------------------------- */

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    ctx.should_stop = 0;
    g_ctx = &ctx;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) { errno = rc; perror("pthread_mutex_init"); return 1; }

    rc = pthread_mutex_init(&ctx.waiter_lock, NULL);
    if (rc != 0) { errno = rc; perror("pthread_mutex_init waiter"); return 1; }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) { errno = rc; perror("bounded_buffer_init"); return 1; }

    /* Create log directory */
    mkdir(LOG_DIR, 0755);

    /* Open kernel monitor device (optional — may not be loaded) */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        fprintf(stderr, "[supervisor] Warning: /dev/container_monitor not available (%s). "
                "Memory monitoring disabled.\n", strerror(errno));
    } else {
        fprintf(stderr, "[supervisor] Kernel memory monitor connected.\n");
    }

    /* Install signal handlers */
    struct sigaction sa_chld, sa_term;
    memset(&sa_chld, 0, sizeof(sa_chld));
    sa_chld.sa_handler = sigchld_handler;
    sa_chld.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa_chld, NULL);

    memset(&sa_term, 0, sizeof(sa_term));
    sa_term.sa_handler = sigterm_handler;
    sigaction(SIGINT, &sa_term, NULL);
    sigaction(SIGTERM, &sa_term, NULL);

    /* Start logging consumer thread */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        fprintf(stderr, "pthread_create logger: %s\n", strerror(rc));
        return 1;
    }

    /* Create UNIX domain socket */
    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }
    if (listen(ctx.server_fd, 16) < 0) {
        perror("listen");
        return 1;
    }

    /* Make the server socket non-blocking for graceful shutdown checks */
    int flags = fcntl(ctx.server_fd, F_GETFL, 0);
    fcntl(ctx.server_fd, F_SETFL, flags | O_NONBLOCK);

    fprintf(stdout, "[supervisor] Ready. Base rootfs: %s\n", rootfs);
    fprintf(stdout, "[supervisor] Control socket: %s\n", CONTROL_PATH);
    fflush(stdout);

    /* Main accept loop */
    while (!ctx.should_stop) {
        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                /* No pending connections — yield briefly */
                struct timespec ts = {0, 10 * 1000 * 1000}; /* 10ms */
                nanosleep(&ts, NULL);
                continue;
            }
            if (errno == EINTR) continue;
            if (!ctx.should_stop)
                perror("accept");
            break;
        }

        handle_control_request(&ctx, client_fd);
        close(client_fd);
    }

    fprintf(stdout, "[supervisor] Shutting down...\n");

    /* Kill all running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *c = ctx.containers;
    while (c) {
        if (c->state == CONTAINER_RUNNING || c->state == CONTAINER_STARTING) {
            kill(c->host_pid, SIGTERM);
        }
        c = c->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Wait briefly for containers to exit */
    sleep(2);

    /* Force kill any survivors */
    pthread_mutex_lock(&ctx.metadata_lock);
    c = ctx.containers;
    while (c) {
        if (c->state == CONTAINER_RUNNING || c->state == CONTAINER_STARTING) {
            kill(c->host_pid, SIGKILL);
            c->state = CONTAINER_KILLED;
        }
        c = c->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Reap all children */
    while (waitpid(-1, NULL, WNOHANG) > 0);

    /* Shutdown logging pipeline */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);

    /* Free metadata list */
    pthread_mutex_lock(&ctx.metadata_lock);
    c = ctx.containers;
    while (c) {
        container_record_t *next = c->next;
        free(c);
        c = next;
    }
    ctx.containers = NULL;
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Free waiter list */
    pthread_mutex_lock(&ctx.waiter_lock);
    run_waiter_t *w = ctx.waiters;
    while (w) {
        run_waiter_t *wn = w->next;
        close(w->notify_fd);
        free(w);
        w = wn;
    }
    ctx.waiters = NULL;
    pthread_mutex_unlock(&ctx.waiter_lock);

    close(ctx.server_fd);
    unlink(CONTROL_PATH);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    pthread_mutex_destroy(&ctx.waiter_lock);

    fprintf(stdout, "[supervisor] Clean shutdown complete.\n");
    return 0;
}

/* ---------------------------------------------------------------
 * Client-side: send a control request to supervisor
 * --------------------------------------------------------------- */

static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect: is the supervisor running?");
        close(fd);
        return 1;
    }

    if (send(fd, req, sizeof(*req), 0) != (ssize_t)sizeof(*req)) {
        perror("send");
        close(fd);
        return 1;
    }

    /* Receive first response */
    control_response_t resp;
    ssize_t n = recv(fd, &resp, sizeof(resp), 0);
    if (n <= 0) {
        fprintf(stderr, "No response from supervisor\n");
        close(fd);
        return 1;
    }

    /* For CMD_LOGS: also stream remaining data */
    if (req->kind == CMD_LOGS) {
        printf("%s\n", resp.message);  /* log file path */
        char chunk[4096];
        ssize_t nr;
        while ((nr = recv(fd, chunk, sizeof(chunk), 0)) > 0)
            fwrite(chunk, 1, (size_t)nr, stdout);
        fflush(stdout);
        close(fd);
        return resp.status == 0 ? 0 : 1;
    }

    printf("%s\n", resp.message);
    fflush(stdout);

    /* For CMD_RUN: block until second response (exit code) */
    if (req->kind == CMD_RUN && resp.status == 0) {
        control_response_t final_resp;
        n = recv(fd, &final_resp, sizeof(final_resp), MSG_WAITALL);
        if (n == (ssize_t)sizeof(final_resp)) {
            printf("%s\n", final_resp.message);
            close(fd);
            return final_resp.exit_code;
        }
    }

    close(fd);
    return resp.status == 0 ? 0 : 1;
}

/* ---------------------------------------------------------------
 * CLI Command Handlers
 * --------------------------------------------------------------- */

static int cmd_start(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

/* ---------------------------------------------------------------
 * main
 * --------------------------------------------------------------- */

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }
    if (strcmp(argv[1], "start") == 0)  return cmd_start(argc, argv);
    if (strcmp(argv[1], "run") == 0)    return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps") == 0)     return cmd_ps();
    if (strcmp(argv[1], "logs") == 0)   return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop") == 0)   return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
