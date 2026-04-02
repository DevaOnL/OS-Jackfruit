/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)
#define STOP_GRACE_SECONDS 3

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
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
    int exit_code;
    int exit_signal;
    int final_status;
    int stop_requested;
    int stop_escalated;
    time_t stop_requested_at;
    int monitor_registered;
    int producer_thread_started;
    pthread_t producer_thread;
    int wait_client_fd;
    char final_reason[32];
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
} control_request_t;

typedef struct {
    int status;
    int exit_status;
    size_t payload_length;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

struct supervisor_ctx;

typedef struct {
    struct supervisor_ctx *ctx;
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
} producer_arg_t;

typedef struct supervisor_ctx {
    int server_fd;
    int monitor_fd;
    int should_stop;
    int logger_thread_started;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
    char base_rootfs[PATH_MAX];
} supervisor_ctx_t;

enum read_result {
    READ_RESULT_OK = 0,
    READ_RESULT_INTERRUPTED,
    READ_RESULT_CLOSED,
    READ_RESULT_ERROR
};

static int g_signal_pipe[2] = {-1, -1};
static volatile sig_atomic_t g_sigchld_seen = 0;
static volatile sig_atomic_t g_shutdown_seen = 0;
static volatile sig_atomic_t g_client_forward_stop = 0;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

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
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int is_active_state(container_state_t state)
{
    return state == CONTAINER_STARTING || state == CONTAINER_RUNNING;
}

static void copy_cstr(char *dest, size_t dest_size, const char *src)
{
    size_t length;

    if (dest_size == 0)
        return;

    length = strlen(src);
    if (length >= dest_size)
        length = dest_size - 1;

    memcpy(dest, src, length);
    dest[length] = '\0';
}

static void format_time_value(time_t when, char *buffer, size_t buffer_size)
{
    struct tm tm_value;

    if (when == 0) {
        snprintf(buffer, buffer_size, "-");
        return;
    }

    if (localtime_r(&when, &tm_value) == NULL) {
        snprintf(buffer, buffer_size, "invalid");
        return;
    }

    strftime(buffer, buffer_size, "%Y-%m-%d %H:%M:%S", &tm_value);
}

static void supervisor_signal_handler(int signo)
{
    unsigned char marker = (unsigned char)signo;

    if (signo == SIGCHLD)
        g_sigchld_seen = 1;
    else
        g_shutdown_seen = 1;

    if (g_signal_pipe[1] >= 0)
        (void)write(g_signal_pipe[1], &marker, sizeof(marker));
}

static void client_signal_handler(int signo)
{
    (void)signo;
    g_client_forward_stop = 1;
}

static int set_fd_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);

    if (flags < 0)
        return -1;

    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
        return -1;

    return 0;
}

static void drain_signal_pipe(void)
{
    unsigned char buffer[64];

    if (g_signal_pipe[0] < 0)
        return;

    while (read(g_signal_pipe[0], buffer, sizeof(buffer)) > 0)
        ;
}

static int normalize_existing_path(const char *input, char *output, size_t output_size)
{
    char resolved[PATH_MAX];

    if (realpath(input, resolved) == NULL)
        return -1;

    if (strlen(resolved) >= output_size) {
        errno = ENAMETOOLONG;
        return -1;
    }

    strcpy(output, resolved);
    return 0;
}

static int ensure_log_dir(void)
{
    struct stat st;

    if (stat(LOG_DIR, &st) == 0) {
        if (!S_ISDIR(st.st_mode)) {
            errno = ENOTDIR;
            return -1;
        }
        return 0;
    }

    if (errno != ENOENT)
        return -1;

    if (mkdir(LOG_DIR, 0755) < 0)
        return -1;

    return 0;
}

static int build_log_path(const char *container_id, char *buffer, size_t buffer_size)
{
    int written = snprintf(buffer, buffer_size, "%s/%s.log", LOG_DIR, container_id);

    if (written < 0 || (size_t)written >= buffer_size) {
        errno = ENAMETOOLONG;
        return -1;
    }

    return 0;
}

static ssize_t write_full(int fd, const void *buffer, size_t count)
{
    const char *cursor = buffer;
    size_t remaining = count;

    while (remaining > 0) {
        ssize_t written = write(fd, cursor, remaining);

        if (written < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }

        cursor += written;
        remaining -= (size_t)written;
    }

    return (ssize_t)count;
}

static enum read_result read_full(int fd, void *buffer, size_t count, int interruptible)
{
    char *cursor = buffer;
    size_t remaining = count;

    while (remaining > 0) {
        ssize_t bytes_read = read(fd, cursor, remaining);

        if (bytes_read == 0)
            return READ_RESULT_CLOSED;

        if (bytes_read < 0) {
            if (errno == EINTR) {
                if (interruptible && remaining == count)
                    return READ_RESULT_INTERRUPTED;
                continue;
            }
            return READ_RESULT_ERROR;
        }

        cursor += bytes_read;
        remaining -= (size_t)bytes_read;
    }

    return READ_RESULT_OK;
}

static int send_response(int fd,
                         int status,
                         int exit_status,
                         const char *message,
                         const char *payload,
                         size_t payload_length)
{
    control_response_t response;

    memset(&response, 0, sizeof(response));
    response.status = status;
    response.exit_status = exit_status;
    response.payload_length = payload_length;
    if (message != NULL)
        copy_cstr(response.message, sizeof(response.message), message);

    if (write_full(fd, &response, sizeof(response)) < 0)
        return -1;

    if (payload_length > 0 && payload != NULL) {
        if (write_full(fd, payload, payload_length) < 0)
            return -1;
    }

    return 0;
}

static enum read_result receive_response(int fd,
                                         control_response_t *response,
                                         char **payload,
                                         int interruptible)
{
    enum read_result rc;

    *payload = NULL;

    rc = read_full(fd, response, sizeof(*response), interruptible);
    if (rc != READ_RESULT_OK)
        return rc;

    if (response->payload_length == 0)
        return READ_RESULT_OK;

    *payload = malloc(response->payload_length + 1);
    if (*payload == NULL)
        return READ_RESULT_ERROR;

    rc = read_full(fd, *payload, response->payload_length, interruptible);
    if (rc != READ_RESULT_OK) {
        free(*payload);
        *payload = NULL;
        return rc;
    }

    (*payload)[response->payload_length] = '\0';
    return READ_RESULT_OK;
}

static container_record_t *find_container_by_id_locked(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *record;

    for (record = ctx->containers; record != NULL; record = record->next) {
        if (strcmp(record->id, id) == 0)
            return record;
    }

    return NULL;
}

static container_record_t *find_container_by_pid_locked(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t *record;

    for (record = ctx->containers; record != NULL; record = record->next) {
        if (record->host_pid == pid)
            return record;
    }

    return NULL;
}

static int active_container_count(supervisor_ctx_t *ctx)
{
    container_record_t *record;
    int count = 0;

    pthread_mutex_lock(&ctx->metadata_lock);
    for (record = ctx->containers; record != NULL; record = record->next) {
        if (is_active_state(record->state))
            count++;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    return count;
}

static char *build_ps_payload(supervisor_ctx_t *ctx, size_t *payload_length)
{
    container_record_t *record;
    char *buffer = NULL;
    size_t size = 0;
    FILE *stream = open_memstream(&buffer, &size);

    if (stream == NULL)
        return NULL;

    fprintf(stream,
            "%-12s %-8s %-10s %-9s %-9s %-6s %-6s %-18s %-19s %s\n",
            "ID",
            "PID",
            "STATE",
            "SOFT_MIB",
            "HARD_MIB",
            "NICE",
            "EXIT",
            "REASON",
            "STARTED",
            "LOG");

    pthread_mutex_lock(&ctx->metadata_lock);
    for (record = ctx->containers; record != NULL; record = record->next) {
        char started_at[32];

        format_time_value(record->started_at, started_at, sizeof(started_at));
        fprintf(stream,
                "%-12s %-8d %-10s %-9lu %-9lu %-6d %-6d %-18s %-19s %s\n",
                record->id,
                record->host_pid,
                state_to_string(record->state),
                record->soft_limit_bytes >> 20,
                record->hard_limit_bytes >> 20,
                record->nice_value,
                record->final_status,
                record->final_reason[0] ? record->final_reason : "-",
                started_at,
                record->log_path);
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (fclose(stream) != 0) {
        free(buffer);
        return NULL;
    }

    *payload_length = size;
    return buffer;
}

static char *read_entire_file(const char *path, size_t *payload_length)
{
    FILE *file = fopen(path, "rb");
    char *buffer;
    long file_size;
    size_t bytes_read;

    if (file == NULL)
        return NULL;

    if (fseek(file, 0, SEEK_END) != 0) {
        fclose(file);
        return NULL;
    }

    file_size = ftell(file);
    if (file_size < 0) {
        fclose(file);
        return NULL;
    }

    if (fseek(file, 0, SEEK_SET) != 0) {
        fclose(file);
        return NULL;
    }

    buffer = malloc((size_t)file_size + 1);
    if (buffer == NULL) {
        fclose(file);
        return NULL;
    }

    bytes_read = fread(buffer, 1, (size_t)file_size, file);
    if (bytes_read != (size_t)file_size && ferror(file)) {
        free(buffer);
        fclose(file);
        return NULL;
    }

    buffer[bytes_read] = '\0';
    *payload_length = bytes_read;
    fclose(file);
    return buffer;
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static void *producer_thread_main(void *arg)
{
    producer_arg_t *producer = arg;
    char chunk[LOG_CHUNK_SIZE];

    for (;;) {
        ssize_t bytes_read = read(producer->read_fd, chunk, sizeof(chunk));

        if (bytes_read == 0)
            break;

        if (bytes_read < 0) {
            if (errno == EINTR)
                continue;
            perror("read");
            break;
        }

        log_item_t item;

        memset(&item, 0, sizeof(item));
        copy_cstr(item.container_id, sizeof(item.container_id), producer->container_id);
        item.length = (size_t)bytes_read;
        memcpy(item.data, chunk, (size_t)bytes_read);

        if (bounded_buffer_push(&producer->ctx->log_buffer, &item) != 0)
            break;
    }

    close(producer->read_fd);
    free(producer);
    return NULL;
}

void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char path[PATH_MAX];
        int fd;

        if (build_log_path(item.container_id, path, sizeof(path)) != 0)
            continue;

        fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            perror("open log");
            continue;
        }

        if (write_full(fd, item.data, item.length) < 0)
            perror("write log");

        close(fd);
    }

    return NULL;
}

int child_fn(void *arg)
{
    child_config_t *cfg = arg;
    int devnull_fd;

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2");
        return 1;
    }

    if (cfg->log_write_fd > STDERR_FILENO)
        close(cfg->log_write_fd);

    devnull_fd = open("/dev/null", O_RDONLY);
    if (devnull_fd >= 0) {
        if (dup2(devnull_fd, STDIN_FILENO) < 0) {
            perror("dup2 stdin");
            return 1;
        }
        if (devnull_fd > STDIN_FILENO)
            close(devnull_fd);
    }

    if (sethostname(cfg->id, strlen(cfg->id)) < 0) {
        perror("sethostname");
        return 1;
    }

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) < 0) {
        perror("mount private");
        return 1;
    }

    if (chdir(cfg->rootfs) < 0) {
        perror("chdir rootfs");
        return 1;
    }

    if (chroot(cfg->rootfs) < 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") < 0) {
        perror("chdir /");
        return 1;
    }

    if (mkdir("/proc", 0555) < 0 && errno != EEXIST) {
        perror("mkdir /proc");
        return 1;
    }

    if (mount("proc", "/proc", "proc", 0, NULL) < 0) {
        perror("mount /proc");
        return 1;
    }

    if (setpriority(PRIO_PROCESS, 0, cfg->nice_value) < 0) {
        perror("setpriority");
        return 1;
    }

    execl("/bin/sh", "/bin/sh", "-c", cfg->command, (char *)NULL);
    perror("execl");
    return 127;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    if (monitor_fd < 0)
        return 0;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    copy_cstr(req.container_id, sizeof(req.container_id), container_id);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    if (monitor_fd < 0)
        return 0;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    copy_cstr(req.container_id, sizeof(req.container_id), container_id);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

static int send_error_reply(int fd, const char *message)
{
    return send_response(fd, 1, 0, message, NULL, 0);
}

static int create_control_socket(void)
{
    int fd;
    struct sockaddr_un addr;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_cstr(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    unlink(CONTROL_PATH);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    if (listen(fd, 16) < 0) {
        close(fd);
        unlink(CONTROL_PATH);
        return -1;
    }

    if (set_fd_nonblocking(fd) < 0) {
        close(fd);
        unlink(CONTROL_PATH);
        return -1;
    }

    return fd;
}

static int request_stop(supervisor_ctx_t *ctx, const char *container_id, char *message, size_t message_size)
{
    container_record_t *record;
    pid_t pid_to_signal = -1;

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container_by_id_locked(ctx, container_id);
    if (record == NULL) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(message, message_size, "No container with id '%s'", container_id);
        return -1;
    }

    if (!is_active_state(record->state)) {
        snprintf(message,
                 message_size,
                 "Container '%s' is already %s",
                 record->id,
                 state_to_string(record->state));
        pthread_mutex_unlock(&ctx->metadata_lock);
        return 0;
    }

    if (!record->stop_requested) {
        record->stop_requested = 1;
        record->stop_requested_at = time(NULL);
        pid_to_signal = record->host_pid;
    }

    snprintf(message, message_size, "Stop requested for '%s' (pid=%d)", record->id, record->host_pid);
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (pid_to_signal > 0) {
        if (kill(pid_to_signal, SIGTERM) < 0 && errno != ESRCH)
            return -1;
    }

    return 0;
}

static int start_container(supervisor_ctx_t *ctx,
                           const control_request_t *req,
                           int wait_client_fd,
                           int *defer_response,
                           char *message,
                           size_t message_size)
{
    container_record_t *existing;
    container_record_t *iter;
    container_record_t *record = NULL;
    producer_arg_t *producer_arg = NULL;
    child_config_t *child_cfg = NULL;
    void *stack = NULL;
    int log_pipe[2] = {-1, -1};
    int scratch_fd = -1;
    char rootfs[PATH_MAX];
    pid_t child_pid;
    int rc;

    *defer_response = 0;

    if (req->container_id[0] == '\0' || req->command[0] == '\0') {
        snprintf(message, message_size, "Container id and command are required");
        return -1;
    }

    if (normalize_existing_path(req->rootfs, rootfs, sizeof(rootfs)) != 0) {
        snprintf(message,
                 message_size,
                 "Unable to resolve rootfs: %s",
                 strerror(errno));
        return -1;
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    existing = find_container_by_id_locked(ctx, req->container_id);
    if (existing != NULL) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(message, message_size, "Container id '%s' already exists", req->container_id);
        return -1;
    }

    for (iter = ctx->containers; iter != NULL; iter = iter->next) {
        if (is_active_state(iter->state) && strcmp(iter->rootfs, rootfs) == 0) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            snprintf(message,
                     message_size,
                     "Rootfs '%s' is already in use by container '%s'",
                     rootfs,
                     iter->id);
            return -1;
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (ensure_log_dir() != 0) {
        snprintf(message, message_size, "Unable to create log directory: %s", strerror(errno));
        return -1;
    }

    record = calloc(1, sizeof(*record));
    producer_arg = calloc(1, sizeof(*producer_arg));
    child_cfg = calloc(1, sizeof(*child_cfg));
    stack = malloc(STACK_SIZE);

    if (record == NULL || producer_arg == NULL || child_cfg == NULL || stack == NULL) {
        snprintf(message, message_size, "Out of memory while creating container");
        goto fail;
    }

    if (pipe(log_pipe) < 0) {
        snprintf(message, message_size, "pipe failed: %s", strerror(errno));
        goto fail;
    }

    copy_cstr(record->id, sizeof(record->id), req->container_id);
    copy_cstr(record->rootfs, sizeof(record->rootfs), rootfs);
    copy_cstr(record->command, sizeof(record->command), req->command);
    record->started_at = time(NULL);
    record->state = CONTAINER_STARTING;
    record->soft_limit_bytes = req->soft_limit_bytes;
    record->hard_limit_bytes = req->hard_limit_bytes;
    record->nice_value = req->nice_value;
    record->exit_code = -1;
    record->exit_signal = 0;
    record->final_status = -1;
    record->wait_client_fd = wait_client_fd;
    copy_cstr(record->final_reason, sizeof(record->final_reason), "starting");

    if (build_log_path(record->id, record->log_path, sizeof(record->log_path)) != 0) {
        snprintf(message, message_size, "Log path is too long for container '%s'", record->id);
        goto fail;
    }

    scratch_fd = open(record->log_path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (scratch_fd < 0) {
        snprintf(message, message_size, "Unable to create log file: %s", strerror(errno));
        goto fail;
    }
    close(scratch_fd);
    scratch_fd = -1;

    copy_cstr(child_cfg->id, sizeof(child_cfg->id), record->id);
    copy_cstr(child_cfg->rootfs, sizeof(child_cfg->rootfs), record->rootfs);
    copy_cstr(child_cfg->command, sizeof(child_cfg->command), record->command);
    child_cfg->nice_value = record->nice_value;
    child_cfg->log_write_fd = log_pipe[1];

    child_pid = clone(child_fn,
                      (char *)stack + STACK_SIZE,
                      CLONE_NEWUTS | CLONE_NEWPID | CLONE_NEWNS | SIGCHLD,
                      child_cfg);
    if (child_pid < 0) {
        snprintf(message, message_size, "clone failed: %s", strerror(errno));
        goto fail;
    }

    record->host_pid = child_pid;
    record->state = CONTAINER_RUNNING;
    copy_cstr(record->final_reason, sizeof(record->final_reason), "running");

    close(log_pipe[1]);
    log_pipe[1] = -1;

    producer_arg->ctx = ctx;
    producer_arg->read_fd = log_pipe[0];
    copy_cstr(producer_arg->container_id, sizeof(producer_arg->container_id), record->id);

    rc = pthread_create(&record->producer_thread, NULL, producer_thread_main, producer_arg);
    if (rc != 0) {
        kill(child_pid, SIGKILL);
        waitpid(child_pid, NULL, 0);
        snprintf(message, message_size, "pthread_create failed: %s", strerror(rc));
        goto fail;
    }

    record->producer_thread_started = 1;
    producer_arg = NULL;

    if (register_with_monitor(ctx->monitor_fd,
                              record->id,
                              record->host_pid,
                              record->soft_limit_bytes,
                              record->hard_limit_bytes) == 0) {
        record->monitor_registered = (ctx->monitor_fd >= 0);
    } else if (ctx->monitor_fd >= 0) {
        kill(child_pid, SIGKILL);
        waitpid(child_pid, NULL, 0);
        pthread_join(record->producer_thread, NULL);
        record->producer_thread_started = 0;
        snprintf(message, message_size, "monitor registration failed: %s", strerror(errno));
        goto fail;
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    record->next = ctx->containers;
    ctx->containers = record;
    pthread_mutex_unlock(&ctx->metadata_lock);

    free(child_cfg);
    free(stack);

    snprintf(message, message_size, "Started container '%s' (pid=%d)", record->id, record->host_pid);
    if (wait_client_fd >= 0)
        *defer_response = 1;
    return 0;

fail:
    if (log_pipe[0] >= 0)
        close(log_pipe[0]);
    if (log_pipe[1] >= 0)
        close(log_pipe[1]);
    if (scratch_fd >= 0)
        close(scratch_fd);
    if (producer_arg != NULL)
        free(producer_arg);
    if (record != NULL)
        free(record);
    if (child_cfg != NULL)
        free(child_cfg);
    if (stack != NULL)
        free(stack);
    return -1;
}

static void finish_container(supervisor_ctx_t *ctx, container_record_t *record)
{
    int wait_fd = -1;
    int producer_started = 0;
    pthread_t producer_thread;
    char message[CONTROL_MESSAGE_LEN];
    int exit_status;

    if (record->monitor_registered) {
        if (unregister_from_monitor(ctx->monitor_fd, record->id, record->host_pid) == 0)
            record->monitor_registered = 0;
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    wait_fd = record->wait_client_fd;
    record->wait_client_fd = -1;
    producer_started = record->producer_thread_started;
    producer_thread = record->producer_thread;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (producer_started) {
        pthread_join(producer_thread, NULL);
        pthread_mutex_lock(&ctx->metadata_lock);
        record->producer_thread_started = 0;
        pthread_mutex_unlock(&ctx->metadata_lock);
    }

    if (wait_fd >= 0) {
        exit_status = record->final_status < 0 ? 1 : record->final_status;
        snprintf(message,
                 sizeof(message),
                 "Container '%s' finished: %s",
                 record->id,
                 record->final_reason);
        send_response(wait_fd, 0, exit_status, message, NULL, 0);
        close(wait_fd);
    }
}

static void reap_children(supervisor_ctx_t *ctx)
{
    for (;;) {
        int status = 0;
        pid_t pid = waitpid(-1, &status, WNOHANG);
        container_record_t *record;

        if (pid <= 0)
            break;

        pthread_mutex_lock(&ctx->metadata_lock);
        record = find_container_by_pid_locked(ctx, pid);
        if (record != NULL) {
            if (WIFEXITED(status)) {
                record->exit_code = WEXITSTATUS(status);
                record->exit_signal = 0;
                record->final_status = record->exit_code;
                if (record->stop_requested) {
                    record->state = CONTAINER_STOPPED;
                    copy_cstr(record->final_reason, sizeof(record->final_reason), "stopped");
                } else {
                    record->state = CONTAINER_EXITED;
                    copy_cstr(record->final_reason, sizeof(record->final_reason), "exited");
                }
            } else if (WIFSIGNALED(status)) {
                record->exit_code = -1;
                record->exit_signal = WTERMSIG(status);
                record->final_status = 128 + record->exit_signal;
                if (record->stop_requested) {
                    record->state = CONTAINER_STOPPED;
                    copy_cstr(record->final_reason, sizeof(record->final_reason), "stopped");
                } else if (record->exit_signal == SIGKILL) {
                    record->state = CONTAINER_KILLED;
                    copy_cstr(record->final_reason, sizeof(record->final_reason), "hard_limit_killed");
                } else {
                    record->state = CONTAINER_KILLED;
                    copy_cstr(record->final_reason, sizeof(record->final_reason), "killed");
                }
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (record != NULL)
            finish_container(ctx, record);
    }
}

static void escalate_stops(supervisor_ctx_t *ctx)
{
    container_record_t *record;
    time_t now = time(NULL);

    pthread_mutex_lock(&ctx->metadata_lock);
    for (record = ctx->containers; record != NULL; record = record->next) {
        if (!is_active_state(record->state))
            continue;
        if (!record->stop_requested || record->stop_escalated)
            continue;
        if ((now - record->stop_requested_at) < STOP_GRACE_SECONDS)
            continue;

        record->stop_escalated = 1;
        kill(record->host_pid, SIGKILL);
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void stop_all_containers(supervisor_ctx_t *ctx)
{
    container_record_t *record;

    pthread_mutex_lock(&ctx->metadata_lock);
    for (record = ctx->containers; record != NULL; record = record->next) {
        if (!is_active_state(record->state))
            continue;

        if (!record->stop_requested) {
            record->stop_requested = 1;
            record->stop_requested_at = time(NULL);
            kill(record->host_pid, SIGTERM);
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static int handle_request(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    char *payload = NULL;
    char message[CONTROL_MESSAGE_LEN];
    size_t payload_length = 0;
    int defer_response = 0;

    memset(&req, 0, sizeof(req));
    memset(message, 0, sizeof(message));

    switch (read_full(client_fd, &req, sizeof(req), 0)) {
    case READ_RESULT_OK:
        break;
    case READ_RESULT_CLOSED:
        close(client_fd);
        return 0;
    default:
        send_error_reply(client_fd, "Failed to read control request");
        close(client_fd);
        return -1;
    }

    switch (req.kind) {
    case CMD_START:
        if (start_container(ctx, &req, -1, &defer_response, message, sizeof(message)) != 0)
            send_error_reply(client_fd, message);
        else
            send_response(client_fd, 0, 0, message, NULL, 0);
        close(client_fd);
        return 0;

    case CMD_RUN:
        if (start_container(ctx, &req, client_fd, &defer_response, message, sizeof(message)) != 0) {
            send_error_reply(client_fd, message);
            close(client_fd);
        } else if (!defer_response) {
            send_response(client_fd, 0, 0, message, NULL, 0);
            close(client_fd);
        }
        return 0;

    case CMD_PS:
        payload = build_ps_payload(ctx, &payload_length);
        if (payload == NULL)
            send_error_reply(client_fd, "Failed to build container list");
        else
            send_response(client_fd, 0, 0, "ok", payload, payload_length);
        free(payload);
        close(client_fd);
        return 0;

    case CMD_LOGS: {
        container_record_t *record;
        char log_path[PATH_MAX];

        pthread_mutex_lock(&ctx->metadata_lock);
        record = find_container_by_id_locked(ctx, req.container_id);
        if (record != NULL)
            copy_cstr(log_path, sizeof(log_path), record->log_path);
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (record == NULL) {
            send_error_reply(client_fd, "Unknown container id");
            close(client_fd);
            return -1;
        }

        payload = read_entire_file(log_path, &payload_length);
        if (payload == NULL)
            send_error_reply(client_fd, "Failed to read log file");
        else
            send_response(client_fd, 0, 0, "ok", payload, payload_length);
        free(payload);
        close(client_fd);
        return 0;
    }

    case CMD_STOP:
        if (request_stop(ctx, req.container_id, message, sizeof(message)) != 0)
            send_error_reply(client_fd, message);
        else
            send_response(client_fd, 0, 0, message, NULL, 0);
        close(client_fd);
        return 0;

    default:
        send_error_reply(client_fd, "Unsupported command");
        close(client_fd);
        return -1;
    }
}

static void free_container_list(supervisor_ctx_t *ctx)
{
    container_record_t *record;
    container_record_t *next;

    pthread_mutex_lock(&ctx->metadata_lock);
    record = ctx->containers;
    ctx->containers = NULL;
    pthread_mutex_unlock(&ctx->metadata_lock);

    while (record != NULL) {
        next = record->next;
        if (record->wait_client_fd >= 0)
            close(record->wait_client_fd);
        free(record);
        record = next;
    }
}

static int install_supervisor_signals(void)
{
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = supervisor_signal_handler;
    sigemptyset(&sa.sa_mask);

    if (sigaction(SIGCHLD, &sa, NULL) < 0)
        return -1;
    if (sigaction(SIGINT, &sa, NULL) < 0)
        return -1;
    if (sigaction(SIGTERM, &sa, NULL) < 0)
        return -1;

    signal(SIGPIPE, SIG_IGN);
    return 0;
}

static int wait_for_shutdown_events(supervisor_ctx_t *ctx)
{
    fd_set readfds;
    struct timeval timeout;
    int max_fd = -1;
    int rc;

    FD_ZERO(&readfds);

    if (ctx->server_fd >= 0) {
        FD_SET(ctx->server_fd, &readfds);
        max_fd = ctx->server_fd;
    }

    if (g_signal_pipe[0] >= 0) {
        FD_SET(g_signal_pipe[0], &readfds);
        if (g_signal_pipe[0] > max_fd)
            max_fd = g_signal_pipe[0];
    }

    timeout.tv_sec = 1;
    timeout.tv_usec = 0;

    rc = select(max_fd + 1, &readfds, NULL, NULL, &timeout);
    if (rc < 0) {
        if (errno == EINTR)
            return 0;
        return -1;
    }

    if (rc == 0)
        return 0;

    if (g_signal_pipe[0] >= 0 && FD_ISSET(g_signal_pipe[0], &readfds))
        drain_signal_pipe();

    if (ctx->server_fd >= 0 && FD_ISSET(ctx->server_fd, &readfds)) {
        for (;;) {
            int client_fd = accept(ctx->server_fd, NULL, NULL);

            if (client_fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    break;
                if (errno == EINTR)
                    continue;
                return -1;
            }

            handle_request(ctx, client_fd);
        }
    }

    return 0;
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    if (normalize_existing_path(rootfs, ctx.base_rootfs, sizeof(ctx.base_rootfs)) != 0) {
        perror("base-rootfs");
        return 1;
    }

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (ensure_log_dir() != 0) {
        perror("mkdir logs");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (pipe(g_signal_pipe) < 0) {
        perror("pipe");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    set_fd_nonblocking(g_signal_pipe[0]);
    set_fd_nonblocking(g_signal_pipe[1]);

    if (install_supervisor_signals() != 0) {
        perror("sigaction");
        close(g_signal_pipe[0]);
        close(g_signal_pipe[1]);
        g_signal_pipe[0] = -1;
        g_signal_pipe[1] = -1;
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "Warning: /dev/container_monitor unavailable, continuing without kernel monitor\n");

    ctx.server_fd = create_control_socket();
    if (ctx.server_fd < 0) {
        perror("control socket");
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        close(g_signal_pipe[0]);
        close(g_signal_pipe[1]);
        g_signal_pipe[0] = -1;
        g_signal_pipe[1] = -1;
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        close(g_signal_pipe[0]);
        close(g_signal_pipe[1]);
        g_signal_pipe[0] = -1;
        g_signal_pipe[1] = -1;
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }
    ctx.logger_thread_started = 1;

    while (!ctx.should_stop) {
        if (wait_for_shutdown_events(&ctx) != 0) {
            perror("select");
            ctx.should_stop = 1;
        }

        if (g_sigchld_seen) {
            g_sigchld_seen = 0;
            reap_children(&ctx);
        }

        if (g_shutdown_seen)
            ctx.should_stop = 1;

        escalate_stops(&ctx);
    }

    if (ctx.server_fd >= 0) {
        close(ctx.server_fd);
        ctx.server_fd = -1;
    }
    unlink(CONTROL_PATH);

    stop_all_containers(&ctx);

    while (active_container_count(&ctx) > 0) {
        wait_for_shutdown_events(&ctx);
        if (g_sigchld_seen) {
            g_sigchld_seen = 0;
            reap_children(&ctx);
        }
        escalate_stops(&ctx);
    }

    reap_children(&ctx);
    bounded_buffer_begin_shutdown(&ctx.log_buffer);

    if (ctx.logger_thread_started)
        pthread_join(ctx.logger_thread, NULL);

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    if (g_signal_pipe[0] >= 0)
        close(g_signal_pipe[0]);
    if (g_signal_pipe[1] >= 0)
        close(g_signal_pipe[1]);
    g_signal_pipe[0] = -1;
    g_signal_pipe[1] = -1;

    free_container_list(&ctx);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
}

static int connect_control_socket(void)
{
    int fd;
    struct sockaddr_un addr;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
        copy_cstr(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    return fd;
}

static int send_stop_request_silently(const char *container_id)
{
    control_request_t req;
    control_response_t response;
    char *payload = NULL;
    int fd;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    copy_cstr(req.container_id, sizeof(req.container_id), container_id);

    fd = connect_control_socket();
    if (fd < 0)
        return -1;

    if (write_full(fd, &req, sizeof(req)) < 0) {
        close(fd);
        return -1;
    }

    if (receive_response(fd, &response, &payload, 0) != READ_RESULT_OK) {
        close(fd);
        free(payload);
        return -1;
    }

    free(payload);
    close(fd);
    return response.status == 0 ? 0 : -1;
}

static int send_control_request(const control_request_t *req)
{
    int fd;
    control_response_t response;
    char *payload = NULL;
    int forwarded_stop = 0;
    struct sigaction old_int;
    struct sigaction old_term;
    struct sigaction sa;
    int need_signal_handlers = (req->kind == CMD_RUN);

    memset(&old_int, 0, sizeof(old_int));
    memset(&old_term, 0, sizeof(old_term));

    signal(SIGPIPE, SIG_IGN);

    if (need_signal_handlers) {
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = client_signal_handler;
        sigemptyset(&sa.sa_mask);
        g_client_forward_stop = 0;

        if (sigaction(SIGINT, &sa, &old_int) < 0) {
            perror("sigaction");
            return 1;
        }
        if (sigaction(SIGTERM, &sa, &old_term) < 0) {
            perror("sigaction");
            sigaction(SIGINT, &old_int, NULL);
            return 1;
        }
    }

    fd = connect_control_socket();
    if (fd < 0) {
        perror("connect");
        if (need_signal_handlers) {
            sigaction(SIGINT, &old_int, NULL);
            sigaction(SIGTERM, &old_term, NULL);
        }
        return 1;
    }

    if (write_full(fd, req, sizeof(*req)) < 0) {
        perror("write");
        close(fd);
        if (need_signal_handlers) {
            sigaction(SIGINT, &old_int, NULL);
            sigaction(SIGTERM, &old_term, NULL);
        }
        return 1;
    }

    for (;;) {
        enum read_result rc = receive_response(fd, &response, &payload, need_signal_handlers);

        if (rc == READ_RESULT_OK)
            break;

        if (rc == READ_RESULT_INTERRUPTED && need_signal_handlers) {
            if (g_client_forward_stop && !forwarded_stop) {
                send_stop_request_silently(req->container_id);
                forwarded_stop = 1;
            }
            g_client_forward_stop = 0;
            continue;
        }

        if (rc == READ_RESULT_CLOSED)
            fprintf(stderr, "Supervisor closed the connection unexpectedly\n");
        else
            perror("read");

        close(fd);
        if (need_signal_handlers) {
            sigaction(SIGINT, &old_int, NULL);
            sigaction(SIGTERM, &old_term, NULL);
        }
        return 1;
    }

    close(fd);

    if (need_signal_handlers) {
        sigaction(SIGINT, &old_int, NULL);
        sigaction(SIGTERM, &old_term, NULL);
    }

    if (payload != NULL && response.payload_length > 0)
        fwrite(payload, 1, response.payload_length, stdout);

    if (response.status != 0) {
        if (response.message[0] != '\0')
            fprintf(stderr, "%s\n", response.message);
        free(payload);
        return 1;
    }

    if (req->kind != CMD_PS && req->kind != CMD_LOGS && response.message[0] != '\0')
        printf("%s\n", response.message);

    free(payload);

    if (req->kind == CMD_RUN)
        return response.exit_status;

    return 0;
}

static int validate_string_size(const char *value, size_t max_size, const char *field_name)
{
    if (strlen(value) >= max_size) {
        fprintf(stderr, "%s is too long\n", field_name);
        return -1;
    }

    return 0;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    if (validate_string_size(argv[2], sizeof(req.container_id), "container id") != 0 ||
        validate_string_size(argv[3], sizeof(req.rootfs), "container rootfs") != 0 ||
        validate_string_size(argv[4], sizeof(req.command), "command") != 0)
        return 1;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);
    copy_cstr(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_cstr(req.command, sizeof(req.command), argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    if (validate_string_size(argv[2], sizeof(req.container_id), "container id") != 0 ||
        validate_string_size(argv[3], sizeof(req.rootfs), "container rootfs") != 0 ||
        validate_string_size(argv[4], sizeof(req.command), "command") != 0)
        return 1;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);
    copy_cstr(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_cstr(req.command, sizeof(req.command), argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

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
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    if (validate_string_size(argv[2], sizeof(req.container_id), "container id") != 0)
        return 1;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    if (validate_string_size(argv[2], sizeof(req.container_id), "container id") != 0)
        return 1;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);

    return send_control_request(&req);
}

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

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
