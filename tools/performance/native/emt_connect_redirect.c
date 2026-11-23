/*
 * Test-only, per-process connect() redirect for the EMT fault fixture.
 *
 * It changes no firewall, routing table, service, or connection other than
 * an AF_INET connect that exactly matches the three environment variables
 * below.  The proxy child sets the bypass flag before opening its real
 * upstream connection, preventing a redirect loop.
 */

#define _GNU_SOURCE

#include <arpa/inet.h>
#include <dlfcn.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

typedef int (*connect_fn)(int, const struct sockaddr *, socklen_t);

static connect_fn real_connect_fn(void) {
    static connect_fn real = NULL;
    if (real == NULL) {
        real = (connect_fn)dlsym(RTLD_NEXT, "connect");
    }
    return real;
}

static int env_port(const char *name, unsigned short *out) {
    const char *value = getenv(name);
    char *end = NULL;
    long parsed;
    if (value == NULL || *value == '\0') {
        return 0;
    }
    errno = 0;
    parsed = strtol(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed < 1 || parsed > 65535) {
        return 0;
    }
    *out = (unsigned short)parsed;
    return 1;
}

static int should_redirect(const struct sockaddr *address, socklen_t length) {
    const char *enabled = getenv("TRADECORE_SESSION_FAULT_CONNECT_REDIRECT");
    const char *bypass = getenv("TRADECORE_SESSION_FAULT_CONNECT_REDIRECT_BYPASS");
    const char *target_host = getenv("TRADECORE_SESSION_FAULT_TARGET_HOST");
    struct in_addr configured_target;
    const struct sockaddr_in *target;
    unsigned short target_port;
    unsigned short proxy_port;

    if (enabled == NULL || strcmp(enabled, "1") != 0 || bypass != NULL || target_host == NULL) {
        return 0;
    }
    if (address == NULL || address->sa_family != AF_INET || length < sizeof(struct sockaddr_in)) {
        return 0;
    }
    if (inet_pton(AF_INET, target_host, &configured_target) != 1) {
        return 0;
    }
    if (!env_port("TRADECORE_SESSION_FAULT_TARGET_PORT", &target_port) ||
        !env_port("TRADECORE_SESSION_FAULT_PROXY_PORT", &proxy_port)) {
        return 0;
    }
    target = (const struct sockaddr_in *)address;
    return target->sin_addr.s_addr == configured_target.s_addr && target->sin_port == htons(target_port) && proxy_port != 0;
}

int connect(int socket_fd, const struct sockaddr *address, socklen_t length) {
    connect_fn real = real_connect_fn();
    if (real == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (!should_redirect(address, length)) {
        return real(socket_fd, address, length);
    }
    {
        struct sockaddr_in redirected = *(const struct sockaddr_in *)address;
        redirected.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        {
            unsigned short proxy_port;
            if (!env_port("TRADECORE_SESSION_FAULT_PROXY_PORT", &proxy_port)) {
                errno = EINVAL;
                return -1;
            }
            redirected.sin_port = htons(proxy_port);
        }
        return real(socket_fd, (const struct sockaddr *)&redirected, sizeof(redirected));
    }
}
