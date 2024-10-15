#ifndef _META_SERVER_H
#define _META_SERVER_H

#include "util/flags.h"
#include "util/log.h"

#include <sys/socket.h>
#include <sys/epoll.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string>
#include <vector>

#define MAX_CONNECTION 4096

void run() {
    std::vector<std::string> proxy_servers = parse_proxy_server_list();
    std::string proxy_ip = proxy_servers[0];
    sockaddr_in proxy_address;
    proxy_address.sin_family = AF_INET;
    inet_pton(AF_INET, proxy_ip.c_str(), &proxy_address.sin_addr);
    proxy_address.sin_port = htons(20001);
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(20000);
    int status_code = bind(server_fd, (sockaddr *)&server_address, sizeof(sockaddr_in));
    CHECK(status_code == 0) << "cannot bind to 20000";
    listen(server_fd, MAX_CONNECTION);
    LOG(INFO) << "meta server started";
    int epoll_fd = epoll_create1(0);
    epoll_event event, events[MAX_CONNECTION];
    event.events = EPOLLIN;
    event.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event);
    while (true) {
        int num_events = epoll_wait(epoll_fd, events, MAX_CONNECTION, -1);
        for (int i = 0; i < num_events; i++) {
            if (events[i].data.fd == server_fd) {
                sockaddr_in client_address;
                socklen_t client_address_size = sizeof(sockaddr_in);
                int client_fd = accept(server_fd, (sockaddr *)&client_address, &client_address_size);
                send(client_fd, &proxy_address, sizeof(sockaddr_in), 0);
                close(client_fd);
            } else {
                continue;
            }
        }
    }
    close(server_fd);
    close(epoll_fd);
}

#endif