//
//  ngx_gowild_kafka.c
//  qdreamer-proxy
//
//  Created by psyun on 2017/9/28.
//  Copyright © 2017年 gowild. All rights reserved.
//

#include "ngx_stream.h"

typedef struct {
    u_char      *deviceId;
    u_char      *hook;
    u_char      *body;
} ngx_gowild_kafka_msg_t;

void
add_line(char **res, int *line_offset, char *buf){
    res[*line_offset] = (char*)malloc(((int)strlen(buf)) * sizeof(char*));
    strcpy(res[*line_offset], buf);
    *line_offset += 1;
}

void 
ngx_gowild_msg_parse(char *msg, ngx_log_t *log){
    int offset = 0;
    int i = 0;
    char buf[512];
    int slash_count = 0;
    int line_offset = 0;
    char **res;
    res = (char**)malloc(10 * sizeof(char*));

    do {
        if(slash_count == 2) {
            buf[offset] = msg[i];
            offset += 1;
        } else if(msg[i] == '\r' && msg[i + 1] == '\n') {
            i += 2;
            buf[offset] = '\0';
            add_line(res, &line_offset, buf);

            offset = 0;
            slash_count += 1;
            
            continue;

        } else {
            slash_count = 0;
            buf[offset] = msg[i];
            offset += 1;
        }

        i += 1;
    } while (msg[i] != '\0');
    add_line(res, &line_offset, buf);

    for(i =0; i < line_offset; i++) {
        ngx_log_debug1(NGX_LOG_DEBUG_STREAM, log, 0, "http response header: %s\n", res[i]);
    }
}

void
ngx_gowild_kafka_send_msg(ngx_log_t *log, ngx_buf_t *b) {
    char *msg = (char*)b->pos;
    size_t len = strlen(msg);
    ngx_gowild_msg_parse(msg, log);
    if(rd_kafka_produce(rkt,
                    RD_KAFKA_PARTITION_UA,
                    RD_KAFKA_MSG_F_COPY,
                    msg, len,
                    NULL, 0,
                    NULL) == -1){
                        ngx_log_debug1(NGX_LOG_DEBUG_STREAM, log, 0,
                                "Failed send msg to kafka:%s",
                                rd_kafka_err2str(rd_kafka_last_error()));
                    }
}
