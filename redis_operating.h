#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <assert.h>
#include <hiredis/hiredis.h>
#include <apr_pools.h>
#include <apr_hash.h>
#include <osip2/osip.h>
#include <osip2/osip_timer.h>

#ifndef __REDIS_OPERATING_H
#define __REDIS_OPERATING_H

typedef struct redis_reply_result
{
	int type;
	char *result;
}redis_reply_result_t;

typedef struct hash_string
{
	apr_pool_t *pool;
	char *string;
}hash_string_t;

typedef struct yaproxy_lock
{
    apr_pool_t *pool;
	char *ipcGbID;/* ipc id */
	char *clientID;
	int level;
	apr_time_t timer;
}yaproxy_lock_t;

typedef struct redis_operating
{
	apr_pool_t *pool;
	int id;
	char* type;
	char *key;
	redisContext* connect;
}redis_operating_t;

typedef enum _redis_operating_type{
	NUMBERS,
	STRINGS,
	TIME,
	REFERENCE,
	LIST
} redis_operating_type_t;

redisContext *__redis_operating_connect(const char *ip, int port, struct timeval *tv);

int __redis_operating_execution(redisContext* gClient, char *strcmd, apr_pool_t *pool, redis_reply_result_t *reply_result);

osip_ring_t* __redis_operating_reader(apr_pool_t *pool, redisContext* gClient, char *strcmd);

int __redis_operating_execution_multi(apr_pool_t *pool, redisContext* gClient, osip_ring_t* ring);

apr_hash_t* __redis_operating_reader_multi(redisContext* gClient, apr_pool_t *pool, osip_ring_t* ring);

int redis_operating_hset(apr_pool_t *pool, const char *ip, int port, const struct timeval tv, char *key, char *field, char *value);

int redis_operating_hmset(apr_pool_t *pool, const char *ip, int port, const struct timeval tv, char *key, apr_hash_t *hash, char *str_val);

int redis_operating_hget(apr_pool_t *pool, const char *ip, int port, const struct timeval tv, char *key, 
						 char *field, char **value);

int redis_operating_hmget(apr_pool_t *pool, const char *ip, int port, 
						  struct timeval tv,char *key,  osip_ring_t *key_ring, osip_ring_t **val_ring);

int redis_operating_hgetall(apr_pool_t *pool, const char *ip, int port, 
							struct timeval tv,char *key, osip_ring_t **val_ring);

int redis_operating_sadd(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, osip_ring_t *ring, char *str_val);

int redis_operating_srem(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, osip_ring_t *ring, char *str_val);

int redis_operating_sinter(apr_pool_t *pool, const char *ip, int port, 
						   struct timeval tv, osip_ring_t *key_ring, osip_ring_t **val_ring);

int redis_operating_zadd(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, osip_ring_t *ring, char *str_val);

int redis_operating_zrem(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, osip_ring_t *ring, char *str_val);

//int redis_operating_del(apr_pool_t *pool, const char *ip, int port, const struct timeval tv, osip_ring_t *key_ring);
int redis_operating_del(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key);

int redis_operating_incr(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, int increment);

int redis_operating_set(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, char *value, int ex_flag);

int redis_operating_mset(apr_pool_t *pool, const char *ip, int port, struct timeval tv, apr_hash_t *hash);

int redis_operating_get(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, char **value);

int redis_operating_mget(apr_pool_t *pool, const char *ip, int port, 
						  struct timeval tv,osip_ring_t *key_ring, osip_ring_t **val_ring);

int redis_operating_keys(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, osip_ring_t **val_ring);

int redis_operating_exists(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key);

//此处之后的函数，考虑在以后分成独立的h文件：redis_struct.h
typedef int (*func_call_class)(apr_pool_t *, char *, void *);

int __redis_update_class_id(apr_pool_t *pool, char *type);

osip_ring_t* redis_get_class_id(apr_pool_t *pool, char *type, char *member, ...);

int __redis_set_class_num(apr_pool_t *pool, char *value, char *type, int id);

int __redis_set_class_all(apr_pool_t *pool, char *type, int id);

int __redis_set_class_timerheap(apr_pool_t *pool, char *type, char *key, apr_time_t timer);

int redis_get_class_timerheap(apr_pool_t *pool, char *type, apr_time_t timer, osip_ring_t **result);

int __redis_set_class_memberset(apr_pool_t *pool, char *type, char *member_name, char *member_val, int class_id);

int __redis_set_class_indices(apr_pool_t *pool, char *type, char *member_name, char *member_val, int class_id);

int __redis_set_class_zindices(apr_pool_t *pool, char *type, char *member_name, char *member_val, int class_id);

int redis_del_objects_bymember(apr_pool_t *pool, char *type, char *member, ...);

int redis_del_single_object_byid(apr_pool_t *pool, char *type, int id);

int __redis_del_single_object_dictset(apr_pool_t *pool, char *type, int id, char *key);

int __redis_del_single_object_memberset(apr_pool_t *pool, char *key, int id);

int __redis_del_single_object_timerheap(apr_pool_t *pool, char *key, char *type, int id);

int redis_update_single_object_byid(apr_pool_t *pool, char *type, int id, char member_name, char *new_member_val);

int __redis_update_single_object_dictset(apr_pool_t *pool, char *type, char *member_name, char *old_member_val, 
										 char *new_member_val, int id, char *key);

int __redis_update_single_object_memberset(apr_pool_t *pool, char *type, char *member_name, 
										   char *old_member_val, char *new_member_val, int id);

int redis_update_single_object_timerheap(apr_pool_t *pool, char *key, char *type, int id, apr_time_t new_time);

int __cleanup_redis_operating(void *ctx);

redis_operating_t *redis_operating_create(apr_pool_t *pool, char *type);

#endif
