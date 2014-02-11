#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdarg.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include <hiredis/hiredis.h>
#include "redis_operating.h"
#include <osip2/osip.h>
#include <apr_strings.h>
#include <apr_pools.h>
#include <unistd.h>
#include <apr_thread_pool.h>
#include <time.h>
#include <apr_time.h>

int main()
{
/*
	unsigned int j;
	redisContext *c;
	redisReply *reply;
	int size = 100000;

	clock_t start,finish;
	double jishi;
	char key[1000];
	char val[1000];
	char times[100];

	struct timeval timeout = { 1, 500000 }; // 1.5 seconds
	c = redisConnectWithTimeout((char*)"127.0.0.1", 6379, timeout);
	if (c->err) {
		printf("Connection error: %s\n", c->errstr);
		exit(1);
	}
	start = clock();
	return 0;
*/

	int i = 0;int j = 0;
	redisContext *c;
	redisReply *reply;
	apr_pool_t *pool = NULL;
	apr_pool_t *subpool = NULL;
	apr_hash_t *hash = NULL;
	apr_hash_t *hash_str = NULL;
	char *tmp1 = NULL;
	char *tmp2 = NULL;
	osip_ring_t *key_ring = NULL;
	osip_ring_t *val_ring = NULL;
	osip_ring_t *string_key_ring = NULL;

	struct timeval timeout = { 1, 500000 }; // 1.5 seconds
	apr_pool_initialize();
	//apr_pool_create(&pool, NULL);

	apr_time_t now = 0;
	now = apr_time_now();

	//测试HMSET/HMGET
	/*for(j=0;j<10000;j++)
	{
		apr_pool_create(&subpool, pool);
		hash = apr_hash_make(subpool);

		tmp1 = apr_psprintf(subpool, "test1:%d:%d", i, i+1);
		tmp2 = apr_psprintf(subpool, "%d", i+101);
		apr_hash_set(hash, tmp1, APR_HASH_KEY_STRING, tmp2);

		tmp1 = apr_psprintf(subpool, "test2:%d:%d", i, i+1);
		tmp2 = apr_psprintf(subpool, "%d", i+101);
		apr_hash_set(hash, tmp1, APR_HASH_KEY_STRING, tmp2);

		tmp1 = apr_psprintf(pool, "test_hmeset-%d-%d", j, i);
		redis_operating_hmset(pool,(char*)"127.0.0.1", 6379, timeout, tmp1, hash);

		apr_hash_clear(hash);
		apr_pool_destroy(subpool);
	}

	for(j=0;j<10000;j++)
	{
		apr_pool_create(&subpool, pool);
		osip_ring_create(subpool, &key_ring);

		tmp1 = apr_psprintf(subpool, "test1:%d:%d", i, i+1);
		osip_ring_add(key_ring, tmp1, -1);

		tmp1 = apr_psprintf(subpool, "test2:%d:%d", i, i+1);
		osip_ring_add(key_ring, tmp1, -1);

		tmp1 = apr_psprintf(pool, "test_hmeset-%d-%d", j, i);
		redis_operating_hmget(pool,(char*)"127.0.0.1", 6379, timeout, tmp1, key_ring, &val_ring);

		apr_pool_destroy(subpool);
	}*/

	//测试mset
	/*hash_str = apr_hash_make(pool);
	osip_ring_create(pool, &string_key_ring);
	for(i = 0;i < 10000; i++)
	{
		tmp1 = apr_psprintf(pool, "1:%d:%d", i, i+1);
		tmp2 = apr_psprintf(pool, "%d", i+101);
		apr_hash_set(hash_str, tmp1, APR_HASH_KEY_STRING, tmp2);
		//osip_ring_add(string_key_ring, tmp1, -1);

		tmp1 = apr_psprintf(pool, "2:%d:%d", i, i+1);
		tmp2 = apr_psprintf(pool, "%d", i+101);
		apr_hash_set(hash_str, tmp1, APR_HASH_KEY_STRING, tmp2);
		//osip_ring_add(string_key_ring, tmp1, -1);
	}

	val_ring = NULL;
	redis_operating_mset(pool,(char*)"127.0.0.1", 6379, timeout, hash_str);*/

	//测试mget
	/*
	tmp1 = apr_psprintf(pool, "2:%d:%d", 1, 2);
	osip_ring_add(string_key_ring, tmp1, -1);
	tmp1 = apr_psprintf(pool, "1:%s:%d", "a1", 1);
	osip_ring_add(string_key_ring, tmp1, -1);
	tmp1 = apr_psprintf(pool, "2s:%d:%d", 4141, 4142);
	osip_ring_add(string_key_ring, tmp1, -1);

	redis_operating_mget(pool,(char*)"127.0.0.1", 6379, timeout, string_key_ring, &val_ring);
	tmp1 = apr_psprintf(pool, "%d:*", 2);
	redis_operating_keys(pool,(char*)"127.0.0.1", 6379, timeout, tmp1, &val_ring);

	void *obj;
	osip_ring_iterator_t *it = NULL;
	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));
	int iCount = 0;

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(val_ring, it); obj; obj = osip_ring_get_next(it))
	{
		iCount++;
		apr_pool_create(&subpool2, pool);
	    printf("%d : %s\n", iCount, (char *)obj);
		apr_pool_destroy(strcmd->pool);
		strcmd->pool = subpool2;
	}
	osip_ring_destroy_iterator(it);*/

	//set/get, hset/hget测试
	/*tmp1 = apr_psprintf(pool, "1:%d:%d", i, i+1);
	tmp2 = apr_psprintf(pool, "%d", i+101);

	redis_operating_set(pool, (char*)"127.0.0.1", 6379, timeout, tmp1, tmp2, -1);
	redis_operating_get(pool, (char*)"127.0.0.1", 6379, timeout, tmp1, &tmp2);
	printf("GET:%s\n", tmp2);

	tmp2 = apr_psprintf(pool, "%d", i+101);
	redis_operating_hset(pool, (char*)"127.0.0.1", 6379, timeout, "hash_hey_1", tmp1, tmp2);
	redis_operating_hget(pool, (char*)"127.0.0.1", 6379, timeout, "hash_hey_1", tmp1, &tmp2);
	printf("HGET:%s\n", tmp2);*/

	//测试EXISTS
	//redis_operating_exists(pool,(char*)"127.0.0.1", 6379, timeout, "2:1:2");

	//SADD/SREM测试
	/*osip_ring_create(pool, &val_ring);
	for(i = 0;i < 10000; i++)
	{
		tmp2 = apr_psprintf(pool, "%d", i+101);
		osip_ring_add(val_ring, tmp2, -1);
	}

	redis_operating_sadd(pool, (char*)"127.0.0.1", 6379, timeout, "set_test1", val_ring);
	redis_operating_sadd(pool, (char*)"127.0.0.1", 6379, timeout, "set_test2", val_ring);

	osip_ring_create(pool, &key_ring);osip_ring_add(key_ring, "set_test1", -1);osip_ring_add(key_ring, "set_test2", -1);
	redis_operating_del(pool, (char*)"127.0.0.1", 6379, timeout, key_ring);*/

	//test_thread_pool();
	test_proxy_lock_set();
	test_proxy_lock_update();
	//test_timeheap();
	return;
}

void test_fork()
{
	pid_t fpid;

	int i = 0;int j = 0;
	redisContext *c;
	redisReply *reply;
	apr_pool_t *pool = NULL;
	apr_pool_t *subpool = NULL;
	apr_hash_t *hash = NULL;
	apr_hash_t *hash_str = NULL;
	char *tmp1 = NULL;
	char *tmp2 = NULL;
	osip_ring_t *key_ring = NULL;
	osip_ring_t *val_ring = NULL;
	osip_ring_t *string_key_ring = NULL;

	struct timeval timeout = { 1, 500000 }; // 1.5 seconds
	apr_pool_initialize();
	apr_pool_create(&pool, NULL);

	fpid=fork();
	if (fpid < 0)
		printf("error in fork!");
	else if (fpid == 0) {
		for(j=0;j<10000;j++)
		{
			apr_pool_create(&subpool, pool);
			hash = apr_hash_make(subpool);
			tmp1 = apr_psprintf(subpool, "c-test1:%d:%d", i, i+1);
			tmp2 = apr_psprintf(subpool, "%d", i+101);
			apr_hash_set(hash, tmp1, APR_HASH_KEY_STRING, tmp2);
			tmp1 = apr_psprintf(subpool, "c-test2:%d:%d", i, i+1);
			tmp2 = apr_psprintf(subpool, "%d", i+101);
			apr_hash_set(hash, tmp1, APR_HASH_KEY_STRING, tmp2);
			tmp1 = apr_psprintf(pool, "c-test_hmeset-%d-%d", j, i);
			redis_operating_hmset(pool,(char*)"127.0.0.1", 6379, timeout, tmp1, hash, NULL);
			apr_hash_clear(hash);
			apr_pool_destroy(subpool);
		}
	}
	else {
		for(j=0;j<10000;j++)
		{
			apr_pool_create(&subpool, pool);
			hash = apr_hash_make(subpool);
			tmp1 = apr_psprintf(subpool, "p-test1:%d:%d", i, i+1);
			tmp2 = apr_psprintf(subpool, "%d", i+101);
			apr_hash_set(hash, tmp1, APR_HASH_KEY_STRING, tmp2);
			tmp1 = apr_psprintf(subpool, "p-test2:%d:%d", i, i+1);
			tmp2 = apr_psprintf(subpool, "%d", i+101);
			apr_hash_set(hash, tmp1, APR_HASH_KEY_STRING, tmp2);
			tmp1 = apr_psprintf(pool, "p-test_hmeset-%d-%d", j, i);
			redis_operating_hmset(pool,(char*)"127.0.0.1", 6379, timeout, tmp1, hash, NULL);
			apr_hash_clear(hash);
			apr_pool_destroy(subpool);
		}
	}
	return 0;
}

apr_pool_t *__pool = NULL;

static void *task_handle(apr_thread_t *me, void *param)
{
	int i = 0;int j = (int)param;
	redisContext *c;
	redisReply *reply;
	
	apr_pool_t *subpool = NULL;
	apr_hash_t *hash = NULL;
	apr_hash_t *hash_str = NULL;
	char *tmp1 = NULL;
	char *tmp2 = NULL;
	osip_ring_t *key_ring = NULL;
	osip_ring_t *val_ring = NULL;
	osip_ring_t *string_key_ring = NULL;
	apr_thread_pool_t *thread_pool = NULL;

	struct timeval timeout = { 1, 500000 }; // 1.5 seconds
	apr_pool_create(&subpool, __pool);

	/*hash = apr_hash_make(subpool);
	tmp1 = apr_psprintf(subpool, "p%d-test1:%d:%d", j, i, i+1);
	tmp2 = apr_psprintf(subpool, "%d", i+101);
	apr_hash_set(hash, tmp1, APR_HASH_KEY_STRING, tmp2);
	tmp1 = apr_psprintf(subpool, "p%d-test2:%d:%d", j, i, i+1);
	tmp2 = apr_psprintf(subpool, "%d", i+101);
	apr_hash_set(hash, tmp1, APR_HASH_KEY_STRING, tmp2);
	tmp1 = apr_psprintf(pool, "p-test_hmeset-%d-%d", j, i);
	redis_operating_hmset(pool,(char*)"127.0.0.1", 6379, timeout, tmp1, hash);
	apr_hash_clear(hash);
	apr_pool_destroy(subpool);*/

	tmp1 = apr_psprintf(subpool, "1:%d:%d", 1, 2);
	tmp2 = apr_psprintf(subpool, "new-%d", j);
	redis_operating_get(subpool, (char*)"127.0.0.1", 6379, timeout, tmp1, &tmp2);
	redis_operating_set(subpool, (char*)"127.0.0.1", 6379, timeout, tmp1, tmp2, -1);
	printf("GET:%s\n", tmp2);
	apr_pool_destroy(subpool);
}

void test_thread_pool()
{
	pid_t fpid;
	int j = 0;
	redisContext *c;
	redisReply *reply;
	apr_pool_t *__pool = NULL;
	apr_pool_t *subpool = NULL;
	apr_hash_t *hash = NULL;
	apr_hash_t *hash_str = NULL;
	char *tmp1 = NULL;
	char *tmp2 = NULL;
	osip_ring_t *key_ring = NULL;
	osip_ring_t *val_ring = NULL;
	osip_ring_t *string_key_ring = NULL;
	apr_thread_pool_t *thread_pool = NULL;

	struct timeval timeout = { 1, 500000 }; // 1.5 seconds
	apr_pool_initialize();
	apr_pool_create(&__pool, NULL);
	apr_thread_pool_create(&thread_pool, 1000, 1000, __pool);
	for(j=0;j<10000;j++)
	{
		apr_thread_pool_push(thread_pool, task_handle, j,
							 APR_THREAD_TASK_PRIORITY_NORMAL, NULL);
	}
	sleep(10);
}

apr_time_t yasips_misc_str_totime_byformat(apr_time_t now_time)
{
	apr_time_t result = 0;
	apr_time_exp_t timec;
	apr_time_exp_gmt(&timec, now_time);
	timec.tm_sec = timec.tm_sec + 30;
	apr_time_exp_gmt_get(&result, &timec);
	return result;
}

//bussiness
int func_call_test_complex2_set(apr_pool_t *pool, char *type, void *obj)
{
	int ret = -1;
	test_complex_2_t *object = (test_complex_2_t *)obj;
	redis_operating_t *handle = redis_operating_nowatch_init(pool, type, 0);
	
	db_generate_string_member(pool, handle, "count_2", apr_psprintf(pool, "%d", object->count_2), 0, 1);
	db_generate_string_member(pool, handle, "str_2", object->str_2, 0, 1);
	db_generate_timer_member(pool, handle, "test_timeheap", "timer_2", object->timer_2, 1, 0);

	redis_operating_exec(handle->pool, handle);
	ret = handle->id;
END:
	return ret;
}

int func_call_test_complex1_set(apr_pool_t *pool, char *type, void *obj)
{
	int ret = -1;
	osip_ring_iterator_t *it = NULL;
	void *sub_obj = NULL;
	test_complex_1_t *object = (test_complex_1_t *)obj;
	redis_operating_t *handle = redis_operating_nowatch_init(pool, type, 0);
	
	db_generate_string_member(pool, handle, "count_1", apr_psprintf(pool, "%d", object->count_1), 0, 1);
	db_generate_string_member(pool, handle, "str_1", object->str_1, 0, 1);
	db_generate_timer_member(pool, handle, "test_timeheap", "timer_1", object->timer_1, 1, 0);

	osip_ring_create_iterator(pool, &it);
	for(sub_obj = osip_ring_get_first(object->ring_complex_2, it); sub_obj; sub_obj = osip_ring_get_next(it))
	{
		db_generate_list_member(pool, handle, "test_complex_2_t", "ring_complex_2", sub_obj, 1, func_call_test_complex2_set);
	}
	osip_ring_destroy_iterator(it);

	redis_operating_exec(handle->pool, handle);
	ret = handle->id;
END:
	return ret;
}

int func_call_yaproxy_lock_set(apr_pool_t *pool, char *type, void *obj)
{
	int ret = -1;
	yaproxy_lock_t *object = (yaproxy_lock_t *)obj;
	redis_operating_t *handle = redis_operating_nowatch_init(pool, type, 0);
	
	db_generate_string_member(pool, handle, "ipcGbID", object->ipcGbID, 0, 1);
	db_generate_string_member(pool, handle, "clientID", object->clientID, 0, 1);
	db_generate_string_member(pool, handle, "level", apr_psprintf(pool, "%d", object->level), 0, 1);
	db_generate_timer_member(pool, handle, "test_timeheap", "timer", object->timer, 1, 0);
	db_generate_reference_member(pool, handle, "test_complex_1_t", "sub_obj", object->sub_obj, 1, func_call_test_complex1_set);

	redis_operating_exec(handle->pool, handle);
	ret = handle->id;
END:
	return ret;
}

int test_timeheap()
{
	osip_ring_t *ret_ring = NULL;
	apr_pool_t *pool = NULL;
	osip_ring_iterator_t *it = NULL;
	apr_pool_create(&pool, NULL);
	void *obj = NULL;
	int iCount = 0;

	while(1)
	{
		redis_get_class_timerheap(pool, "test_timeheap", apr_time_now(), &ret_ring);
		if(ret_ring)
		{
			break;
		}
		iCount++;
		printf("iCount:%d\n", iCount);
	}

	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(ret_ring, it); obj; obj = osip_ring_get_next(it))
	{
		printf("%s\n", (char *)obj);
	}
	printf("NULL!!!\n");
	osip_ring_destroy_iterator(it);
	return 0;
}

test_complex_2_t* test_complex_2_create(apr_pool_t *pool, int count_2, char *str_2, apr_time_t timer_2)
{
	test_complex_2_t *result = apr_pcalloc(pool, sizeof(test_complex_2_t));
	result->count_2 = count_2;
	result->str_2 = apr_pstrdup(pool, str_2);
	result->timer_2 = timer_2;

	return result;
}

int test_proxy_lock_set()
{
	apr_pool_t *pool = NULL;
	apr_pool_create(&pool, NULL);
	yaproxy_lock_t *obj = apr_pcalloc(pool, sizeof(yaproxy_lock_t));
	
	obj->pool = pool;
	obj->clientID = apr_pstrdup(pool, "12345678901234567891");
	obj->ipcGbID = apr_pstrdup(pool, "12345678901234567892");
	obj->level = 123;
	obj->timer = yasips_misc_str_totime_byformat(apr_time_now());
	obj->sub_obj = apr_pcalloc(pool, sizeof(yaproxy_lock_t));
	obj->sub_obj->count_1 = 1;
	obj->sub_obj->str_1 = apr_pstrdup(pool, "test001");
	obj->sub_obj->timer_1 = apr_time_now();
	
	osip_ring_create(pool, &obj->sub_obj->ring_complex_2);
	osip_ring_add(obj->sub_obj->ring_complex_2, test_complex_2_create(pool, 1, "c1", apr_time_now()), -1);
	osip_ring_add(obj->sub_obj->ring_complex_2, test_complex_2_create(pool, 2, "c2", apr_time_now()), -1);
	osip_ring_add(obj->sub_obj->ring_complex_2, test_complex_2_create(pool, 3, "c3", apr_time_now()), -1);

	func_call_yaproxy_lock_set(pool, "yaproxy_lock_t", (void *)obj);

	return 0;
}

void* func_call_test_complex2_get(apr_pool_t *pool, char *key)
{
	test_complex_2_t *result = apr_pcalloc(pool, sizeof(test_complex_2_t));
	result->count_2 = atoi(db_get_value_string_and_time(pool, key, "count_2"));
	result->str_2 = db_get_value_string_and_time(pool, key, "str_2");
	result->timer_2 = atol(db_get_value_string_and_time(pool, key, "timer_2"));

	return result;
}

void* func_call_test_complex1_get(apr_pool_t *pool, char *key)
{
	test_complex_1_t *result = apr_pcalloc(pool, sizeof(test_complex_1_t));
	result->count_1 = atoi(db_get_value_string_and_time(pool, key, "count_1"));
	result->str_1 = db_get_value_string_and_time(pool, key, "str_1");
	result->timer_1 = atol(db_get_value_string_and_time(pool, key, "timer_1"));
	result->ring_complex_2 = db_get_value_list(pool, key, "test_complex_2_t", "ring_complex_2", func_call_test_complex2_get);
	return result;
}

void* func_call_yaproxy_lock_get(apr_pool_t *pool, char *key)
{
	yaproxy_lock_t *result = apr_pcalloc(pool, sizeof(yaproxy_lock_t));

	result->clientID = db_get_value_string_and_time(pool, key, "clientID");
	result->ipcGbID = db_get_value_string_and_time(pool, key, "ipcGbID");
	result->level = atoi(db_get_value_string_and_time(pool, key, "level"));
	result->timer = atol(db_get_value_string_and_time(pool, key, "timer"));
	result->sub_obj = db_get_value_reference(pool, key, "sub_obj", func_call_test_complex1_get);

END:
	return result;
}

int test_proxy_lock_get()
{
	int ret = 0;
	apr_pool_t *pool = NULL;
	apr_pool_create(&pool, NULL);
	test_complex_2_t *obj = NULL;
	osip_ring_t *ring = NULL;
	osip_ring_iterator_t *it =NULL;

	ring = redis_get_class_id(pool, "yaproxy_lock_t", "clientID", "12345678901234567891", NULL);
	if(1 != osip_ring_size(ring))
	{
		ret = -1;
		goto END;
	}

	char *key = (char *)osip_ring_get(ring, 0);
	key = apr_pstrcat(pool, "yaproxy_lock_t:", key, NULL);
	yaproxy_lock_t *result = func_call_yaproxy_lock_get(pool, key);

	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(result->sub_obj->ring_complex_2, it); obj; obj = osip_ring_get_next(it))
	{
		printf("obj:%s\n", obj->str_2);
	}
	osip_ring_destroy_iterator(it);

END:
	return ret;
}

int func_call_test_complex2_del(apr_pool_t *pool, char *key)
{
	int ret = 0;

	db_delete_string_member(pool, key, "count_2");
	db_delete_string_member(pool, key, "str_2");
	db_delete_timer_member(pool, key, "test_timeheap", "timer_2");
	db_delete_other_element(pool, key);

	return ret;
}

int func_call_test_complex1_del(apr_pool_t *pool, char *key)
{
	int ret = 0;

	db_delete_string_member(pool, key, "count_1");
	db_delete_string_member(pool, key, "str_1");
	db_delete_timer_member(pool, key, "test_timeheap", "timer_1");
	db_delete_list_member(pool, key, "ring_complex_2", func_call_test_complex2_del);
	db_delete_other_element(pool, key);

	return ret;
}

int func_call_yaproxy_lock_del(apr_pool_t *pool, char *key)
{
	int ret = 0;

	db_delete_string_member(pool, key, "clientID");
	db_delete_string_member(pool, key, "ipcGbID");
	db_delete_string_member(pool, key, "level");
	db_delete_timer_member(pool, key, "test_timeheap", "timer");
	db_delete_reference_member(pool, key, "sub_obj", func_call_test_complex1_del);
	db_delete_other_element(pool, key);

	return ret;
}

int test_proxy_lock_delete()
{
	int ret = 0;
	apr_pool_t *pool = NULL;
	apr_pool_create(&pool, NULL);
	test_complex_2_t *obj = NULL;
	osip_ring_t *ring = NULL;
	osip_ring_iterator_t *it =NULL;

	ring = redis_get_class_id(pool, "yaproxy_lock_t", "clientID", "12345678901234567891", NULL);
	if(1 != osip_ring_size(ring))
	{
		ret = -1;
		goto END;
	}

	char *key = (char *)osip_ring_get(ring, 0);
	key = apr_pstrcat(pool, "yaproxy_lock_t:", key, NULL);
	func_call_yaproxy_lock_del(pool, key);
END:
	return 0;
}

int test_proxy_lock_update()
{
	apr_pool_t *pool = NULL;
	apr_pool_create(&pool, NULL);
	yaproxy_lock_t *obj = apr_pcalloc(pool, sizeof(yaproxy_lock_t));
	osip_ring_t *ring = NULL;
	osip_ring_iterator_t *it =NULL;
	int ret = 0;

	ring = redis_get_class_id(pool, "yaproxy_lock_t", "clientID", "12345678901234567891", NULL);
	if(1 != osip_ring_size(ring))
	{
		ret = -1;
		goto END;
	}

	char *key = (char *)osip_ring_get(ring, 0);
	key = apr_pstrcat(pool, "yaproxy_lock_t:", key, NULL);

	obj->pool = pool;
	obj->clientID = apr_pstrdup(pool, "xxx45678901234567891");
	obj->ipcGbID = apr_pstrdup(pool, "xxx45678901234567892");
	obj->level = 456;
	obj->timer = yasips_misc_str_totime_byformat(apr_time_now());
	obj->sub_obj = apr_pcalloc(pool, sizeof(test_complex_1_t));
	obj->sub_obj->count_1 = 1;
	obj->sub_obj->str_1 = apr_pstrdup(pool, "test001");
	obj->sub_obj->timer_1 = apr_time_now();
	
	osip_ring_create(pool, &obj->sub_obj->ring_complex_2);
	osip_ring_add(obj->sub_obj->ring_complex_2, test_complex_2_create(pool, 4, "c4", apr_time_now()), -1);
	osip_ring_add(obj->sub_obj->ring_complex_2, test_complex_2_create(pool, 5, "c5", apr_time_now()), -1);
	osip_ring_add(obj->sub_obj->ring_complex_2, test_complex_2_create(pool, 6, "c6", apr_time_now()), -1);

	func_call_yaproxy_lock_update(pool, key, (void *)obj);

END:
	return ret;
}

/*int func_call_test_complex2_update(apr_pool_t *pool, char *key, void *new_value)
{
	test_complex_2_t *obj = (test_complex_2_t *)new_value;
	db_update_string_member(pool, key, "count_2", apr_psprintf(pool, "%d", obj->count_2));
	db_update_string_member(pool, key, "str_2", obj->str_2);
	db_update_timer_member(pool, key, "test_timeheap", "timer_2", obj->timer_2);

	return 0;
}*/

int func_call_test_complex1_update(apr_pool_t *pool, char *key, void *new_value)
{
	test_complex_1_t *obj = (test_complex_1_t *)new_value;

	db_update_string_member(pool, key, "count_1", apr_psprintf(pool, "%d", obj->count_1));
	db_update_string_member(pool, key, "str_1", obj->str_1);
	db_update_timer_member(pool, key, "test_timeheap", "timer_1", obj->timer_1);
	db_update_list_member(pool, key, "test_complex_2_t", "ring_complex_2", obj->ring_complex_2, 
						  func_call_test_complex2_del, func_call_test_complex2_set);

	return 0;
}

int func_call_yaproxy_lock_update(apr_pool_t *pool, char *key, void *new_value)
{
	yaproxy_lock_t *obj = (yaproxy_lock_t *)new_value;

	db_update_string_member(pool, key, "clientID", obj->clientID);
	db_update_string_member(pool, key, "ipcGbID", obj->ipcGbID);
	db_update_string_member(pool, key, "level", apr_psprintf(pool, "%d", obj->level));
	db_update_timer_member(pool, key, "test_timeheap", "timer", obj->timer);
	db_update_reference_member(pool, key, "sub_obj", (void *)obj->sub_obj, func_call_test_complex1_del, func_call_test_complex1_update);

	return 0;
}
