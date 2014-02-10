#include "redis_operating.h"
#include <apr_pools.h>
#include <apr_strings.h>

redisContext *__redis_operating_connect(const char *ip, int port, struct timeval *tv)
{
	int ret = -1;
    redisContext *gClient = NULL;

	if(NULL == ip)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	if(tv)
		gClient = redisConnectWithTimeout(ip, port, *tv);
	else
		gClient = redisConnect(ip, port);

	if ((NULL == gClient)||(gClient->err))
	{
		printf("Info.Connect to redis failed.[%s:%d]:gClient->err:%s\n",__FILE__, __LINE__, gClient->errstr);
        ret = -1;
        goto END;
	}
	ret = 0;

END:
	if(ret != 0 && NULL != gClient)
	{
		redisFree(gClient);
		gClient = NULL;
	}
	return gClient;
}

int __redis_operating_execution(redisContext* gClient, char *strcmd, apr_pool_t *pool, redis_reply_result_t *reply_result)
{
	int ret = -1;
	redisReply* reply = NULL;

	if(NULL == gClient || NULL == strcmd)
	{
		printf("Error.redis operating execution failed.[%s:%d]\n",__FILE__, __LINE__);
        ret = -1;
        goto END;
	}

	reply = (redisReply*)redisCommand(gClient, strcmd);
    if (NULL == reply) 
    {
		printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
        ret = -1;
        goto END;
    }

	if(REDIS_REPLY_ERROR == reply->type)
	{
		ret = 0;
		if(pool && reply_result) 
		{
			reply_result->result = apr_pstrdup(pool, "error");
			reply_result->type = REDIS_REPLY_ERROR;
		}
	}
	else if(REDIS_REPLY_INTEGER == reply->type)
	{
		ret = 0;
		if(pool && reply_result) 
		{
			reply_result->result = apr_psprintf(pool, "%ld", reply->integer);
			reply_result->type = REDIS_REPLY_INTEGER;
		}
	}
	else if(REDIS_REPLY_NIL == reply->type)
	{
		ret = 0;
		if(pool && reply_result) 
		{
			reply_result->result = apr_pstrdup(pool, "nil");
			reply_result->type = REDIS_REPLY_NIL;
		}
	}
	else if(REDIS_REPLY_STATUS == reply->type)
	{
		ret = 0;
		if(pool && reply_result) 
		{
			reply_result->result = apr_pstrdup(pool, reply->str);
			reply_result->type = REDIS_REPLY_STATUS;
		}
	}
	else if(REDIS_REPLY_STRING == reply->type)
	{
		ret = 0;
		if(pool && reply_result) 
		{
			reply_result->result = apr_pstrdup(pool, reply->str);
			reply_result->type = REDIS_REPLY_STRING;
		}
	}
	else if(REDIS_REPLY_ARRAY == reply->type)
	{
		ret = 0;
		if(pool && reply_result) 
		{
			reply_result->result = apr_pstrdup(pool, "Array");
			reply_result->type = REDIS_REPLY_ARRAY;
		}
	}
	else
	{
		ret = -1;
	}

END:
    if (NULL != reply)
    {
        freeReplyObject(reply);
    }
	return ret;
}

osip_ring_t* __redis_operating_reader(apr_pool_t *pool, redisContext* gClient, char *strcmd)
{
	redisReply* reply = NULL;
	osip_ring_t *ret_ring = NULL;
	int ret = -1;
	char *add_str = NULL;

	reply = (redisReply*)redisCommand(gClient, strcmd);
    if (NULL == reply) 
    {
		printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
        ret = -1;
        goto END;
    }
	else
	{
		if (reply->type == REDIS_REPLY_ARRAY) 
		{
			int j = 0;
			if(0 != osip_ring_create(pool, &ret_ring))
			{
				printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
				ret = -1;
				goto END;
			}
			for (j = 0; j < reply->elements; j++) {
				//osip_ring_add(ret_ring, reply->element[j]->str, -1);
				if(REDIS_REPLY_ERROR == reply->element[j]->type)
				{
					ret = 0;
					osip_ring_add(ret_ring, "error", -1);
				}
				else if(REDIS_REPLY_INTEGER == reply->element[j]->type)
				{
					ret = 0;add_str = apr_psprintf(pool, "%ld", reply->element[j]->integer);
					osip_ring_add(ret_ring, add_str, -1);
				}
				else if(REDIS_REPLY_NIL == reply->element[j]->type)
				{
					ret = 0;
					osip_ring_add(ret_ring, "nil", -1);
				}
				else if(REDIS_REPLY_STATUS == reply->element[j]->type)
				{
					ret = 0;add_str = apr_psprintf(pool, "%s", reply->element[j]->str);
				    osip_ring_add(ret_ring, add_str, -1);
				}
				else if(REDIS_REPLY_STRING == reply->element[j]->type)
				{
					ret = 0;add_str = apr_psprintf(pool, "%s", reply->element[j]->str);
				    osip_ring_add(ret_ring, add_str, -1);
				}
				else
				{
					ret = -1;
					goto END;
				}
			}
		}
		else
		{
			printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
			ret = -1;
			goto END;
		}
	}
	ret = 0;

END:
    if (NULL != reply)
    {
        freeReplyObject(reply);
    }
	if (ret != 0 && ret_ring != NULL)
	{
		void *obj = NULL;
		osip_ring_iterator_t *it = NULL;

		osip_ring_create_iterator(pool, &it);
	    for(obj = osip_ring_get_first(ret_ring, it); obj; obj = osip_ring_get_next(it))
		{
			osip_ring_iterator_remove(it);
		}
	    osip_ring_destroy_iterator(it);
		return NULL;
	}
	return ret_ring;
}

int __redis_operating_execution_multi(apr_pool_t *pool, redisContext* gClient, osip_ring_t* ring)
{
	int ret = -1;
	redisReply* reply = NULL;
	osip_ring_iterator_t *it = NULL;
	void *obj = NULL;
	char *strcmd = NULL;

	if(NULL == gClient)
	{
		printf("redis operating executionargs by multi!");
		ret = -1;
		goto END;
	}

	reply = (redisReply*)redisCommand(gClient, "MULTI");
    if (NULL == reply) 
    {
		printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
        ret = -1;
        goto END;
    }
	freeReplyObject(reply);reply = NULL;

	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(ring, it); obj; obj = osip_ring_get_next(it))
	{
		strcmd = (char *)obj;
		if(__redis_operating_execution(gClient, strcmd, NULL, NULL) != 0)
		{
			printf("Info.redis command exec failed:%s.[%s:%d]\n", strcmd, __FILE__, __LINE__);
			ret = -1;
			reply = (redisReply*)redisCommand(gClient, "DISCARD");
			goto END;
		}
	}
	osip_ring_destroy_iterator(it);

	reply = (redisReply*)redisCommand(gClient, "EXEC");
    if (NULL == reply) 
    {
		printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
        ret = -1;
        goto END;
    }

END:
	if(!reply) freeReplyObject(reply);
	return ret;
}

apr_hash_t* __redis_operating_reader_multi(redisContext* gClient, apr_pool_t *pool, osip_ring_t* ring)
{
	int ret = -1;
	apr_hash_t *hash = NULL;
	osip_ring_t *tmp_ring = NULL;
	osip_ring_iterator_t *it = NULL;
	redisReply* reply = NULL;
	void *obj = NULL;
	char *strcmd = NULL;

	if(ring == NULL)
	{
		printf("redis operating executionargs by multi!");
		ret = -1;
		goto END;
	}

	hash = apr_hash_make(pool);

	reply = (redisReply*)redisCommand(gClient, "MULTI");
    if (NULL == reply) 
    {
		printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
        ret = -1;
        goto END;
    }
	freeReplyObject(reply);reply = NULL;

	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(ring, it); obj; obj = osip_ring_get_next(it))
	{
		strcmd = (char *)obj;
		tmp_ring = __redis_operating_reader(pool, gClient, strcmd);
		if(tmp_ring == NULL)
		{
			printf("Info.redis command exec failed:%s.[%s:%d]\n", strcmd, __FILE__, __LINE__);
			ret = -1;
			goto END;
		}
		apr_hash_set(hash, strcmd, APR_HASH_KEY_STRING, tmp_ring);
	}
	osip_ring_destroy_iterator(it);

	reply = (redisReply*)redisCommand(gClient, "EXEC");
    if (NULL == reply) 
    {
		printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
        ret = -1;
        goto END;
    }
	ret = 0;

END:
	if(!reply) freeReplyObject(reply);
	if(ret != 0 && hash != NULL)
	{
		apr_hash_clear(hash);
		return NULL;
	}
	return hash;
}

int redis_operating_hset(apr_pool_t *pool, redis_operating_t *handle, char *key, char *field, char *value)
{
	char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;
	redisContext* gClient = NULL;

	if(NULL == key || NULL == pool || NULL == field || NULL == value)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd = apr_psprintf(pool, "HSET %s %s %s", key, field, value);
	
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(handle->connect, strcmd, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	ret = 0;

END:
	return ret;

}

int redis_operating_hmset(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, apr_hash_t *hash, char *str_val)
{
	//char *strcmd = NULL;
	int ret = -1;
	apr_hash_index_t *hi = NULL;
    char *hash_val = NULL;
	char *hash_key = NULL;
	redis_reply_result_t *reply_result = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(subpool1, "HMSET %s ", key);
	if(NULL == str_val)
	{
		for (hi = apr_hash_first(pool, hash); hi; hi = apr_hash_next(hi)) 
		{
			apr_hash_this(hi, &hash_key, NULL, &hash_val);
			if (hash_val && hash_key)
			{
				apr_pool_create(&subpool2, pool);
				strcmd->string = apr_pstrcat(subpool2, strcmd->string, " ", (char *)hash_key, " ", (char *)hash_val, NULL);
				apr_pool_destroy(strcmd->pool);
				strcmd->pool = subpool2;
			}
			else
			{
				printf("Info.Connect to redis failed.[%s:%d]\n", __FILE__, __LINE__);
				ret = -1;
				goto END;
			}
		}
	}
	else
	{
		strcmd->string = apr_pstrcat(strcmd->pool, strcmd->string, " ", str_val, " ", NULL);
	}

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd->string, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

int redis_operating_hget(apr_pool_t *pool, redis_operating_t *handle, char *key, char *field, char **value)
{
	char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;
	redis_operating_t *sub_handle = redis_operating_nomutli_init(pool, handle->type, handle->id);

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd = apr_psprintf(pool, "HGET %s %s", key, field);
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(sub_handle->connect, strcmd, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	*value = apr_pstrdup(pool, reply_result->result);
	ret = 0;

END:
    if (sub_handle)
    {
        apr_pool_cleanup_run(sub_handle->pool, sub_handle, __cleanup_redis_operating);
	}
	return ret;

}

int redis_operating_hmget(apr_pool_t *pool, const char *ip, int port, 
						  struct timeval tv,char *key,  osip_ring_t *key_ring, osip_ring_t **val_ring)
{
	//char *strcmd = NULL;
	int ret = -1;
	apr_hash_index_t *hi = NULL;
	void *val = NULL;
	osip_ring_iterator_t *it = NULL;
	void *obj = NULL;
	osip_ring_t *tmp_ring = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key_ring || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(subpool1, "HMGET %s", key);
	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(key_ring, it); obj; obj = osip_ring_get_next(it))
	{
		apr_pool_create(&subpool2, pool);
		strcmd->string = apr_pstrcat(subpool2, strcmd->string, " ", (char *)obj, NULL);
		apr_pool_destroy(strcmd->pool);
		strcmd->pool = subpool2;
	}
	osip_ring_destroy_iterator(it);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	tmp_ring = __redis_operating_reader(pool, gClient, strcmd->string);
	if(NULL == tmp_ring)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	*val_ring = tmp_ring;
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}


int redis_operating_hgetall(apr_pool_t *pool, const char *ip, int port, 
						  struct timeval tv,char *key, osip_ring_t **val_ring)
{
	//char *strcmd = NULL;
	int ret = -1;
	apr_hash_index_t *hi = NULL;
	void *val = NULL;
	osip_ring_iterator_t *it = NULL;
	void *obj = NULL;
	osip_ring_t *tmp_ring = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(subpool1, "HGETALL %s", key);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	tmp_ring = __redis_operating_reader(pool, gClient, strcmd->string);
	if(NULL == tmp_ring)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	*val_ring = tmp_ring;
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

int redis_operating_hdel(apr_pool_t *pool, redis_operating_t *handle, char *key, char *field)
{
	char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd = apr_psprintf(pool, "HDEL %s %s", key, field);
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(handle->connect, strcmd, pool, reply_result);

	if(ret == 0 && (3 == reply_result->type || (5 == reply_result->type && 0 == strcasecmp(reply_result->result, "QUEUED"))))
		ret = 0;
	else
		ret = -1;

END:
    if (0 != ret)
    {
        apr_pool_cleanup_run(handle->pool, handle, __cleanup_redis_operating);
	}
	return ret;
}

int redis_operating_set(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, char *value, int ex_flag)
{
	char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;

	if(NULL == key || NULL == pool || NULL == value)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	
	if(0 == ex_flag)
		strcmd = apr_psprintf(pool, "SET %s %s NX", key, value);
	else if(1 == ex_flag)
		strcmd = apr_psprintf(pool, "SET %s %s XX", key, value);
	else
		strcmd = apr_psprintf(pool, "SET %s %s", key, value);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	
	ret = __redis_operating_execution(gClient, strcmd, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;

}

int redis_operating_mset(apr_pool_t *pool, const char *ip, int port, struct timeval tv, apr_hash_t *hash)
{
	//char *strcmd = NULL;
	int ret = -1;
    char *hash_val = NULL;
	char *hash_key = NULL;
	redis_reply_result_t *reply_result = NULL;
	void *obj = NULL;
	osip_ring_iterator_t *it = NULL;
	apr_hash_index_t *hi = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_pstrdup(subpool1, "MSET ");
    for (hi = apr_hash_first(pool, hash); hi; hi = apr_hash_next(hi)) 
    {
        apr_hash_this(hi, &hash_key, NULL, &hash_val);
        if (hash_val && hash_key)
		{
			apr_pool_create(&subpool2, pool);
			strcmd->string = apr_pstrcat(subpool2, strcmd->string, " ", (char *)hash_key, " ", (char *)hash_val, NULL);
			apr_pool_destroy(strcmd->pool);
			strcmd->pool = subpool2;
		}
		else
		{
			printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
			ret = -1;
			goto END;
		}
	}

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd->string, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

int redis_operating_get(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, char **value)
{
	char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd = apr_psprintf(pool, "GET %s", key);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n", __FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	*value = apr_pstrdup(pool, reply_result->result);
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;

}

int redis_operating_mget(apr_pool_t *pool, const char *ip, int port, 
						  struct timeval tv, osip_ring_t *key_ring, osip_ring_t **val_ring)
{
	//char *strcmd = NULL;
	int ret = -1;
	apr_hash_index_t *hi = NULL;
	void *val = NULL;
	osip_ring_iterator_t *it = NULL;
	void *obj = NULL;
	osip_ring_t *tmp_ring = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key_ring || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(subpool1, "MGET ");
	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(key_ring, it); obj; obj = osip_ring_get_next(it))
	{
		//strcmd = apr_pstrcat(pool, strcmd, " ", (char *)obj, NULL);
		apr_pool_create(&subpool2, pool);
		strcmd->string = apr_pstrcat(subpool2, strcmd->string, " ", (char *)obj, NULL);
		apr_pool_destroy(strcmd->pool);
		strcmd->pool = subpool2;
	}
	osip_ring_destroy_iterator(it);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	tmp_ring = __redis_operating_reader(pool, gClient, strcmd->string);
	if(NULL == tmp_ring)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	*val_ring = tmp_ring;
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

int redis_operating_keys(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, osip_ring_t **val_ring)
{
	char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;
	osip_ring_t *ret_ring = NULL;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd = apr_psprintf(pool, "KEYS %s", key);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	ret_ring = __redis_operating_reader(pool, gClient, strcmd);
	if(NULL == ret_ring)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	if(ret != 0) *val_ring = NULL;
	*val_ring = ret_ring;
	return ret;
}

int redis_operating_exists(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key)
{
	char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd = apr_psprintf(pool, "EXISTS %s", key);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd, pool, reply_result);
	if(0 != ret)
	{
		ret = -1;
		goto END;
	}

	if(3 != reply_result->type || 0 != strcasecmp(reply_result->result, "1"))
	{
		ret = -1;
		goto END;
	}
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

int redis_operating_sadd(apr_pool_t *pool, redis_operating_t *handle, char *key, osip_ring_t *ring, char *str_val)
{
	//char *strcmd = NULL;
	int ret = -1;
	osip_ring_iterator_t *it = NULL;
	redis_reply_result_t *reply_result = NULL;
	void *obj = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(pool, "SADD %s ", key);

	if(NULL == str_val)
	{
		osip_ring_create_iterator(pool, &it);
		for(obj = osip_ring_get_first(ring, it); obj; obj = osip_ring_get_next(it))
		{
			apr_pool_create(&subpool2, pool);
			strcmd->string = apr_pstrcat(pool, strcmd->string, " ", (char *)obj, " ", NULL);
			apr_pool_destroy(strcmd->pool);
			strcmd->pool = subpool2;
		}
		osip_ring_destroy_iterator(it);
	}
	else
	{
		strcmd->string = apr_pstrcat(pool, strcmd->string, " ", str_val, " ", NULL);
	}

	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(handle->connect, strcmd->string, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	if(reply_result && (3 == reply_result->type || (5 == reply_result->type && 0 == strcasecmp(reply_result->result, "QUEUED"))))
		ret = 0;
	else
		ret = -1;

END:
	return ret;
}

int redis_operating_srem(apr_pool_t *pool, redis_operating_t *handle, char *key, osip_ring_t *ring, char *str_val)
{
	//char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;
	void *obj = NULL;
	osip_ring_iterator_t * it = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(subpool1, "SREM %s ", key);

	if(NULL == str_val)
	{
		osip_ring_create_iterator(pool, &it);
		for(obj = osip_ring_get_first(ring, it); obj; obj = osip_ring_get_next(it))
		{
			apr_pool_create(&subpool2, pool);
			strcmd->string = apr_pstrcat(subpool2, strcmd->string, " ", (char *)obj, " ", NULL);
			apr_pool_destroy(strcmd->pool);
			strcmd->pool = subpool2;
		}
		osip_ring_destroy_iterator(it);
	}
	else
	{
		strcmd->string = apr_pstrcat(pool, strcmd->string, " ", str_val, " ", NULL);
	}
	
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(handle->connect, strcmd->string, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

		if(reply_result && (3 == reply_result->type || (5 == reply_result->type && 0 == strcasecmp(reply_result->result, "QUEUED"))))
		ret = 0;
	else
		ret = -1;

END:
	apr_pool_destroy(strcmd->pool);
	return ret;
}

int redis_operating_scard(apr_pool_t *pool, redis_operating_t *handle, char *key)
{
	//char *strcmd = NULL;
	int ret = -1;
	osip_ring_iterator_t *it = NULL;
	redis_reply_result_t *reply_result = NULL;
	void *obj = NULL;
	redis_operating_t *sub_handle = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(pool, "SCARD %s ", key);

	sub_handle = redis_operating_nomutli_init(pool, handle->type, handle->id);

	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(sub_handle->connect, strcmd->string, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	if(reply_result && 3 == reply_result->type)
		ret = atoi(reply_result->result);
	else
		ret = -1;

END:
    if(sub_handle)
	{
		apr_pool_cleanup_run(sub_handle->pool, sub_handle, __cleanup_redis_operating);
	}
	apr_pool_destroy(strcmd->pool);
	return ret;
}

int redis_operating_smembers(apr_pool_t *pool, redis_operating_t *handle, char *key, osip_ring_t **val_ring)
{
	//char *strcmd = NULL;
	int ret = -1;
	apr_hash_index_t *hi = NULL;
	void *val = NULL;
	osip_ring_iterator_t *it = NULL;
	void *obj = NULL;
	osip_ring_t *tmp_ring = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	redis_operating_t* sub_handle = redis_operating_nomutli_init(pool, handle->type, handle->id);

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(subpool1, "SMEMBERS %s", key);

	tmp_ring = __redis_operating_reader(pool, sub_handle->connect, strcmd->string);
	if(NULL == tmp_ring)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	*val_ring = tmp_ring;
	ret = 0;

END:
    if(sub_handle)
	{
		apr_pool_cleanup_run(sub_handle->pool, sub_handle, __cleanup_redis_operating);
	}
	apr_pool_destroy(strcmd->pool);
	return ret;
}


int redis_operating_sismember(apr_pool_t *pool, redis_operating_t *handle, char *key, char *val)
{
	char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;
	redis_operating_t *sub_handle = NULL;

	if(NULL == key || NULL == pool)
	{
		ret = -1;
		goto END;
	}

	strcmd = apr_psprintf(pool, "SISMEMBER %s %s", key, val);

	sub_handle = redis_operating_nomutli_init(pool, handle->type, handle->id);

	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(sub_handle->connect, strcmd, pool, reply_result);
	if(0 != ret)
	{
		ret = -1;
		goto END;
	}

	if(3 != reply_result->type || 0 != strcasecmp(reply_result->result, "1"))
	{
		ret = -1;
		goto END;
	}
	ret = 0;

END:
	if(sub_handle)
	{
		apr_pool_cleanup_run(sub_handle->pool, sub_handle, __cleanup_redis_operating);
	}
	return ret;
}

int redis_operating_type(apr_pool_t *pool, redis_operating_t *handle, char *key)
{
	char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;
	redis_operating_t *sub_handle = NULL;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd = apr_psprintf(pool, "TYPE %s", key);

	sub_handle = redis_operating_nomutli_init(pool, handle->type, handle->id);

	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(sub_handle->connect, strcmd, pool, reply_result);
	if(0 != ret)
	{
		ret = -1;
		goto END;
	}

	if(5 != reply_result->type || 0 != strcasecmp(reply_result->result, "set"))
	{
		ret = -1;
		goto END;
	}
	ret = 0;

END:
	if (sub_handle)
	{
		apr_pool_cleanup_run(sub_handle->pool, sub_handle, __cleanup_redis_operating);
	}
	return ret;
}

int redis_operating_del(apr_pool_t *pool, redis_operating_t *handle, char *key)
{
	char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd = apr_psprintf(pool, "DEL %s", key);

	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(handle->connect, strcmd, pool, reply_result);
	if(0 != ret)
	{
		ret = -1;
		goto END;
	}

	if(reply_result && (1 == reply_result->type || (5 == reply_result->type && 0 == strcasecmp(reply_result->result, "QUEUED"))))
		ret = 0;
	else
		ret = -1;

END:
	return ret;
}

int redis_operating_sinter(apr_pool_t *pool, redis_operating_t *handle, osip_ring_t *key_ring, osip_ring_t **val_ring)
{
	int ret = -1;
	apr_hash_index_t *hi = NULL;
	void *val = NULL;
	osip_ring_iterator_t *it = NULL;
	void *obj = NULL;
	osip_ring_t *tmp_ring = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));
	redis_operating_t *sub_handle = NULL;

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key_ring || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(subpool1, "SINTER ");
	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(key_ring, it); obj; obj = osip_ring_get_next(it))
	{
		apr_pool_create(&subpool2, pool);
		sub_handle = redis_operating_nomutli_init(pool, handle->type, handle->id);
		if(NULL == sub_handle) 
		{
			ret = -1;
			goto END;
		}

		if(redis_operating_type(pool, sub_handle, (char *)obj) != -1)
			strcmd->string = apr_pstrcat(subpool2, strcmd->string, " ", (char *)obj, NULL);
		else
		{
			printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
			apr_pool_destroy(subpool2);
			ret = -1;
			goto END;
		}

		if (sub_handle)
		{
			apr_pool_cleanup_run(sub_handle->pool, sub_handle, __cleanup_redis_operating);
			sub_handle = NULL;
		}
		apr_pool_destroy(strcmd->pool);
		strcmd->pool = subpool2;
	}
	osip_ring_destroy_iterator(it);

	sub_handle = redis_operating_nomutli_init(pool, handle->type, handle->id);
	if(NULL == sub_handle) 
	{
		ret = -1;
		goto END;
	}

	tmp_ring = __redis_operating_reader(pool, sub_handle->connect, strcmd->string);
	if(NULL == tmp_ring)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	*val_ring = tmp_ring;
	ret = 0;

END:
    if (sub_handle)
    {
		apr_pool_cleanup_run(sub_handle->pool, sub_handle, __cleanup_redis_operating);
	}
	if(strcmd) apr_pool_destroy(strcmd->pool);
	return ret;
}

int redis_operating_zadd(apr_pool_t *pool, redis_operating_t *handle, char *key, osip_ring_t *ring, char *str_val)
{
	//char *strcmd = NULL;
	int ret = -1;
	osip_ring_iterator_t *it = NULL;
	redis_reply_result_t *reply_result = NULL;
	void *obj = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(pool, "ZADD %s ", key);

	if(NULL == str_val)
	{
		osip_ring_create_iterator(pool, &it);
		for(obj = osip_ring_get_first(ring, it); obj; obj = osip_ring_get_next(it))
		{
			apr_pool_create(&subpool2, pool);
			strcmd->string = apr_pstrcat(pool, strcmd->string, " ", (char *)obj, " ", NULL);
			apr_pool_destroy(strcmd->pool);
			strcmd->pool = subpool2;
		}
		osip_ring_destroy_iterator(it);
	}
	else
	{
		strcmd->string = apr_pstrcat(pool, strcmd->string, " ", str_val, " ", NULL);
	}

	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(handle->connect, strcmd->string, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	if(reply_result && (3 == reply_result->type || (5 == reply_result->type && 0 == strcasecmp(reply_result->result, "QUEUED"))))
		ret = 0;
	else
		ret = -1;

END:
	return ret;
}

int redis_operating_zrem(apr_pool_t *pool, redis_operating_t *handle, char *key, osip_ring_t *ring, char *str_val)
{
	//char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;
	void *obj = NULL;
	osip_ring_iterator_t * it = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(pool, "ZREM %s ", key);
	if(NULL == str_val)
	{
		osip_ring_create_iterator(pool, &it);
		for(obj = osip_ring_get_first(ring, it); obj; obj = osip_ring_get_next(it))
		{
			apr_pool_create(&subpool2, pool);
			strcmd->string = apr_pstrcat(subpool2, strcmd->string, " ", (char *)obj, " ", NULL);
			apr_pool_destroy(strcmd->pool);
			strcmd->pool = subpool2;
		}
		osip_ring_destroy_iterator(it);
	}
	else
	{
		strcmd->string = apr_pstrcat(strcmd->pool, strcmd->string, " ", str_val, " ", NULL);
	}
	
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(handle->connect, strcmd->string, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	if(reply_result && (3 == reply_result->type || (5 == reply_result->type && 0 == strcasecmp(reply_result->result, "QUEUED"))))
		ret = 0;
	else
		ret = -1;

END:
	apr_pool_destroy(strcmd->pool);
	return ret;
}

int redis_operating_zrangbyscore(apr_pool_t *pool, redis_operating_t *handle, char *key, char *min, char *max, osip_ring_t **val_ring)
{
	//char *strcmd = NULL;
	int ret = -1;
	apr_hash_index_t *hi = NULL;
	void *val = NULL;
	osip_ring_iterator_t *it = NULL;
	void *obj = NULL;
	osip_ring_t *tmp_ring = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));
	redis_operating_t *sub_handle = NULL;

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key || NULL == pool || NULL == min || NULL == max)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(subpool1, "ZRANGEBYSCORE %s %s %s", key, min, max);

	sub_handle = redis_operating_nomutli_init(pool, handle->type, handle->id);
	if(NULL == sub_handle)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	tmp_ring = __redis_operating_reader(pool, sub_handle->connect, strcmd->string);
	if(NULL == tmp_ring)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	*val_ring = tmp_ring;
	ret = 0;

END:
    if (sub_handle)
    {
		apr_pool_cleanup_run(sub_handle->pool, sub_handle, __cleanup_redis_operating);
	}
	apr_pool_destroy(strcmd->pool);
	return ret;
}

int redis_operating_zcard(apr_pool_t *pool, redis_operating_t *handle, char *key)
{
	//char *strcmd = NULL;
	int ret = -1;
	osip_ring_iterator_t *it = NULL;
	redis_reply_result_t *reply_result = NULL;
	void *obj = NULL;
	redis_operating_t *sub_handle = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(pool, "ZCARD %s ", key);

	sub_handle = redis_operating_nomutli_init(pool, handle->type, handle->id);

	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(sub_handle->connect, strcmd->string, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	if(reply_result && 3 == reply_result->type)
		ret = reply_result->result;
	else
		ret = -1;

END:
    if (sub_handle)
    {
		apr_pool_cleanup_run(sub_handle->pool, sub_handle, __cleanup_redis_operating);
	}
	apr_pool_destroy(strcmd->pool);
	return ret;
}

int redis_operating_incr(apr_pool_t *pool, redisContext* gClient, char *key, int increment)
{
	//char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;
	void *obj = NULL;
	osip_ring_iterator_t *it = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(pool, "INCRBY %s %d", key, increment);
	
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd->string, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	if(reply_result && 3 == reply_result->type)
	{
		int itmp = atoi(reply_result->result);
		if(itmp > 0) ret = itmp;
		else ret = -1;
	}
	else
		ret = -1;

END:
	return ret;
}

int redis_operating_watch(apr_pool_t *pool, redis_operating_t *handle, char *str_watch)
{
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;

	if(NULL == pool || NULL == handle)
	{
		ret = -1;
		goto END;
	}

	char *strcmd = apr_psprintf(pool, "WATCH %s", str_watch);
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(handle->connect, strcmd, pool, reply_result);
	if(ret != -1 && 5 == reply_result->type && 0 == strcasecmp("OK", reply_result->result))
	{
		ret = 0;
		goto END;
	}
	ret = -1;

END:
	if(ret != 0)
	{
		apr_pool_cleanup_run(handle->pool, handle, __cleanup_redis_operating);
	}
	return ret;
}

int redis_operating_mutli(apr_pool_t *pool, redis_operating_t *handle)
{
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;

	if(NULL == pool || NULL == handle)
	{
		ret = -1;
		goto END;
	}

	char *strcmd = apr_psprintf(pool, "MULTI");
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(handle->connect, strcmd, pool, reply_result);
	if(ret != -1 && 5 == reply_result->type && 0 == strcasecmp("OK", reply_result->result))
	{
		ret = 0;
		goto END;
	}
	ret = -1;

END:
	if(ret != 0)
	{
		apr_pool_cleanup_run(handle->pool, handle, __cleanup_redis_operating);
	}
	return ret;
}

int redis_operating_discard(apr_pool_t *pool, redis_operating_t *handle)
{
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;

	if(NULL == pool || NULL == handle)
	{
		ret = -1;
		goto END;
	}

	char *strcmd = apr_psprintf(pool, "DISCARD");
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(handle->connect, strcmd, pool, reply_result);
	if(ret != -1 && 5 == reply_result->type && 0 == strcasecmp("OK", reply_result->result))
	{
		ret = 0;
		goto END;
	}
	ret = -1;

END:
	if(ret != 0)
	{
		apr_pool_cleanup_run(handle->pool, handle, __cleanup_redis_operating);
	}
	return ret;
}

int redis_operating_exec(apr_pool_t *pool, redis_operating_t *handle)
{
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;

	if(NULL == pool || NULL == handle)
	{
		ret = -1;
		goto END;
	}

	char *strcmd = apr_psprintf(pool, "EXEC");
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(handle->connect, strcmd, pool, reply_result);
	if(ret != -1 && 4 != reply_result->type)
	{
		ret = 0;
		goto END;
	}
	ret = -1;

END:
	if(handle)
	{
		apr_pool_cleanup_run(handle->pool, handle, __cleanup_redis_operating);
	}
	return ret;
}

//business-common layer

char *__strIP = "127.0.0.1";
int __port = 6379;
struct timeval __timeout = { 1, 500000 }; // 1.5 seconds

int __redis_update_class_id(apr_pool_t *pool, char *type)
{
	int ret = -1;
	if(type) goto END;
	char *tmp = apr_psprintf(pool, "%s:id", type);
	redisContext *conn = __redis_operating_connect(__strIP, __port, &__timeout);
	if(redis_operating_incr(pool, conn, tmp, 1) != 0)
	{
		ret = -1;
		goto END;
	}
	ret = 0;

END:
	if(conn)
	{
		redisFree(conn);
	}
	return ret;
}

//char *local_id = redis_get_class_id(pool, "local", gbid, "12345678901234567890");
//

osip_ring_t* redis_get_class_id(apr_pool_t *pool, char *type, ...)
{
	va_list arg_ptr;
	int va_count = 0;
	redis_operating_t *handle = NULL;

	char *tmp = NULL;
	char *arg_set = NULL;

	osip_ring_t *arg_ring = NULL;
	osip_ring_t *ret_ring = NULL;

	if(0 != osip_ring_create(pool, &arg_ring))
	{
		printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
		ret_ring = NULL;
		goto END;
	}

	//偶数集合为对应的name，奇数集合为对应的value
	va_start(arg_ptr, type);
	do
	{
		tmp = va_arg(arg_ptr, char *);
		if(NULL == tmp) break;
		if(0 == va_count%2)
		{
			arg_set = apr_pstrdup(pool, tmp);
		}
		else
		{
			arg_set = apr_pstrcat(pool, type, ":", arg_set, ":", tmp, NULL);
			osip_ring_add(arg_ring, arg_set, -1);
			arg_set = NULL;
			va_count = 0;
		}
		va_count++;
	}while(tmp != NULL);
	va_end(arg_ptr);

	handle = redis_operating_create(pool, type, -1, -1);

	if(redis_operating_sinter(pool, handle, arg_ring, &ret_ring) != 0)
	{
		ret_ring = NULL;
		goto END;
	}

END:
	if (arg_ring != NULL)
	{
		void *obj = NULL;
		osip_ring_iterator_t *it = NULL;

		osip_ring_create_iterator(pool, &it);
	    for(obj = osip_ring_get_first(arg_ring, it); obj; obj = osip_ring_get_next(it))
		{
			osip_ring_iterator_remove(it);
		}
	    osip_ring_destroy_iterator(it);
	}
	return ret_ring;
}

int __redis_set_class_num(apr_pool_t *pool, redis_operating_t *handle, char *field, char *value)
{
	int ret = -1;
	char *key = NULL;

	if(NULL == field || NULL == value || NULL == handle)
	{
		ret = -1;
		goto END;
	}

	if(value)
	{
		key = apr_psprintf(pool, "%s:%d", handle->type, handle->id);
		if(redis_operating_hset(pool, handle, key, field, value) != -1)
		{
			ret = 0;
			goto END;
		}
	}
	ret = -1;

END:
	return ret;
}

int __redis_set_class_all(apr_pool_t *pool, redis_operating_t *handle)
{
	int ret = -1;
	char *key = NULL;
	char *val = NULL;

	if(NULL == pool || NULL == handle)
	{
		ret = -1;
		goto END;
	}

	key = apr_psprintf(pool, "%s:all", handle->type);
	val = apr_psprintf(pool, "%s:%d", handle->type, handle->id);
	redis_operating_sadd(pool, handle, key, NULL, val);

	ret = 0;
END:
	return ret;
}


int __redis_set_class_timerheap(apr_pool_t *pool, redis_operating_t *handle, char *key, char *val, apr_time_t timer)
{
	int ret = -1;
	char *tmp = NULL;

	if(NULL == pool || NULL == key || NULL == val)
	{
		ret = -1;
		goto END;
	}

	//redis_operating_t *handle = redis_operating_nowatch_init(pool, type, id);
	tmp = apr_psprintf(pool, "%ld %s", timer, val);
	redis_operating_zadd(pool, handle, key, NULL, tmp);
	//redis_operating_exec(handle->pool, handle);
	ret = 0;

END:
	return ret;
}

int redis_get_class_timerheap(apr_pool_t *pool, char *key, apr_time_t timer, osip_ring_t **result)
{
	int ret = -1;
	char *sorce = NULL;

	if(NULL == pool || NULL == key)
	{
		ret = -1;
		goto END;
	}

	char *type = NULL;
	char *id = NULL;
	char *p = NULL;
	
	type=  apr_strtok(key, ":", &p);
	id = apr_strtok(NULL, ":" ,&p);

	redis_operating_t *handle = redis_operating_create(pool, type, id, -1);
	if(NULL == handle)
	{
		*result = NULL;
		ret = -1;
		goto END;
	}

	sorce = apr_psprintf(pool, "%ld", timer);
	if(redis_operating_zrangbyscore(pool, handle, key, "-inf", sorce, result) != 0)
	{
		*result = NULL;
		ret = -1;
		goto END;
	}

	handle = redis_operating_nowatch_init(pool, type, id);
	if(result && redis_operating_zrem(pool, handle, key, *result, NULL) != 0)
	{
		*result = NULL;
		ret = -1;
		goto END;
	}
	redis_operating_exec(handle->pool, handle);
	ret = 0;

END:
	if(handle)
	{
		apr_pool_cleanup_run(handle->pool, handle, __cleanup_redis_operating);
	}
	return ret;
}

int __redis_set_class_list(apr_pool_t *pool, redis_operating_t *handle, char *member_name, char *member_val)
{
	int ret = -1;
	char *key = NULL;
	char *val = NULL;

	if(NULL == pool || NULL == handle)
	{
		ret = -1;
		goto END;
	}

	key = apr_psprintf(pool, "%s:%d:%s", handle->type, handle->id, member_name);
	val = member_val;
	redis_operating_sadd(pool, handle, key, NULL, val);
	
	ret = 0;
END:
	return ret;
}


int __redis_get_class_list(apr_pool_t *pool, redis_operating_t *handle, char *key, char *member_name, osip_ring_t **ret_ring)
{
	int ret = -1;
	char *val = NULL;

	if(NULL == pool || NULL == key)
	{
		ret = -1;
		goto END;
	}
	
	key = apr_psprintf(pool, "%s:%s", key, member_name);
	if(0 != redis_operating_smembers(pool, handle, key, ret_ring))
	{
		ret = -1;
		goto END;
	}
	
	ret = 0;
END:
	return ret;
}

int __redis_set_class_memberset(apr_pool_t *pool, redis_operating_t *handle, char *member_name, char *member_val)
{
	int ret = -1;
	char *key = NULL;
	char *val = NULL;

	if(NULL == pool || NULL == member_name || NULL == member_val)
	{
		ret = -1;
		goto END;
	}

	key = apr_psprintf(pool, "%s:%s:%s", handle->type, member_name, member_val);
	val = apr_psprintf(pool, "%d", handle->id);
	redis_operating_sadd(pool, handle, key, NULL, val);
	
	ret = 0;
END:
	return ret;
}

int __redis_set_class_indices(apr_pool_t *pool, redis_operating_t *handle, char *member_name, char *member_val)
{
	int ret = -1;
	char *key = NULL;
	char *val = NULL;

	if(NULL == pool || NULL == member_name || NULL == member_val)
	{
		ret = -1;
		goto END;
	}

	key = apr_psprintf(pool, "%s:%d:_indices", handle->type, handle->id);
	val = apr_psprintf(pool, "%s:%s:%s", handle->type, member_name, member_val);
	redis_operating_sadd(pool, handle, key, NULL, val);
	
	ret = 0;
END:
	return ret;
}

int __redis_set_class_zindices(apr_pool_t *pool, redis_operating_t *handle, char *member_name, char *member_val)
{
	int ret = -1;
	char *key = NULL;
	char *val = NULL;

	if(NULL == pool || NULL == member_name || NULL == member_val)
	{
		ret = -1;
		goto END;
	}

	key = apr_psprintf(pool, "%s:%d:_zindices", handle->type, handle->id);
	val = apr_psprintf(pool, "%s:%s:%s", handle->type, member_name, member_val);
	redis_operating_sadd(pool, handle, key, NULL, val);

	ret = 0;
END:
	return ret;
}

int redis_del_objects_bymember(apr_pool_t *pool, char *type, ...)
{
	va_list arg_ptr;
	int va_count = 0;

	char *tmp = NULL;
	char *arg_set = NULL;

	osip_ring_t *arg_ring = NULL;
	osip_ring_t *ret_ring = NULL;

	if(0 != osip_ring_create(pool, &arg_ring))
	{
		printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
		ret_ring = NULL;
		goto END;
	}

	//偶数集合为对应的name，奇数集合为对应的value
	va_start(arg_ptr, type);
	do
	{
		tmp = va_arg(arg_ptr, char *);
		if(NULL == tmp) break;
		if(0 == va_count%2)
		{
			arg_set = apr_pstrdup(pool, tmp);
		}
		else
		{
			arg_set = apr_pstrcat(pool, type, ":", arg_set, ":", tmp, NULL);
			osip_ring_add(arg_ring, arg_set, -1);
			arg_set = NULL;
			va_count = 0;
		}
		va_count++;
	}while(tmp != NULL);
	va_end(arg_ptr);

	redis_operating_t *handle = redis_operating_create(pool, type, -1, -1);

	if(redis_operating_sinter(pool, handle, arg_ring, &ret_ring) != 0)
	{
		ret_ring = NULL;
		goto END;
	}

END:
	if (arg_ring != NULL)
	{
		void *obj = NULL;
		osip_ring_iterator_t *it = NULL;

		osip_ring_create_iterator(pool, &it);
	    for(obj = osip_ring_get_first(arg_ring, it); obj; obj = osip_ring_get_next(it))
		{
			osip_ring_iterator_remove(it);
		}
	    osip_ring_destroy_iterator(it);
	}
	return ret_ring;
}

int redis_del_single_object_byid(apr_pool_t *pool, redis_operating_t *handle, 
								 char *member_name, char *member_val)
{
	int ret = -1;
	char *key = NULL;

	key = apr_psprintf(pool, "%s:%s:_indices", handle->type, handle->id);
	if(0 != __redis_del_single_object_dictset(pool, handle, key, member_name, member_val))
	{
		ret = -1;
		goto END;
	}

	key = apr_psprintf(pool, "%s:%s:_zindices", handle->type, handle->id);
	if(0 != __redis_del_single_object_dictset(pool, handle, key, member_name, member_val))
	{
		ret = -1;
		goto END;
	}

	ret = 0;
END:
	return ret;
}

int __redis_del_single_object_dictset(apr_pool_t *pool, redis_operating_t *handle, 
									  char *key, char *member_name, char *member_val)
{
	int ret = -1;
	osip_ring_t *val_ring = NULL;
	osip_ring_iterator_t *it = NULL;
	void *obj = NULL;
	int iCount = 0;
	char *tmp = NULL;

	tmp = apr_psprintf(pool, "%s:%s:%s", handle->type, member_name, member_val);
	if(0 != redis_operating_sismember(pool, handle, key, tmp))
	{
		ret = 0;
		goto END;
	}

	if(0 != __redis_del_single_object_memberset(pool, handle, tmp))
	{
		ret = -1;
		goto END;
	}

	ret = 0;
END:
	return ret;
}

int __redis_del_single_object_memberset(apr_pool_t *pool, redis_operating_t *handle, char *key)
{
	int ret = -1;

	ret = redis_operating_scard(pool, handle, key);
	if(ret <= 0)
	{
		goto END;
	}
	else if(1 == ret)
	{
		if(0 != redis_operating_del(pool, handle, key))
		{
			ret = -1;
			goto END;
		}
	}
	else
	{
		if(0 != redis_operating_srem(pool, handle, key, NULL, apr_psprintf(pool, "%d", handle->id)))
		{
			ret = -1;
			goto END;
		}
	}
	ret = 0;

END:
	return ret;
}

int __redis_del_single_object_timerheap(apr_pool_t *pool, redis_operating_t *handle, char *key)
{
	int ret = -1;

	if(NULL == key || NULL == handle)
	{
		ret = -1;
		goto END;
	}

	ret = redis_operating_zcard(pool, handle, key);
	if(ret <= 0)
	{
		goto END;
	}
	else if(1 == ret)
	{
		if(0 != redis_operating_del(pool, handle, key))
		{
			ret = -1;
			goto END;
		}
	}
	else
	{
		if(0 != redis_operating_zrem(pool, handle, key, NULL, apr_psprintf(pool, "%s:%d", handle->type, handle->id)))
		{
			ret = -1;
			goto END;
		}
	}
	ret = 0;
END:
	return ret;
}

/*
int redis_update_single_object_byid(apr_pool_t *pool, char *type, int id, char member_name, char *new_member_val);

int __redis_update_single_object_dictset(apr_pool_t *pool, char *type, char *old_member_val, 
										 char *new_member_val, int id, char *key);

int __redis_update_single_object_memberset(apr_pool_t *pool, char *type, char *member_name, 
										   char *old_member_val, char *new_member_val, int id);

int redis_update_single_object_timerheap(apr_pool_t *pool, char *key, char *type, int id);
*/

int redis_update_single_object_byid(apr_pool_t *pool, redis_operating_t *handle, char member_name, char *new_member_val)
{
	int ret = -1;
	char *key = NULL;
	char *old_member_val = NULL;

	key = apr_psprintf(pool, "%s:%d", handle->type, handle->id);
	if(0 != redis_operating_hget(pool, handle, key, member_name, &old_member_val))
	{
		ret = -1;
		goto END;
	}

	key = apr_psprintf(pool, "%s:%d:_indices", handle->type, handle->id);
	if(0 != __redis_update_single_object_dictset(pool, handle, member_name, old_member_val, new_member_val, key))
	{
		ret = -1;
		goto END;
	}

	key = apr_psprintf(pool, "%s:%d:_zindices", handle->type, handle->id);
	if(0 != __redis_update_single_object_dictset(pool, handle, member_name, old_member_val, new_member_val, key))
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_operating_hset(pool, handle, key, member_name, new_member_val))
	{
		ret = -1;
		goto END;
	}

	ret = 0;
END:
	return ret;
}

int __redis_update_single_object_dictset(apr_pool_t *pool, redis_operating_t *handle, char *member_name, char *old_member_val, 
										 char *new_member_val, char *key)
{
	int ret = -1;
	osip_ring_t *val_ring = NULL;
	osip_ring_iterator_t *it = NULL;
	void *obj = NULL;
	int iCount = 0;
	char *member = NULL;

	member = apr_psprintf(pool, "%s:%s:%s", handle->type, member_name, old_member_val);
	if(0 != redis_operating_sismember(pool, handle, key, member))
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_operating_srem(pool, handle, key, NULL, member))
	{
		ret = -1;
		goto END;
	}

	if(0 != __redis_update_single_object_memberset(pool, handle, member_name, old_member_val, new_member_val))
	{
		ret = -1;
		goto END;
	}

	member = apr_psprintf(pool, "%s:%s:%s", handle->type, member_name, new_member_val);
	if(0 != redis_operating_sadd(pool, handle, key, NULL, member))
	{
		ret = -1;
		goto END;
	}
	
	ret = 0;
END:
	return ret;
}

int __redis_update_single_object_memberset(apr_pool_t *pool, redis_operating_t *handle, char *member_name, 
										   char *old_member_val, char *new_member_val)
{
	int ret = -1;
	char *key = NULL;

	key = apr_psprintf(pool, "%s:%s:%s", handle->type, member_name, old_member_val);
	if(0 != __redis_del_single_object_memberset(pool, handle, key))
	{
		ret = -1;
		goto END;
	}

	key = apr_psprintf(pool, "%s:%s:%s", handle->type, member_name, new_member_val);
	if(0 != redis_operating_sadd(pool, handle, key, NULL, handle->id))
	{
		ret = -1;
		goto END;
	}
	ret = 0;

END:
	return ret;
}

int redis_update_single_object_timerheap(apr_pool_t *pool, redis_operating_t *handle, char *key, apr_time_t new_time)
{
	int ret = -1;
	char *val = NULL;
	val = apr_psprintf(pool, "%ld %s:%d", new_time, handle->type, handle->id);

	if(0 != redis_operating_zadd(pool, handle, key, NULL, val))
	{
		ret = -1;
		goto END;
	}
	ret = 0;

END:
	return ret;
}

int db_generate_string_member(apr_pool_t *pool, redis_operating_t* handle, 
								 char *name, char *value, int iszindices, int isrelation)
{
	int ret = -1;
	char *hash_val = NULL;

	if(0 != __redis_set_class_num(pool, handle, name, value))
	{
		ret = -1;
		goto END;
	}
	
	if(0 != isrelation)
	{
		if(0 != __redis_set_class_memberset(pool, handle, name, value))
		{
			ret = -1;
			goto END;
		}
	
		if(0 == iszindices)
		{
			if(0 != __redis_set_class_indices(pool, handle, name, value))
			{
				ret = -1;
				goto END;
			}
		}
		else
		{
			if(0 != __redis_set_class_zindices(pool, handle, name, value))
			{
				ret = -1;
				goto END;
			}
		}
	}

	ret = 0;
END:
	return ret;
}

int db_generate_timer_member(apr_pool_t *pool, redis_operating_t* handle, char *timeheap, 
								char *name, apr_time_t value, int iszindices, int isrelation)
{
	int ret = -1;
	char *tmp = NULL;

	tmp = apr_psprintf(pool, "%ld", value);
	if(0 != __redis_set_class_num(pool, handle, name, tmp))
	{
		ret = -1;
		goto END;
	}

	tmp = apr_psprintf(pool, "%s:%d", handle->type, handle->id);
	if(0 != __redis_set_class_timerheap(pool, handle, timeheap, tmp, value))
	{
		ret = -1;
		goto END;
	}

	if(0 != isrelation)
	{
		tmp = apr_psprintf(pool, "%ld", value);
		if(0 != __redis_set_class_memberset(pool, handle, name, tmp))
		{
			ret = -1;
			goto END;
		}

		if(0 == iszindices)
		{
			if(0 != __redis_set_class_indices(pool, handle, name, tmp))
			{
				ret = -1;
				goto END;
			}
		}
		else
		{
			if(0 != __redis_set_class_zindices(pool, handle, name, tmp))
			{
				ret = -1;
				goto END;
			}
		}
	}
	ret = 0;
END:
	return ret;
}

int db_generate_reference_member(apr_pool_t *pool, redis_operating_t* handle, 
									char *type, char *name, void *value, int isrelation, func_call_class func)
{
	int ret = -1;
	char *tmp = NULL;

	int sub_id = func(pool, type, value);

	tmp = apr_psprintf(pool, "%s:%d", type, sub_id);
	if(0 != __redis_set_class_num(pool, handle, name, tmp))
	{
		ret = -1;
		goto END;
	}
	
	if(0 != isrelation)
	{
		if(0 != __redis_set_class_memberset(pool, handle, name, tmp))
		{
			ret = -1;
			goto END;
		}

		if(0 != __redis_set_class_indices(pool, handle, name, tmp))
		{
			ret = -1;
			goto END;
		}
	}

	ret = 0;
END:
	return ret;
}

int db_generate_list_member(apr_pool_t *pool, redis_operating_t* handle, 
							   char *type, char *name, void *value, int isrelation, func_call_class func)
{
	int ret = -1;
	char *tmp = NULL;
	int sub_id = -1;

	if(type)
	{
		sub_id = func(pool, type, value);
	}
	tmp = apr_psprintf(pool, "%s:%d:%s", handle->type, handle->id, name);

	if(0 != __redis_set_class_num(pool, handle, name, tmp))
	{
		ret = -1;
		goto END;
	}

	if(type)
	{
		tmp = apr_psprintf(pool, "%s:%d", type, sub_id);
	}
	else
	{
		tmp = (char *)value;
	}

	if(0 != __redis_set_class_list(pool, handle, name, tmp))
	{
		ret = -1;
		goto END;
	}

	if(0 != isrelation)
	{
		if(0 != __redis_set_class_memberset(pool, handle, name, tmp))
		{
			ret = -1;
			goto END;
		}

		if(0 != __redis_set_class_indices(pool, handle, name, tmp))
		{
			ret = -1;
			goto END;
		}
	}
	ret = 0;
END:
	return ret;
}

char *db_get_value_string_and_time(apr_pool_t *pool, const char *key, char *member_name)
{
	int ret = -1;
	char *result = NULL;
	char *type = NULL;
	char *id = NULL;
	char *p = NULL;
	char *tmp_key = NULL;
	
	tmp_key = apr_pstrdup(pool ,key);
	type=  apr_strtok(tmp_key, ":", &p);
	id = apr_strtok(NULL, ":" ,&p);
	redis_operating_t *handle = redis_operating_create(pool, type, id, -1);
	if(NULL == handle)
	{
		ret = -1;
		result = NULL;
		goto END;
	}

	ret = redis_operating_hget(handle->pool, handle, key, member_name, &result);
	if(ret != 0 || NULL == result)
	{
		ret = -1;
		result = NULL;
		goto END;
	}

END:
	return result;
}

void *db_get_value_reference(apr_pool_t *pool, const char *key, char *member_name, func_call_get func)
{
	int ret = -1;
	char *reference_obj = NULL;
	void *result = NULL;
	char *type = NULL;
	char *id = NULL;
	char *p = NULL;
	char *tmp_key = NULL;
	
	tmp_key = apr_pstrdup(pool ,key);
	type=  apr_strtok(tmp_key, ":", &p);
	id = apr_strtok(NULL, ":" ,&p);
	redis_operating_t *handle = redis_operating_create(pool, type, atoi(id), -1);
	if(NULL == handle)
	{
		ret = -1;
		result = NULL;
		goto END;
	}

	ret = redis_operating_hget(handle->pool, handle, key, member_name, &reference_obj);
	if(ret != 0 || NULL == reference_obj)
	{
		ret = -1;
		result = NULL;
		goto END;
	}

	result = func(pool, reference_obj);
END:
	return result;
}

void *db_get_value_list(apr_pool_t *pool, const char *key, char *sub_type, char *member_name, func_call_get func)
{
	int ret = -1;
	char *reference_obj = NULL;
	void *result = NULL;
	void *obj = NULL;
	char *type = NULL;
	char *id = NULL;
	char *p = NULL;
	osip_ring_t *val_ring = NULL;
	osip_ring_iterator_t *it = NULL;
	osip_ring_t *ret_ring = NULL;
	char *tmp_key = NULL;
	
	tmp_key = apr_pstrdup(pool ,key);
	type=  apr_strtok(tmp_key, ":", &p);
	id = apr_strtok(NULL, ":" ,&p);

	redis_operating_t *handle = redis_operating_create(pool, type, atoi(id), -1);
	if(NULL == handle)
	{
		ret = -1;
		result = NULL;
		goto END;
	}

	ret = __redis_get_class_list(handle->pool, handle, key, member_name, &val_ring);
	if(ret != 0 || NULL == val_ring)
	{
		ret = -1;
		result = NULL;
		goto END;
	}

	osip_ring_create(pool, &ret_ring);
	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(val_ring, it); obj; obj = osip_ring_get_next(it))
	{
		char *tmp = NULL;
		if(sub_type)
		{
			tmp = (char *)obj;
			obj = func(pool, tmp);
		}
		osip_ring_add(ret_ring, obj, -1);
	}
	osip_ring_destroy_iterator(it);
	result = ret_ring;

END:
	if(val_ring)
	{
		obj = NULL;
		it = NULL;
		osip_ring_create_iterator(pool, &it);
	    for(obj = osip_ring_get_first(val_ring, it); obj; obj = osip_ring_get_next(it))
		{
			osip_ring_iterator_remove(it);
		}
	    osip_ring_destroy_iterator(it);
	}
	return result;
}

int db_delete_string_member(apr_pool_t *pool, char *key, char *member_name)
{
	int ret = -1;
	char *type = NULL;
	char *id = NULL;
	char *p = NULL;
	redis_operating_t *handle = NULL;
	char *str_val = NULL;
	char *tmp_key = NULL;

	tmp_key = apr_pstrdup(pool, key);
	type = apr_strtok(tmp_key, ":", &p);
	id = apr_strtok(NULL, ":" ,&p);

	handle = redis_operating_create(pool, type, atoi(id), -1);
	if(NULL == handle)
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_operating_hget(pool, handle, key, member_name, &str_val))
	{
		ret = -1;
		goto END;
	}

	handle = redis_operating_nowatch_init(pool, type, id);
	if(NULL == handle)
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_del_single_object_byid(handle->pool, handle, member_name, str_val))
	{
		ret = -1;
		goto END;
	}
	
	ret = redis_operating_hdel(handle->pool, handle, key, member_name);
	if(ret != 0)
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_operating_exec(pool, handle))
	{
		ret = -1;
		goto END;
	}
	handle = NULL;
	
END:
	if(handle)
	{
		apr_pool_cleanup_run(handle->pool, handle, __cleanup_redis_operating);
	}
	return ret;
}
int db_delete_timer_member(apr_pool_t *pool, char *key, char *time_key, char *member_name)
{
	int ret = -1;
	char *type = NULL;
	char *id = NULL;
	char *p = NULL;
	char *str_val = NULL;
	redis_operating_t *handle = NULL;
	char *tmp_key = NULL;

	tmp_key = apr_pstrdup(pool, key);
	type=  apr_strtok(tmp_key, ":", &p);
	id = apr_strtok(NULL, ":" ,&p);

	handle = redis_operating_create(pool, type, atoi(id), -1);
	if(NULL == handle)
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_operating_hget(pool, handle, key, member_name, &str_val))
	{
		ret = -1;
		goto END;
	}

	handle = redis_operating_nowatch_init(pool, type, id);
	if(NULL == handle)
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_del_single_object_byid(handle->pool, handle, member_name, str_val))
	{
		ret = -1;
		goto END;
	}

	ret = redis_operating_hdel(handle->pool, handle, key, member_name);
	if(ret != 0)
	{
		ret = -1;
		goto END;
	}

	ret = redis_operating_zrem(handle->pool, handle, time_key, NULL, key);
	if(ret != 0)
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_operating_exec(pool, handle))
	{
		ret = -1;
		goto END;
	}
	handle = NULL;

END:
	if(handle)
	{
		apr_pool_cleanup_run(handle->pool, handle, __cleanup_redis_operating);
	}
	return ret;
}

int db_delete_reference_member(apr_pool_t *pool, char *key, char *member_name, func_call_del func)
{
	int ret = -1;
	char *type = NULL;
	char *id = NULL;
	char *p = NULL;
	redis_operating_t *handle = NULL;
	char *str_val = NULL;
	char *tmp_key = NULL;

	tmp_key = apr_pstrdup(pool, key);
	type=  apr_strtok(tmp_key, ":", &p);
	id = apr_strtok(NULL, ":" ,&p);

	handle = redis_operating_create(pool, type, atoi(id), -1);
	if(NULL == handle)
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_operating_hget(pool, handle, key, member_name, &str_val))
	{
		ret = -1;
		goto END;
	}

	if(0 != func(pool, str_val))
	{
		ret = -1;
		goto END;
	}

	handle = redis_operating_nowatch_init(pool, type, id);
	if(NULL == handle)
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_del_single_object_byid(handle->pool, handle, member_name, str_val))
	{
		ret = -1;
		goto END;
	}

	ret = redis_operating_hdel(handle->pool, handle, key, member_name);
	if(ret != 0)
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_operating_exec(handle->pool, handle))
	{
		ret = -1;
		goto END;
	}
	handle = NULL;
	ret = 0;

END:
	if(handle)
	{
		apr_pool_cleanup_run(handle->pool, handle, __cleanup_redis_operating);
	}
	return ret;
}

int db_delete_list_member(apr_pool_t *pool, char *key, char *member_name, func_call_del func)
{
	int ret = -1;
	char *type = NULL;
	char *id = NULL;
	char *p = NULL;
	redis_operating_t *handle = NULL;
	char *str_val = NULL;
	char *tmp_key = NULL;
	osip_ring_t *ring = NULL;
	osip_ring_iterator_t *it = NULL;
	void *sub_obj = NULL;

	tmp_key = apr_pstrdup(pool, key);
	type=  apr_strtok(tmp_key, ":", &p);
	id = apr_strtok(NULL, ":" ,&p);

	handle = redis_operating_create(pool, type, atoi(id), -1);
	if(NULL == handle)
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_operating_hget(pool, handle, key, member_name, &str_val))
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_operating_smembers(pool, handle, str_val, &ring))
	{
		ret = -1;
		goto END;
	}

	osip_ring_create_iterator(pool, &it);
	for(sub_obj = osip_ring_get_first(ring, it); sub_obj; sub_obj = osip_ring_get_next(it))
	{
		if(0 != func(pool, (char *)sub_obj))
		{
			ret = -1;
			goto END;
		}
	}
	osip_ring_destroy_iterator(it);

	handle = redis_operating_nowatch_init(pool, type, id);
	if(NULL == handle)
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_del_single_object_byid(handle->pool, handle, member_name, str_val))
	{
		ret = -1;
		goto END;
	}

	str_val = apr_psprintf(pool, "%s:%s:%s", type, id, member_name);
	if(0 != redis_operating_del(pool, handle, str_val))
	{
		ret = -1;
		goto END;
	}

	ret = redis_operating_hdel(handle->pool, handle, key, member_name);
	if(ret != 0)
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_operating_exec(handle->pool, handle))
	{
		ret = -1;
		goto END;
	}
	handle = NULL;
	ret = 0;

END:
	if(handle)
	{
		apr_pool_cleanup_run(handle->pool, handle, __cleanup_redis_operating);
	}
	return ret;
}

int db_delete_other_element(apr_pool_t *pool, char *key)
{
	int ret = -1;
	char *type = NULL;
	char *id = NULL;
	char *p = NULL;
	redis_operating_t *handle = NULL;
	char *str_val = NULL;
	char *tmp_key = NULL;

	tmp_key = apr_pstrdup(pool, key);
	type = apr_strtok(tmp_key, ":", &p);
	id = apr_strtok(NULL, ":" ,&p);
	
	char *str_val_hash = apr_psprintf(pool, " %s:%d ", type, id);
	char *str_val_indicts = apr_psprintf(pool, " %s:%d:_indices ", type, id);
	char *str_val_zindicts = apr_psprintf(pool, " %s:%d:_zindices ", type, id);
	str_val = apr_pstrcat(pool, str_val_hash, str_val_indicts, str_val_zindicts, NULL);

	handle = redis_operating_watch_init(pool, type, id, str_val);
	if(NULL == handle)
	{
		ret = -1;
		goto END;
	}

	char *str_val_all = apr_psprintf(pool, "%s:all", type, id);
	if(0 != redis_operating_srem(pool, handle, str_val_all, NULL, atoi(id)))
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_operating_del(pool, handle, str_val_hash))
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_operating_del(pool, handle, str_val_indicts))
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_operating_del(pool, handle, str_val_zindicts))
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_operating_exec(handle->pool, handle))
	{
		ret = -1;
		goto END;
	}
	handle = NULL;
	ret = 0;
END:
	if(handle)
	{
		apr_pool_cleanup_run(handle->pool, handle, __cleanup_redis_operating);
	}
	return ret;
}

int db_update_string_member(apr_pool_t *pool, char *key, char *member_name, char *new_value)
{
	int ret = -1;
	char *type = NULL;
	char *id = NULL;
	char *p = NULL;
	redis_operating_t *handle = NULL;
	char *str_val = NULL;
	char *tmp_key = NULL;

	type=  apr_strtok(tmp_key, ":", &p);
	id = apr_strtok(NULL, ":" ,&p);
	char *str_val_hash = apr_psprintf(pool, " %s:%d ", type, id);
	char *str_val_indicts = apr_psprintf(pool, " %s:%d:_indices ", type, id);
	char *str_val_zindicts = apr_psprintf(pool, " %s:%d:_zindices ", type, id);
	str_val = apr_pstrcat(pool, str_val_hash, str_val_indicts, str_val_zindicts, NULL);

	handle = redis_operating_watch_init(pool, type, id, str_val);
	if(NULL == handle)
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_update_single_object_byid(pool, handle, member_name, new_value))
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_operating_exec(handle->pool, handle))
	{
		ret = -1;
		goto END;
	}
	handle = NULL;
	ret = 0;

END:
	if(handle)
	{
		apr_pool_cleanup_run(handle->pool, handle, __cleanup_redis_operating);
	}
	return ret;
}
int redis_update_timer_member(apr_pool_t *pool, char *key, char *time_key, char *member_name, apr_time_t new_value)
{
	int ret = -1;
	char *type = NULL;
	char *id = NULL;
	char *p = NULL;
	redis_operating_t *handle = NULL;
	char *str_val = NULL;
	char *tmp_key = NULL;

	type=  apr_strtok(tmp_key, ":", &p);
	id = apr_strtok(NULL, ":" ,&p);
	char *str_val_hash = apr_psprintf(pool, " %s:%d ", type, id);
	char *str_val_indicts = apr_psprintf(pool, " %s:%d:_indices ", type, id);
	char *str_val_zindicts = apr_psprintf(pool, " %s:%d:_zindices ", type, id);
	str_val = apr_pstrcat(pool, str_val_hash, str_val_indicts, str_val_zindicts, NULL);

	handle = redis_operating_watch_init(pool, type, id, str_val);
	if(NULL == handle)
	{
		ret = -1;
		goto END;
	}

	str_val = apr_psprintf(pool, "%ld", new_value);
	if(0 != redis_update_single_object_byid(pool, handle, member_name, str_val))
	{
		ret = -1;
		goto END;
	}

	str_val = apr_pstrcat(pool, str_val, " ", key, NULL);
	if(0 != redis_operating_zadd(pool, handle, time_key, NULL, str_val))
	{
		ret = -1;
		goto END;
	}

	if(0 != redis_operating_exec(pool, handle))
	{
		ret = -1;
		goto END;
	}
	ret = 0;

END:
	if(handle)
	{
		apr_pool_cleanup_run(handle->pool, handle, __cleanup_redis_operating);
	}
	return ret;
}
int redis_update_reference_member(apr_pool_t *pool, char *key, char *member_name, void *new_value,
								  func_call_del func_del, func_call_update func_update)
{
	int ret = -1;
	char *type = NULL;
	char *id = NULL;
	char *p = NULL;
	redis_operating_t *handle = NULL;
	char *str_val = NULL;
	char *tmp_key = NULL;

	type=  apr_strtok(tmp_key, ":", &p);
	id = apr_strtok(NULL, ":" ,&p);

	handle = redis_operating_create(pool, type, id, 0);
	if(NULL == handle)
	{
		ret = -1;
		goto END;
	}
	if(0 != redis_operating_hget(pool, handle, key, member_name, &str_val))
	{
		ret = -1;
		goto END;
	}

	if(0 != func_del(pool, str_val))
	{
		ret = -1;
		goto END;
	}

	if(0 != func_update(pool, str_val, new_value))
	{
		ret = -1;
		goto END;
	}

	ret = 0;

END:
	if(handle)
	{
		apr_pool_cleanup_run(handle->pool, handle, __cleanup_redis_operating);
	}
	return ret;
}
int redis_update_list_member(apr_pool_t *pool, char *key, char *member_name, void *value, 
							 func_call_del func_del, func_call_update func_update)
{
	int ret = -1;
	char *reference_obj = NULL;
	void *obj = NULL;
	char *type = NULL;
	char *id = NULL;
	char *p = NULL;
	osip_ring_t *val_ring = NULL;
	osip_ring_iterator_t *it = NULL;
	osip_ring_t *ret_ring = NULL;
	char *tmp_key = NULL;

	type=  apr_strtok(tmp_key, ":", &p);
	id = apr_strtok(NULL, ":" ,&p);

	redis_operating_t *handle = redis_operating_create(pool, type, id, 0);
	if(NULL == handle)
	{
		ret = -1;
		goto END;
	}

	ret = __redis_get_class_list(handle->pool, handle, key, member_name, &val_ring);
	if(ret != 0 || NULL == val_ring)
	{
		ret = -1;
		goto END;
	}

	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(val_ring, it); obj; obj = osip_ring_get_next(it))
	{
		char *tmp = (char *)obj;
		if(obj) 
		{
			func_del(pool, tmp);
			func_update(pool, tmp, value);
		}
	}
	osip_ring_destroy_iterator(it);

END:
	if(handle)
	{
		apr_pool_cleanup_run(handle->pool, handle, __cleanup_redis_operating);
	}
	if(val_ring)
	{
		obj = NULL;
		it = NULL;
		osip_ring_create_iterator(pool, &it);
	    for(obj = osip_ring_get_first(val_ring, it); obj; obj = osip_ring_get_next(it))
		{
			osip_ring_iterator_remove(it);
		}
	    osip_ring_destroy_iterator(it);
	}
	return ret;
}

redis_operating_t *redis_operating_nomutli_init(apr_pool_t *pool, char *type, int id)
{
	redis_operating_t *handle = NULL;
	handle = redis_operating_create(pool, type, id, 1);
	return handle;
}

redis_operating_t *redis_operating_nowatch_init(apr_pool_t *pool, char *type, int id)
{
	redis_operating_t *handle = NULL;
	handle = redis_operating_create(pool, type, id, 1);
	if(NULL == handle) goto END;
	redis_operating_mutli(handle->pool, handle);
END:
	return handle;
}

redis_operating_t *redis_operating_watch_init(apr_pool_t *pool, char *type, int id, char *watch)
{
	redis_operating_t *handle = NULL;
	handle = redis_operating_create(pool, type, id, 1);
	if(NULL == handle) goto END;
	if(watch) redis_operating_watch(pool, type, watch);
	redis_operating_mutli(handle->pool, handle);
END:
	return handle;
}

redis_operating_t *redis_operating_create(apr_pool_t *pool, char *type, int id, int isconnect)
{
	redis_operating_t *result = NULL;
	redisContext *conn = NULL;

	char *key = apr_psprintf(pool, "%s:id", type);
	result = apr_pcalloc(pool, sizeof(redis_operating_t));
	result->pool = pool;
	result->type = apr_pstrdup(result->pool, type);

	if(0 == id) 
	{
		conn = __redis_operating_connect(__strIP, __port, &__timeout);
		if(NULL == conn)
		{
			result = NULL;
			goto END;
		}
		result->id = redis_operating_incr(result->pool, conn, key, 1);
		if(conn) redisFree(conn);
		conn = NULL;
	}
	else 
		result->id = id;

	result->key = apr_psprintf(pool, "%s:%d", type, id);

	if(-1 != isconnect)
	{
		conn = __redis_operating_connect(__strIP, __port, &__timeout);
		if(NULL == conn)
		{
			result = NULL;
			goto END;
		}
		result->connect = conn;
		apr_pool_cleanup_register(pool, result, __cleanup_redis_operating, apr_pool_cleanup_null);
	}
	else
	{
		result->connect = NULL;
	}

END:
	return result;
}

int __cleanup_redis_operating(void *ctx)
{
	redis_operating_t *handle = ctx;

	if(handle->connect)
	{
		redisFree(handle->connect);
		handle->connect = NULL;
	}

	return 0;
}

