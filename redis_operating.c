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
    	printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
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
		if(!pool && !reply_result) 
		{
			reply_result->result = apr_pstrdup(pool, "error");
			reply_result->type = REDIS_REPLY_ERROR;
		}
	}
	else if(REDIS_REPLY_INTEGER == reply->type)
	{
		ret = 0;
		if(NULL != pool && NULL != reply_result) 
		{
			reply_result->result = apr_psprintf(pool, "%ld", reply->integer);
			reply_result->type = REDIS_REPLY_INTEGER;
		}
	}
	else if(REDIS_REPLY_NIL == reply->type)
	{
		ret = 0;
		if(NULL != pool && NULL != reply_result) 
		{
			reply_result->result = apr_pstrdup(pool, "nil");
			reply_result->type = REDIS_REPLY_NIL;
		}
	}
	else if(REDIS_REPLY_STATUS == reply->type)
	{
		ret = 0;
		if(NULL != pool && NULL != reply_result) 
		{
			reply_result->result = apr_pstrdup(pool, reply->str);
			reply_result->type = REDIS_REPLY_STATUS;
		}
	}
	else if(REDIS_REPLY_STRING == reply->type)
	{
		ret = 0;
		if(NULL != pool && NULL != reply_result) 
		{
			reply_result->result = apr_pstrdup(pool, reply->str);
			reply_result->type = REDIS_REPLY_STRING;
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
		if(0 != osip_ring_create(pool, &ret_ring))
		{
			printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
			ret = -1;
			goto END;
		}
		if (reply->type == REDIS_REPLY_ARRAY) 
		{
			int j = 0;
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
			/*if (reply->type == REDIS_REPLY_STRING)
			{
hk				osip_ring_add(ret_ring, reply->str, -1);
			}
			else if (reply->type == REDIS_REPLY_INTEGER)
			{
				osip_ring_add(ret_ring, reply->integer, -1);
			}
			else
			{*/
			printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
			ret = -1;
			goto END;
			//}
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

int redis_operating_hset(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, char *field, char *value)
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
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
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
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;

}

int redis_operating_hmset(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, apr_hash_t *hash)
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

int redis_operating_hget(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, 
						 char *field, char **value)
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

	strcmd = apr_psprintf(pool, "HGET %s %s", key, field);

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

/*int redis_operating_hdel(const char *ip, int port, const struct timeval tv, char *strcmd);

  int redis_operating_hgetall_hvals(const char *ip, int port, const struct timeval tv, char *strcmd);*/

int redis_operating_sadd(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, osip_ring_t *ring)
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

	if(NULL == ring || NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(pool, "SADD %s ", key);

	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(ring, it); obj; obj = osip_ring_get_next(it))
	{
		apr_pool_create(&subpool2, pool);
		strcmd->string = apr_pstrcat(pool, strcmd->string, " ", (char *)obj, " ", NULL);
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
	

	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd->string, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	if(reply_result && 3 == reply_result->type)
		ret = 0;
	else
		ret = -1;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

int redis_operating_srem(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, osip_ring_t *ring)
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

	if(NULL == ring || NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(pool, "SREM %s ", key);

	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(ring, it); obj; obj = osip_ring_get_next(it))
	{
		apr_pool_create(&subpool2, pool);
		strcmd->string = apr_pstrcat(pool, strcmd->string, " ", (char *)obj, " ", NULL);
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
	
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd->string, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	if(reply_result && 3 == reply_result->type)
		ret = 0;
	else
		ret = -1;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

int redis_operating_del(apr_pool_t *pool, const char *ip, int port, struct timeval tv, osip_ring_t *key_ring)
{
	//char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;
	redisContext* gClient = NULL;
	void *obj = NULL;
	osip_ring_iterator_t *it = NULL;

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

	strcmd->string = apr_pstrdup(pool, "DEL ");
	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(key_ring, it); obj; obj = osip_ring_get_next(it))
	{
		apr_pool_create(&subpool2, pool);
		strcmd->string = apr_pstrcat(pool, strcmd->string, " ", (char *)obj, " ", NULL);
		apr_pool_destroy(strcmd->pool);
		strcmd->pool = subpool2;
	}
	osip_ring_destroy_iterator(it);

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

	if(reply_result && 3 == reply_result->type)
	{
		int itmp = atoi(reply_result->result);
		if(itmp > 0) ret = 0;
		else ret = -1;
	}
	else
		ret = -1;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

/*int redis_operating_smembers(const char *ip, int port, const struct timeval tv, char *strcmd);

  int redis_operating_sismembers(const char *ip, int port, const struct timeval tv, char *strcmd);*/

