package com.yukthi.ccg.util;

public interface ICacheableBean<T>
{
	public void reinitalize(ObjectCacheFactory<T> factory);
}
