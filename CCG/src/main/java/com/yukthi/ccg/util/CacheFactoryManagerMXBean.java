package com.yukthi.ccg.util;

public interface CacheFactoryManagerMXBean 
{
	public int getActiveCount();
	public int getFreeCount();
	public void clean();
}
