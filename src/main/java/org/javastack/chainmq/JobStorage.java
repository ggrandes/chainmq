package org.javastack.chainmq;

/**
 * Interface for Job Storage
 */
public interface JobStorage {
	void putJob(final long id, final Job job);

	Job getJob(final long id);

	void removeJob(final long id);

	int totalJobs();
}
