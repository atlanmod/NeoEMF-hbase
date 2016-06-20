package fr.inria.atlanmod.neoemf.util;

/**
 * An enum type for Hadoop Job Counters  
 * It counts the update times on a HTable when running a hadoop/hbase job 
 * @author Amine BENELALLAM
 *
 */

public enum WriteCount {

	UPDATE_MANY,
	UPDATE_SINGLE
	
}
