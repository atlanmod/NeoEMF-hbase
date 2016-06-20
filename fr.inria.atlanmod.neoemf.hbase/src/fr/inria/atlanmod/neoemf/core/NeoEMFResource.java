/*******************************************************************************
 * Copyright (c) 2014 Abel G�mez.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Abel G�mez - initial API and implementation
 ******************************************************************************/
package fr.inria.atlanmod.neoemf.core;

import org.eclipse.emf.ecore.InternalEObject;
import org.eclipse.emf.ecore.resource.Resource;

public interface NeoEMFResource extends Resource, Resource.Internal {

	/**
	 * an option for creating read-only models
	 * by default  {@value <code>false</code>}
	 */
	String  OPTIONS_HBASE_READ_ONLY = "hbase.readOnly";
	boolean OPTIONS_HBASE_READ_ONLY_DEFAULT = false; 
	
	/**
	 * an option for adding counters to resources read-only models
	 * by default  {@value <code>false</code>}
	 */
	String  OPTIONS_HBASE_HAS_COUNTERS = "hbase.has.counters";
	boolean OPTIONS_HBASE_HAS_COUNTERS_DEFAULT = false; 
	
	/**
	 * an option for adding counters to resources read-only models
	 * by default  {@value <code>false</code>}
	 */
	String  OPTIONS_HBASE_JOB_CONTEXT = "hbase.job.context";
	
	public abstract InternalEObject.EStore eStore();
	
}
