/*******************************************************************************
 * Copyright (c) 2014 AtlanMod
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Amine Benelallam
 ******************************************************************************/
package fr.inria.atlanmod.neoemf.datastore.estores.counters;

import java.io.IOException;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.InternalEObject;
import org.eclipse.emf.ecore.resource.Resource;

import fr.inria.atlanmod.neoemf.datastore.estores.impl.DirectWriteHbaseResourceEStoreImpl;
import fr.inria.atlanmod.neoemf.util.ReadCount;
import fr.inria.atlanmod.neoemf.util.WriteCount;

public class DirectWriteHbaseResourceWithCountersEStoreImpl extends DirectWriteHbaseResourceEStoreImpl {

	protected TaskAttemptContext jobContext;

	public DirectWriteHbaseResourceWithCountersEStoreImpl(Resource.Internal resource) throws IOException {
		super(resource);
	}

	public DirectWriteHbaseResourceWithCountersEStoreImpl(Resource.Internal resource, TaskAttemptContext context) throws IOException {
		super(resource);
		this.jobContext = context;	
	}




//	@Override
//	public Object get(InternalEObject object, EStructuralFeature feature, int index) {
//		Object result = super.get(object, feature, index);
//		if (feature.isMany()) {
//			increment(WriteCount.UPDATE_MANY);
//		} else {
//			increment(WriteCount.UPDATE_SINGLE);
//		}
//		return result;
//	}
	

	@Override
	public Object set(InternalEObject object, EStructuralFeature feature, int index, Object value) {
		Object result = super.set(object, feature, index, value);
		increment(WriteCount.UPDATE_SINGLE);
		return result;

	}
	
	@Override
	public Object remove(InternalEObject object, EStructuralFeature feature, int index) {
		Object result = super.remove(object, feature, index);
		increment(WriteCount.UPDATE_MANY);
		return result;
	}
	
	@Override
	public void add(InternalEObject object, EStructuralFeature feature, int index, Object value) {
		super.add(object, feature, index, value);
		increment(WriteCount.UPDATE_MANY);
	}
	private void increment(Enum<?> update) {
		jobContext.getCounter(update).increment(1);
	}

}
