/*******************************************************************************
 * Copyright (c) 2014 AtlanMod.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Amine Benelallam - implementation
 ******************************************************************************/
package fr.inria.atlanmod.neoemf.datastore.estores.counters;

import java.io.IOException;
import java.text.MessageFormat;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.resource.Resource;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import fr.inria.atlanmod.neoemf.core.NeoEMFEObject;
import fr.inria.atlanmod.neoemf.datastore.estores.impl.ReadOnlyHbaseResourceEStoreImpl;
import fr.inria.atlanmod.neoemf.logger.Logger;
import fr.inria.atlanmod.neoemf.util.NeoEMFCount;
import fr.inria.atlanmod.neoemf.util.NeoEMFUtil;


public class ReadOnlyHbaseResourceWithCountersEStoreImpl extends ReadOnlyHbaseResourceEStoreImpl {


	protected TaskAttemptContext jobContext;
	

	public ReadOnlyHbaseResourceWithCountersEStoreImpl(Resource.Internal resource) throws IOException {
		super(resource); 
	}

	public ReadOnlyHbaseResourceWithCountersEStoreImpl(Resource.Internal resource,TaskAttemptContext context) throws IOException {
		super(resource);
		this.jobContext = context;
	}


    private void increment(Enum<?> readType) {
    	jobContext.getCounter(readType).increment(1);
    }
	
	/**
	 * Gets the {@link EStructuralFeature} {@code feature} from the
	 * {@link Table} for the {@link NeoEMFEObject} {@code object} if not locally cached.
	 * It also reports the access to the mapReduce Job {@link Job} trough the 
	 * 
	 * @param object
	 * @param feature
	 * @return The value of the {@code feature}. It can be a {@link String} for
	 *         single-valued {@link EStructuralFeature}s or a {@link String}[]
	 *         for many-valued {@link EStructuralFeature}s
	 */
	protected Object getFromTableIfNotExisting(NeoEMFEObject object, EStructuralFeature feature) {
		EStoreEntryKey entry = new EStoreEntryKey(object.neoemfId(), feature);
		if (featuresMap.containsKey(entry) 
				&& featuresMap.get(entry) != null)  {
			increment(NeoEMFCount.LOCAL_READ);
			return featuresMap.get(entry);
		} else {
			try {
				Result result = table.get(new Get(Bytes.toBytes(object.neoemfId())));
				byte[] value = result.getValue(PROPERTY_FAMILY, Bytes.toBytes(feature.getName()));
				if (!feature.isMany()) {
					featuresMap.put(entry, Bytes.toString(value));
				} else {
					featuresMap.put(entry, feature instanceof EAttribute ?
													NeoEMFUtil.EncoderUtil.toStrings(value) : 
														NeoEMFUtil.EncoderUtil.toStringsReferences(value)	);
				}
			} catch (IOException e) {
				Logger.log(Logger.SEVERITY_ERROR, MessageFormat.format("Unable to get property ''{0}'' for ''{1}''", feature.getName(), object));
			}
		}
		increment(NeoEMFCount.REMOTE_READ);
		return featuresMap.get(entry);
	}


}
