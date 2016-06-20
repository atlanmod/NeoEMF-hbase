/*******************************************************************************
 * Copyright (c) 2015 Abel G{\'o}mez.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Abel G{\'o}mez - initial API and implementation
 *     Amine Benelallam - implementation
 ******************************************************************************/
package fr.inria.atlanmod.neoemf.core.impl;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;
import org.eclipse.emf.common.notify.impl.NotificationImpl;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.ETypedElement;
import org.eclipse.emf.ecore.InternalEObject;
import org.eclipse.emf.ecore.InternalEObject.EStore;
import org.eclipse.emf.ecore.impl.EClassifierImpl;
import org.eclipse.emf.ecore.impl.EReferenceImpl;
import org.eclipse.emf.ecore.impl.EStoreEObjectImpl;
import org.eclipse.emf.ecore.impl.EStoreEObjectImpl.EStoreEList;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.impl.ResourceImpl;

import fr.inria.atlanmod.neoemf.core.NeoEMFEObject;
import fr.inria.atlanmod.neoemf.core.NeoEMFInternalEObject;
import fr.inria.atlanmod.neoemf.core.NeoEMFResource;
import fr.inria.atlanmod.neoemf.datastore.estores.SearcheableResourceEStore;
import fr.inria.atlanmod.neoemf.datastore.estores.counters.DirectWriteHbaseResourceWithCountersEStoreImpl;
import fr.inria.atlanmod.neoemf.datastore.estores.counters.ReadOnlyHbaseResourceWithCountersEStoreImpl;
import fr.inria.atlanmod.neoemf.datastore.estores.impl.DirectWriteHbaseResourceEStoreImpl;
import fr.inria.atlanmod.neoemf.datastore.estores.impl.IsSetCachingDelegatedEStoreImpl;
import fr.inria.atlanmod.neoemf.datastore.estores.impl.ReadOnlyHbaseResourceEStoreImpl;
import fr.inria.atlanmod.neoemf.datastore.estores.impl.SizeCachingDelegatedEStoreImpl;
import fr.inria.atlanmod.neoemf.datastore.exceptions.InvalidOptionsException;

public class NeoEMFHbaseResourceImpl extends ResourceImpl implements NeoEMFResource {

	/**
	 * Fake {@link EStructuralFeature} that represents the
	 * {@link Resource#getContents()} feature.
	 * 
	 * @author agomez
	 * 
	 */
	protected static class ResourceContentsEStructuralFeature extends EReferenceImpl {
		protected static final String RESOURCE__CONTENTS__FEATURE_NAME = "eContents";

		public ResourceContentsEStructuralFeature() {
			this.setUpperBound(ETypedElement.UNBOUNDED_MULTIPLICITY);
			this.setLowerBound(0);
			this.setName(RESOURCE__CONTENTS__FEATURE_NAME);
			this.setEType(new EClassifierImpl() {
			});
			this.setFeatureID(RESOURCE__CONTENTS);
		}
	}

	/**
	 * Dummy {@link EObject} that represents the root entry point for this
	 * {@link Resource}
	 * 
	 * @author agomez
	 * 
	 */
	protected final class DummyRootEObject extends NeoEMFEObjectImpl {
		protected static final String ROOT_EOBJECT_ID = "ROOT";

		public DummyRootEObject(Resource.Internal resource) {
			super();
			this.id = ROOT_EOBJECT_ID;
			eSetDirectResource(resource);
		}
	}

	protected static final ResourceContentsEStructuralFeature ROOT_CONTENTS_ESTRUCTURALFEATURE = new ResourceContentsEStructuralFeature();

	protected final DummyRootEObject DUMMY_ROOT_EOBJECT = new DummyRootEObject(this);

	protected Map<Object, Object> options;
	
	protected SearcheableResourceEStore eStore;

	//protected Connection connection;
	
	protected boolean isPersistent = false;

	public NeoEMFHbaseResourceImpl(URI uri) {

		super(uri);
		//this.connection = null;
		this.eStore = null;
		this.isPersistent = false;
		// initialize the map options with default values 
		options =  new HashMap<Object, Object> (); 
		options.put(NeoEMFResource.OPTIONS_HBASE_READ_ONLY, NeoEMFResource.OPTIONS_HBASE_READ_ONLY_DEFAULT);
		options.put(NeoEMFResource.OPTIONS_HBASE_HAS_COUNTERS, NeoEMFResource.OPTIONS_HBASE_HAS_COUNTERS_DEFAULT);
			
		
	}

	@Override
	public void load(Map<?, ?> options) throws IOException {
		
		// merging options 
		if (this.options != null) {
			// Check that the save options do not collide with previous load options
			for (Entry<?, ?> entry : options.entrySet()) {
				Object key = entry.getKey();
				Object value = entry.getValue();
				if (this.options.containsKey(key)) {
					this.options.remove(key);
				}
				this.options.put(key, value);
			}
		}
		
		try {
			isLoading = true;
			if (isLoaded) {
				return;
			} else {
				this.eStore =createResourceEStore(false);
			}
			
			isLoaded = true;
		} finally {
			isLoading = false;
		}
	}



	@Override
	public void save(Map<?, ?> options) throws IOException {
		if (this.options != null) {
			// Check that the save options do not collide with previous load options
			for (Entry<?, ?> entry : options.entrySet()) {
				Object key = entry.getKey();
				Object value = entry.getValue();
				if (this.options.containsKey(key) && value != null) {
					if (!value.equals(this.options.get(key))) {
						throw new IOException(new InvalidOptionsException(MessageFormat.format("key = {0}; value = {1}", key.toString(), value.toString())));
					}
				}
			}
		}

		if (!isLoaded() || !this.isPersistent) {
			//this.connection = createConnection();
			this.isPersistent = true;
			this.eStore = createResourceEStore(true);
			this.isLoaded = true;
		}
	}

//	protected Connection createConnection() throws IOException {
//		Configuration conf = HBaseConfiguration.create();
//		conf.set("hbase.zookeeper.quorum", getURI().host());
//		conf.set("hbase.zookeeper.property.clientPort", getURI().port() != null ? getURI().port() : "2181");
//		return ConnectionFactory.createConnection(conf);
//	}

	@Override
	public EList<EObject> getContents() {
		return new ResourceContentsEStoreEList(DUMMY_ROOT_EOBJECT, ROOT_CONTENTS_ESTRUCTURALFEATURE, eStore());
	}

	@Override
	public EObject getEObject(String uriFragment) {
		EObject eObject = eStore.getEObject(uriFragment);
		if (eObject != null) {
			return eObject;
		} else {
			return super.getEObject(uriFragment);
		}
	}

	@Override
	public String getURIFragment(EObject eObject) {
		if (eObject.eResource() != this) {
			return "/-1";
		} else {
			// Try to adapt as a NeoEMFEObject and return the ID
			NeoEMFEObject neoEMFEObject = NeoEMFEObjectAdapterFactoryImpl.getAdapter(eObject, NeoEMFEObject.class);
			if (neoEMFEObject != null) {
				return (neoEMFEObject.neoemfId());
			}
		}
		return super.getURIFragment(eObject);
	}

	protected void shutdown() throws IOException {
//		this.connection.close();
		this.eStore = null;
		this.isPersistent = false;
	}

	@Override
	protected void doUnload() {
		Iterator<EObject> allContents = getAllProperContents(unloadingContents);
		getErrors().clear();
		getWarnings().clear();
		while (allContents.hasNext()) {
			unloaded((InternalEObject) allContents.next());
		}
		try {
			shutdown();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void finalize() throws Throwable {
		unload();
		super.finalize();
	}

	@Override
	public InternalEObject.EStore eStore() {
		return eStore;
	}

	/**
	 * Creates the {@link SearcheableResourceEStore} used by this
	 * {@link Resource}.
	 * 
	 * @param graph
	 * @return
	 * @throws IOException 
	 */
	protected SearcheableResourceEStore createResourceEStore(boolean isSave) throws IOException {
		if (isSave && this.options.get(NeoEMFResource.OPTIONS_HBASE_READ_ONLY).equals(Boolean.FALSE) ) {
			Exception e = new IllegalArgumentException("Cannot save a read only resource. Please check your resource options !");
			e.printStackTrace();
			}
		if (this.options.get(NeoEMFResource.OPTIONS_HBASE_READ_ONLY).equals(Boolean.FALSE)) {
			this.isPersistent = true;
			if (options.containsKey(NeoEMFResource.OPTIONS_HBASE_JOB_CONTEXT)) {
				try { 
					TaskAttemptContext context = (TaskAttemptContext) options.get(NeoEMFResource.OPTIONS_HBASE_JOB_CONTEXT);
					return new IsSetCachingDelegatedEStoreImpl(new SizeCachingDelegatedEStoreImpl(new DirectWriteHbaseResourceWithCountersEStoreImpl(this, context)));
				} catch (ClassCastException ex) {
					System.err.println("Context should be of type: "+TaskAttemptContext.class);
				}
				
			} 
			return new IsSetCachingDelegatedEStoreImpl(new SizeCachingDelegatedEStoreImpl(new DirectWriteHbaseResourceEStoreImpl(this)));	
		} else {
			if (options.containsKey(NeoEMFResource.OPTIONS_HBASE_JOB_CONTEXT)) {
				try { 
					TaskAttemptContext context = (TaskAttemptContext) options.get(NeoEMFResource.OPTIONS_HBASE_JOB_CONTEXT);
					return new IsSetCachingDelegatedEStoreImpl(new SizeCachingDelegatedEStoreImpl(new ReadOnlyHbaseResourceWithCountersEStoreImpl(this, context)));
				} catch (ClassCastException ex) {
					System.err.println("Context should be of type: "+TaskAttemptContext.class);
				}
			}	
			return new IsSetCachingDelegatedEStoreImpl(new SizeCachingDelegatedEStoreImpl(new ReadOnlyHbaseResourceEStoreImpl(this)));
		
		}
	}
	/**
	 * Creates a read-only {@value SearcheableResourceEStore}
	 * @return
	 * @throws 
	 */
	protected SearcheableResourceEStore createReadOnlyResourceEStore() {
		
		try {
			return new IsSetCachingDelegatedEStoreImpl(new SizeCachingDelegatedEStoreImpl(new ReadOnlyHbaseResourceEStoreImpl(this)));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * A notifying {@link EStoreEList} list implementation for supporting
	 * {@link Resource#getContents}.
	 * 
	 * @author agomez
	 * 
	 */
	protected class ResourceContentsEStoreEList extends EStoreEObjectImpl.EStoreEList<EObject> {
		protected static final long serialVersionUID = 1L;

		protected ResourceContentsEStoreEList(InternalEObject owner, EStructuralFeature eStructuralFeature, EStore store) {
			super(owner, eStructuralFeature, store);
		}

		@Override
		protected EObject validate(int index, EObject object) {
			if (!canContainNull() && object == null) {
				throw new IllegalArgumentException("The 'no null' constraint is violated");
			}
			return object;
		}

		@Override
		public Object getNotifier() {
			return NeoEMFHbaseResourceImpl.this;
		}

		@Override
		public int getFeatureID() {
			return RESOURCE__CONTENTS;
		}

		@Override
		protected boolean isNotificationRequired() {
			return NeoEMFHbaseResourceImpl.this.eNotificationRequired();
		}

		@Override
		protected boolean useEquals() {
			return false;
		}

		@Override
		protected boolean hasInverse() {
			return true;
		}

		@Override
		protected boolean isUnique() {
			return true;
		}

		@Override
		public NotificationChain inverseAdd(EObject object, NotificationChain notifications) {
			InternalEObject eObject = (InternalEObject) object;
			notifications = eObject.eSetResource(NeoEMFHbaseResourceImpl.this, notifications);
			NeoEMFHbaseResourceImpl.this.attached(eObject);
			return notifications;
		}

		@Override
		public NotificationChain inverseRemove(EObject object, NotificationChain notifications) {
			InternalEObject eObject = (InternalEObject) object;
			if (NeoEMFHbaseResourceImpl.this.isLoaded || unloadingContents != null) {
				NeoEMFHbaseResourceImpl.this.detached(eObject);
			}
			return eObject.eSetResource(null, notifications);
		}
		
		@Override
		public boolean add(EObject object)
		{
		    if (isUnique() && contains(object))
		    {
		      return false;
		    }
		    else
		    {
		      addUnique(object);
		      return true;
		    }
		}
		 
		 @Override
		  public void addUnique(EObject object)
		  {
			int index = size() == 0 ? 0 : -1;
			
		    if (isNotificationRequired())
		    {      
		      boolean oldIsSet = isSet();
		      doAddUnique(index, object);
		      NotificationImpl notification = createNotification(Notification.ADD, null, object, index, oldIsSet);
		      if (hasInverse())
		      {
		        NotificationChain notifications = inverseAdd(object, null);
		        if (hasShadow())
		        {
		          notifications = shadowAdd(object, notifications);
		        }
		        if (notifications == null)
		        {
		          dispatchNotification(notification);
		        }
		        else
		        {
		          notifications.add(notification);
		          notifications.dispatch();
		        }
		      }
		      else
		      {
		        dispatchNotification(notification);
		      }
		    }
		    else
		    {
		      doAddUnique(index, object);
		      if (hasInverse())
		      {
		        NotificationChain notifications = inverseAdd(object, null);
		        if (notifications != null) notifications.dispatch();
		      }
		    }
		  }
		 
		@Override
		protected void delegateAdd(int index, EObject object) {
			// FIXME? Maintain a list of hard links to the elements while moving
			// them to the new resource. If a garbage collection happens while
			// traversing the children elements, some unsaved objects that are
			// referenced from a saved object may be garbage collected before
			// they have been completely stored in the DB
			List<EObject> hardLinksList = new ArrayList<>();
			
			// Collect all contents
			hardLinksList.add(object);
			for (Iterator<EObject> it = object.eAllContents(); it.hasNext(); hardLinksList.add(it.next()));

			// The delegate add has to be processed before adding the child elements to the resource
			// so that the root element is created
			super.delegateAdd(index, object);
			
			// Iterate using the hard links list instead the getAllContents
			// We ensure that using the hardLinksList it is not taken out by JIT
			// compiler
			for (EObject element : hardLinksList) {
				NeoEMFInternalEObject internalElement = NeoEMFEObjectAdapterFactoryImpl.getAdapter(element, NeoEMFInternalEObject.class);
				internalElement.neoemfSetResource(NeoEMFHbaseResourceImpl.this);
			}
		}

		@Override
		protected EObject delegateRemove(int index) {
			EObject object = super.delegateRemove(index);
			List<EObject> hardLinksList = new ArrayList<>();
			NeoEMFInternalEObject eObject = NeoEMFEObjectAdapterFactoryImpl.getAdapter(object, NeoEMFInternalEObject.class);
			// Collect all contents
			hardLinksList.add(object);
			for (Iterator<EObject> it = eObject.eAllContents(); it.hasNext(); hardLinksList.add(it.next()));
			// Iterate using the hard links list instead the getAllContents
			// We ensure that using the hardLinksList it is not taken out by JIT
			// compiler
			for (EObject element : hardLinksList) {
				NeoEMFInternalEObject internalElement = NeoEMFEObjectAdapterFactoryImpl.getAdapter(element, NeoEMFInternalEObject.class);
				internalElement.neoemfSetResource(null);
			}
			return object;			
		}
		
		@Override
		protected void didAdd(int index, EObject object) {
			super.didAdd(index, object);
			if (index == size() - 1) {
				loaded();
			}
			modified();
		}

		@Override
		protected void didRemove(int index, EObject object) {
			super.didRemove(index, object);
			modified();
		}

		@Override
		protected void didSet(int index, EObject newObject, EObject oldObject) {
			super.didSet(index, newObject, oldObject);
			modified();
		}

		@Override
		protected void didClear(int oldSize, Object[] oldData) {
			if (oldSize == 0) {
				loaded();
			} else {
				super.didClear(oldSize, oldData);
			}
		}

		protected void loaded() {
			if (!NeoEMFHbaseResourceImpl.this.isLoaded()) {
				Notification notification = NeoEMFHbaseResourceImpl.this.setLoaded(true);
				if (notification != null) {
					NeoEMFHbaseResourceImpl.this.eNotify(notification);
				}
			}
		}

		protected void modified() {
			if (isTrackingModification()) {
				setModified(true);
			}
		}
	}

	public static void shutdownWithoutUnload(NeoEMFHbaseResourceImpl resource) throws IOException {
		resource.shutdown();
	}
}
