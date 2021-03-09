/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

import (
	"context"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// NewDelegatingClientInput encapsulates the input parameters to create a new delegating client.
type NewDelegatingClientInput struct {
	CacheReader Reader
	Client      Client
	// zhou: objects that disable read cache
	UncachedObjects []Object
	// zhou: default value is false, which means we doesn't cache unstructured objects.
	CacheUnstructured bool
}

// zhou: implement Client interface
//       Used by cluster.DefaultNewClient()

// NewDelegatingClient creates a new delegating client.
//
// A delegating client forms a Client by composing separate reader, writer and
// statusclient interfaces.  This way, you can have an Client that reads from a
// cache and writes to the API server.
func NewDelegatingClient(in NewDelegatingClientInput) (Client, error) {
	uncachedGVKs := map[schema.GroupVersionKind]struct{}{}
	for _, obj := range in.UncachedObjects {
		gvk, err := apiutil.GVKForObject(obj, in.Client.Scheme())
		if err != nil {
			return nil, err
		}
		uncachedGVKs[gvk] = struct{}{}
	}

	return &delegatingClient{
		scheme: in.Client.Scheme(),
		mapper: in.Client.RESTMapper(),
		Reader: &delegatingReader{
			// zhou: created by cache.New()
			CacheReader: in.CacheReader,
			// zhou: created by client.New(), for read/write directly from apiserver
			ClientReader: in.Client,
			scheme:       in.Client.Scheme(),
			// zhou: objects disable read cache
			uncachedGVKs: uncachedGVKs,
			// zhou: default value is false.
			cacheUnstructured: in.CacheUnstructured,
		},
		Writer:       in.Client,
		StatusClient: in.Client,
	}, nil
}

// zhou: implement Client interface
type delegatingClient struct {
	// zhou: delegatingReader struct implement Reader interface
	Reader
	// zhou: using client.client Writer and StatusClient implementation
	Writer
	StatusClient

	scheme *runtime.Scheme
	mapper meta.RESTMapper
}

// Scheme returns the scheme this client is using.
func (d *delegatingClient) Scheme() *runtime.Scheme {
	return d.scheme
}

// RESTMapper returns the rest mapper this client is using.
func (d *delegatingClient) RESTMapper() meta.RESTMapper {
	return d.mapper
}

// zhou: implement Reader interface

// delegatingReader forms a Reader that will cause Get and List requests for
// unstructured types to use the ClientReader while requests for any other type
// of object with use the CacheReader.  This avoids accidentally caching the
// entire cluster in the common case of loading arbitrary unstructured objects
// (e.g. from OwnerReferences).
type delegatingReader struct {
	// zhou: cache.cache
	CacheReader Reader
	// zhou: client.client struct
	ClientReader Reader

	uncachedGVKs      map[schema.GroupVersionKind]struct{}
	scheme            *runtime.Scheme
	cacheUnstructured bool
}

// zhou: checking list of objects that disabled read cache.

func (d *delegatingReader) shouldBypassCache(obj runtime.Object) (bool, error) {
	gvk, err := apiutil.GVKForObject(obj, d.scheme)
	if err != nil {
		return false, err
	}
	// TODO: this is producing unsafe guesses that don't actually work,
	// but it matches ~99% of the cases out there.
	if meta.IsListType(obj) {
		gvk.Kind = strings.TrimSuffix(gvk.Kind, "List")
	}
	if _, isUncached := d.uncachedGVKs[gvk]; isUncached {
		return true, nil
	}
	if !d.cacheUnstructured {
		_, isUnstructured := obj.(*unstructured.Unstructured)
		_, isUnstructuredList := obj.(*unstructured.UnstructuredList)
		return isUnstructured || isUnstructuredList, nil
	}
	return false, nil
}

// zhou: manager.GetClient().Get() -> here

// Get retrieves an obj for a given object key from the Kubernetes Cluster.
func (d *delegatingReader) Get(ctx context.Context, key ObjectKey, obj Object, opts ...GetOption) error {
	if isUncached, err := d.shouldBypassCache(obj); err != nil {
		return err
	} else if isUncached {
		// zhou: read from apiserver, typedClient.Get()/ unstructuredClient.Get()
		return d.ClientReader.Get(ctx, key, obj, opts...)
	}
	return d.CacheReader.Get(ctx, key, obj, opts...)
}

// List retrieves list of objects for a given namespace and list options.
func (d *delegatingReader) List(ctx context.Context, list ObjectList, opts ...ListOption) error {
	if isUncached, err := d.shouldBypassCache(list); err != nil {
		return err
	} else if isUncached {
		return d.ClientReader.List(ctx, list, opts...)
	}
	return d.CacheReader.List(ctx, list, opts...)
}
