/*
Copyright 2026 The KubeEdge Authors.

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

package util

import (
	"encoding/json"
	"reflect"
	"strings"
	"sync"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	apidiscoveryv2beta1 "k8s.io/api/apidiscovery/v2beta1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"

	kubeedgescheme "github.com/kubeedge/api/client/clientset/versioned/scheme"
)

// MappingChangedFunc is called when ResourceFor starts returning a different
// resource for a kind. Storage that keys objects by resource uses it to move
// the objects saved under the old resource.
type MappingChangedFunc func(gvk schema.GroupVersionKind, oldGVR, newGVR schema.GroupVersionResource)

// RESTMapper maps kinds to resources and back.
//
// Lookups are answered, in order, from:
//  1. mappings learned at runtime from authoritative sources: API discovery
//     documents, and objects returned by the API server for a known resource;
//  2. the resource types registered in the client-go and KubeEdge schemes,
//     and CustomResourceDefinitions;
//  3. the string rules of UnsafeKindToResource and UnsafeResourceToKind, only
//     for types never seen through 1 or 2.
type RESTMapper struct {
	lock sync.RWMutex

	learnedKindToResource map[schema.GroupVersionKind]schema.GroupVersionResource
	learnedResourceToKind map[schema.GroupVersionResource]schema.GroupVersionKind

	builtinKindToResource map[schema.GroupVersionKind]schema.GroupVersionResource
	builtinResourceToKind map[schema.GroupVersionResource]schema.GroupVersionKind

	listeners []MappingChangedFunc
}

var defaultRESTMapper = NewRESTMapper()

// DefaultRESTMapper returns the RESTMapper shared by the process.
func DefaultRESTMapper() *RESTMapper {
	return defaultRESTMapper
}

// NewRESTMapper returns a RESTMapper that knows the types of the client-go and
// KubeEdge schemes, and CustomResourceDefinitions.
func NewRESTMapper() *RESTMapper {
	m := &RESTMapper{
		learnedKindToResource: map[schema.GroupVersionKind]schema.GroupVersionResource{},
		learnedResourceToKind: map[schema.GroupVersionResource]schema.GroupVersionKind{},
		builtinKindToResource: map[schema.GroupVersionKind]schema.GroupVersionResource{},
		builtinResourceToKind: map[schema.GroupVersionResource]schema.GroupVersionKind{},
	}
	for _, s := range []*runtime.Scheme{scheme.Scheme, kubeedgescheme.Scheme} {
		m.addScheme(s)
	}
	// Registered directly rather than through the apiextensions scheme, which
	// edgecore does not otherwise depend on.
	for _, version := range []string{"v1", "v1beta1"} {
		m.addBuiltin(schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: version, Kind: "CustomResourceDefinition"})
	}
	return m
}

var objectMetaAccessor = reflect.TypeOf((*metav1.Object)(nil)).Elem()

// addScheme indexes the top-level object types of s. Kubernetes and KubeEdge
// name the resources of their own types with the standard plural rules, so
// meta.UnsafeGuessKindToResource is exact for them.
func (m *RESTMapper) addScheme(s *runtime.Scheme) {
	for gvk, t := range s.AllKnownTypes() {
		if gvk.Version == runtime.APIVersionInternal || strings.HasSuffix(gvk.Kind, "List") {
			continue
		}
		// Options and other types without ObjectMeta are not resources.
		if !reflect.PointerTo(t).Implements(objectMetaAccessor) {
			continue
		}
		m.addBuiltin(gvk)
	}
}

func (m *RESTMapper) addBuiltin(gvk schema.GroupVersionKind) {
	gvr, _ := meta.UnsafeGuessKindToResource(gvk)
	m.builtinKindToResource[gvk] = gvr
	m.builtinResourceToKind[gvr] = gvk
}

// AddMappingChangedListener registers fn to be called whenever ResourceFor
// starts returning a different resource for a kind.
func (m *RESTMapper) AddMappingChangedListener(fn MappingChangedFunc) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.listeners = append(m.listeners, fn)
}

// RegisterMapping records the authoritative mapping between gvr and kind. It
// returns true if the mapping was not known before.
func (m *RESTMapper) RegisterMapping(gvr schema.GroupVersionResource, kind string) bool {
	if gvr.Resource == "" || kind == "" || strings.Contains(gvr.Resource, "/") {
		return false
	}
	gvk := gvr.GroupVersion().WithKind(kind)

	m.lock.Lock()
	if m.learnedKindToResource[gvk] == gvr && m.learnedResourceToKind[gvr] == gvk {
		m.lock.Unlock()
		return false
	}
	oldGVR := m.resourceForLocked(gvk)
	m.learnedKindToResource[gvk] = gvr
	m.learnedResourceToKind[gvr] = gvk
	listeners := append([]MappingChangedFunc(nil), m.listeners...)
	m.lock.Unlock()

	klog.V(4).Infof("[metaserver] learned mapping %s <-> %s", gvk.String(), gvr.String())
	if oldGVR != gvr {
		for _, fn := range listeners {
			fn(gvk, oldGVR, gvr)
		}
	}
	return true
}

// HasMapping reports whether the kind of gvr comes from an authoritative
// source, i.e. whether KindFor does not need to guess it.
func (m *RESTMapper) HasMapping(gvr schema.GroupVersionResource) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if _, ok := m.learnedResourceToKind[gvr]; ok {
		return true
	}
	_, ok := m.builtinResourceToKind[gvr]
	return ok
}

// ResourceFor returns the resource of gvk.
func (m *RESTMapper) ResourceFor(gvk schema.GroupVersionKind) schema.GroupVersionResource {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.resourceForLocked(gvk)
}

func (m *RESTMapper) resourceForLocked(gvk schema.GroupVersionKind) schema.GroupVersionResource {
	if gvr, ok := m.learnedKindToResource[gvk]; ok {
		return gvr
	}
	if gvr, ok := m.builtinKindToResource[gvk]; ok {
		return gvr
	}
	return gvk.GroupVersion().WithResource(UnsafeKindToResource(gvk.Kind))
}

// KindFor returns the kind of gvr.
func (m *RESTMapper) KindFor(gvr schema.GroupVersionResource) schema.GroupVersionKind {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if gvk, ok := m.learnedResourceToKind[gvr]; ok {
		return gvk
	}
	if gvk, ok := m.builtinResourceToKind[gvr]; ok {
		return gvk
	}
	return gvr.GroupVersion().WithKind(UnsafeResourceToKind(gvr.Resource))
}

// ListKindFor returns the kind of a list of gvr.
func (m *RESTMapper) ListKindFor(gvr schema.GroupVersionResource) schema.GroupVersionKind {
	gvk := m.KindFor(gvr)
	gvk.Kind += "List"
	return gvk
}

// LearnFromObject records the mapping between gvr and the kind of obj, where
// obj is an object or a list of objects returned by the API server for gvr.
func (m *RESTMapper) LearnFromObject(gvr schema.GroupVersionResource, obj runtime.Object) {
	if obj == nil || gvr.Resource == "" {
		return
	}
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind == "" || gvk.GroupVersion() != gvr.GroupVersion() {
		return
	}
	kind := gvk.Kind
	if meta.IsListType(obj) {
		if !strings.HasSuffix(kind, "List") {
			return
		}
		kind = strings.TrimSuffix(kind, "List")
	}
	m.RegisterMapping(gvr, kind)
}

// LearnFromDiscovery records the mappings of a discovery document: either the
// APIResourceList served at /api/<version> and /apis/<group>/<version>, or the
// aggregated APIGroupDiscoveryList served at /api and /apis. Other documents
// are ignored. It returns the number of mappings that were not known before.
func (m *RESTMapper) LearnFromDiscovery(data []byte) int {
	var typeMeta metav1.TypeMeta
	if err := json.Unmarshal(data, &typeMeta); err != nil {
		return 0
	}
	learned := 0
	register := func(gvr schema.GroupVersionResource, kind string) {
		if m.RegisterMapping(gvr, kind) {
			learned++
		}
	}

	switch {
	case typeMeta.Kind == "APIResourceList":
		var list metav1.APIResourceList
		if err := json.Unmarshal(data, &list); err != nil {
			return 0
		}
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			return 0
		}
		for _, r := range list.APIResources {
			// Subresources, such as pods/status, are not stored on their own.
			if strings.Contains(r.Name, "/") {
				continue
			}
			register(gv.WithResource(r.Name), r.Kind)
		}

	case typeMeta.Kind == "APIGroupDiscoveryList" && typeMeta.APIVersion == apidiscoveryv2.SchemeGroupVersion.String():
		var list apidiscoveryv2.APIGroupDiscoveryList
		if err := json.Unmarshal(data, &list); err != nil {
			return 0
		}
		for _, group := range list.Items {
			for _, version := range group.Versions {
				gv := schema.GroupVersion{Group: group.Name, Version: version.Version}
				for _, r := range version.Resources {
					if r.ResponseKind != nil {
						register(gv.WithResource(r.Resource), r.ResponseKind.Kind)
					}
				}
			}
		}

	case typeMeta.Kind == "APIGroupDiscoveryList" && typeMeta.APIVersion == apidiscoveryv2beta1.SchemeGroupVersion.String():
		var list apidiscoveryv2beta1.APIGroupDiscoveryList
		if err := json.Unmarshal(data, &list); err != nil {
			return 0
		}
		for _, group := range list.Items {
			for _, version := range group.Versions {
				gv := schema.GroupVersion{Group: group.Name, Version: version.Version}
				for _, r := range version.Resources {
					if r.ResponseKind != nil {
						register(gv.WithResource(r.Resource), r.ResponseKind.Kind)
					}
				}
			}
		}
	}
	return learned
}

// ResourceFor returns the resource of gvk using the default RESTMapper.
func ResourceFor(gvk schema.GroupVersionKind) schema.GroupVersionResource {
	return defaultRESTMapper.ResourceFor(gvk)
}

// KindFor returns the kind of gvr using the default RESTMapper.
func KindFor(gvr schema.GroupVersionResource) schema.GroupVersionKind {
	return defaultRESTMapper.KindFor(gvr)
}

// ListKindFor returns the kind of a list of gvr using the default RESTMapper.
func ListKindFor(gvr schema.GroupVersionResource) schema.GroupVersionKind {
	return defaultRESTMapper.ListKindFor(gvr)
}
