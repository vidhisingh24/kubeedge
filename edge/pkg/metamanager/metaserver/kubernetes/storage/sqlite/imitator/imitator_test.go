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

package imitator

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kubeedge/api/apis/componentconfig/edgecore/v1alpha2"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao/dbclient"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao/models"
	"github.com/kubeedge/kubeedge/pkg/metaserver/util"
)

func newObj(apiVersion, kind, namespace, name, rv string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion(apiVersion)
	obj.SetKind(kind)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	obj.SetResourceVersion(rv)
	return obj
}

var groupSeq atomic.Int32

// setup initializes the database and returns an API group never used before,
// since the default RESTMapper keeps what it learned for the whole process.
func setup(base string) string {
	dao.Init("file:imitatortest?mode=memory&cache=shared", &v1alpha2.MetaManager{Enable: true})
	initRESTMapper(util.DefaultRESTMapper())
	return fmt.Sprintf("g%d.%s", groupSeq.Add(1), base)
}

func names(t *testing.T, key string) []string {
	resp, err := DefaultV2Client.List(context.TODO(), key)
	require.NoError(t, err)
	var ret []string
	for _, m := range *resp.Kvs {
		ret = append(ret, m.Name)
	}
	return ret
}

// TestObjectsSavedUnderGuessedResourceAreMoved reproduces an upgrade: objects
// of a kind with an irregular plural were saved under the guessed resource by a
// previous version, and must be served under the real resource once its
// discovery document, cached by the previous version, is loaded.
func TestObjectsSavedUnderGuessedResourceAreMoved(t *testing.T) {
	group := setup("istio.io")
	gv := group + "/v1alpha3"
	ctx := context.TODO()

	// Saved before the mapping is known: Gateway is guessed as gatewaies.
	require.NoError(t, DefaultV2Client.InsertOrUpdateObj(ctx, newObj(gv, "Gateway", "default", "gw1", "10")))
	require.NoError(t, DefaultV2Client.InsertOrUpdateObj(ctx, newObj(gv, "Gateway", "default", "gw2", "11")))
	assert.ElementsMatch(t, []string{"gw1", "gw2"}, names(t, "/"+gv+"/gatewaies/null/null"))
	assert.Empty(t, names(t, "/"+gv+"/gateways/null/null"))

	doc, err := json.Marshal(&metav1.APIResourceList{
		TypeMeta:     metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
		GroupVersion: gv,
		APIResources: []metav1.APIResource{{Name: "gateways", Namespaced: true, Kind: "Gateway"}},
	})
	require.NoError(t, err)
	require.NoError(t, DefaultV2Client.InsertOrUpdatePassThroughObj(ctx, doc, "/apis/"+gv))

	// What StorageInit does when edgecore starts.
	initRESTMapper(util.DefaultRESTMapper())

	assert.Empty(t, names(t, "/"+gv+"/gatewaies/null/null"))
	assert.ElementsMatch(t, []string{"gw1", "gw2"}, names(t, "/"+gv+"/gateways/null/null"))
	obj, err := DefaultV2Client.Get(ctx, "/"+gv+"/gateways/default/gw1")
	require.NoError(t, err)
	assert.Len(t, *obj.Kvs, 1)

	// Objects saved from now on use the real resource as well.
	require.NoError(t, DefaultV2Client.InsertOrUpdateObj(ctx, newObj(gv, "Gateway", "default", "gw3", "12")))
	assert.ElementsMatch(t, []string{"gw1", "gw2", "gw3"}, names(t, "/"+gv+"/gateways/null/null"))

	// Running StorageInit again, e.g. after a module restart, is harmless.
	initRESTMapper(util.DefaultRESTMapper())
	assert.ElementsMatch(t, []string{"gw1", "gw2", "gw3"}, names(t, "/"+gv+"/gateways/null/null"))
}

// TestMigrateResourceOnlyMovesObjectsOfTheKind checks that objects of another
// kind saved under the old resource stay where they are.
func TestMigrateResourceOnlyMovesObjectsOfTheKind(t *testing.T) {
	group := setup("example.com")
	gv := group + "/v1"
	ctx := context.TODO()

	// Mailbox is guessed as mailboxs.
	require.NoError(t, DefaultV2Client.InsertOrUpdateObj(ctx, newObj(gv, "Mailbox", "default", "box", "20")))
	stranger, err := json.Marshal(newObj(gv, "Stranger", "default", "stranger", "21"))
	require.NoError(t, err)
	oldGVR := schema.GroupVersionResource{Group: group, Version: "v1", Resource: "mailboxs"}
	require.NoError(t, dbclient.NewMetaV2Service().InsertOrReplaceMetaV2(&models.MetaV2{
		Key:                  "/" + gv + "/mailboxs/default/stranger",
		GroupVersionResource: oldGVR.String(),
		Namespace:            "default",
		Name:                 "stranger",
		Value:                string(stranger),
	}))
	assert.ElementsMatch(t, []string{"box", "stranger"}, names(t, "/"+gv+"/mailboxs/null/null"))

	util.DefaultRESTMapper().RegisterMapping(oldGVR.GroupVersion().WithResource("mailboxes"), "Mailbox")

	assert.ElementsMatch(t, []string{"stranger"}, names(t, "/"+gv+"/mailboxs/null/null"))
	assert.ElementsMatch(t, []string{"box"}, names(t, "/"+gv+"/mailboxes/null/null"))
}
