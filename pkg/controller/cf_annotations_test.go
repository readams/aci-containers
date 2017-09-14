// Copyright 2017 Cisco Systems, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"database/sql"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/noironetworks/aci-containers/pkg/cfapi"
	tu "github.com/noironetworks/aci-containers/pkg/testutil"
)

func txn(db *sql.DB, f func (txn *sql.Tx)) {
	txn, _ := db.Begin()
	defer txn.Commit()
	f(txn)
}

func doTestAnnoDBForKind(t *testing.T, kind int) {
	env := testCfEnvironment(t)
	ea_db := EpgAnnotationDb{}

	// add
	txn(env.db, func(txn *sql.Tx) {
		err := ea_db.UpdateAnnotation(txn, "1", kind, "add")
		assert.Nil(t, err)
	})

	// verify
	txn(env.db, func(txn *sql.Tx) {
		val, err := ea_db.GetAnnotation(txn, "1", kind)
		assert.Nil(t, err)
		assert.Equal(t, "add", val)
	})

	// update
	txn(env.db, func(txn *sql.Tx) {
		err := ea_db.UpdateAnnotation(txn, "1", kind, "update")
		assert.Nil(t, err)
	})

	// verify
	txn(env.db, func(txn *sql.Tx) {
		val, err := ea_db.GetAnnotation(txn, "1", kind)
		assert.Nil(t, err)
		assert.Equal(t, "update", val)
	})

	// delete
	txn(env.db, func(txn *sql.Tx) {
		err := ea_db.DeleteAnnotation(txn, "1", kind)
		assert.Nil(t, err)
	})

	// verify
	txn(env.db, func(txn *sql.Tx) {
		val, err := ea_db.GetAnnotation(txn, "1", kind)
		assert.Nil(t, err)
		assert.Equal(t, "", val)
	})

	// delete again
	txn(env.db, func(txn *sql.Tx) {
		err := ea_db.DeleteAnnotation(txn, "1", kind)
		assert.Nil(t, err)
	})
}

func TestAnnotationDbOrgLifecycle(t *testing.T) {
	doTestAnnoDBForKind(t, CF_OBJ_ORG)
}

func TestAnnotationDbSpaceLifecycle(t *testing.T) {
	doTestAnnoDBForKind(t, CF_OBJ_SPACE)
}

func TestAnnotationDbAppLifecycle(t *testing.T) {
	doTestAnnoDBForKind(t, CF_OBJ_APP)
}

func TestAnnotationDbInvalidKind(t *testing.T) {
	env := testCfEnvironment(t)
	ea_db := EpgAnnotationDb{}
	kind := CF_OBJ_LAST

	txn, _ := env.db.Begin()
	err := ea_db.UpdateAnnotation(txn, "1", kind, "add")
	assert.NotNil(t, err)

	err = ea_db.DeleteAnnotation(txn, "1", kind)
	assert.NotNil(t, err)

	_, err = ea_db.GetAnnotation(txn, "1", kind)
	assert.NotNil(t, err)

	txn.Commit()
}

func TestAnnotationDbResolve(t *testing.T) {
	env := testCfEnvironment(t)
	ea_db := EpgAnnotationDb{}

	// resolve -> nothing
	txn(env.db, func(txn *sql.Tx) {
		val, err := ea_db.ResolveAnnotation(txn, "1", "2", "3")
		assert.Nil(t, err)
		assert.Equal(t, "", val)
	})

	// add org annotation
	txn(env.db, func(txn *sql.Tx) {
		err := ea_db.UpdateAnnotation(txn, "3", CF_OBJ_ORG, "org")
		assert.Nil(t, err)
	})

	// resolve and verify
	txn(env.db, func(txn *sql.Tx) {
		val, err := ea_db.ResolveAnnotation(txn, "1", "2", "3")
		assert.Nil(t, err)
		assert.Equal(t, "org", val)
	})

	// add space annotation
	txn(env.db, func(txn *sql.Tx) {
		err := ea_db.UpdateAnnotation(txn, "2", CF_OBJ_SPACE, "space")
		assert.Nil(t, err)
	})

	// resolve and verify
	txn(env.db, func(txn *sql.Tx) {
		val, err := ea_db.ResolveAnnotation(txn, "1", "2", "3")
		assert.Nil(t, err)
		assert.Equal(t, "space", val)
	})

	// add app annotation
	txn(env.db, func(txn *sql.Tx) {
		err := ea_db.UpdateAnnotation(txn, "1", CF_OBJ_APP, "app")
		assert.Nil(t, err)
	})

	// resolve and verify
	txn(env.db, func(txn *sql.Tx) {
		val, err := ea_db.ResolveAnnotation(txn, "1", "2", "3")
		assert.Nil(t, err)
		assert.Equal(t, "app", val)
	})
}

func TestAnnotationHttpPathPrefix(t *testing.T) {
	env := testCfEnvironment(t)
	h := EpgAnnotationHttpHandler{env: env}
	assert.Equal(t, "/networking-aci/epg/", h.Path())
}

func doHttp(t *testing.T, handler *EpgAnnotationHttpHandler, verb, kind, id, data string) (int, string) {
	var rdr io.Reader
	if data != "" {
		str, err := json.Marshal(EpgAnnotationPutMessageBody{Value: data})
		assert.Nil(t, err)
		rdr = strings.NewReader(string(str))
	}
	req := httptest.NewRequest(verb, "http://localhost/networking-aci/epg/" + kind + "/" + id, rdr)
	req.Header.Add("Authorization", "Bearer testtoken")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	resp := w.Result()
	var msg EpgAnnotationGetMessageBody
	if resp.StatusCode == 200 {
		assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
		body, _ := ioutil.ReadAll(resp.Body)
		assert.Nil(t, json.Unmarshal(body, &msg))
		assert.Equal(t, id, msg.Guid)
		assert.Equal(t, kind, msg.Kind)
	}
	return resp.StatusCode, msg.Value
}

func doTestAnnoHttpHandlerForKind(t *testing.T, kind string) {
	env := testCfEnvironment(t)
	h := &EpgAnnotationHttpHandler{env: env}
	k := env.fakeEtcdKeysApi()
	cont_to_check := []string{"c-1", "c-2"}
	if kind == "space" {
		cont_to_check = append(cont_to_check, "c-3")
	}
	if kind == "org" {
		cont_to_check = append(cont_to_check, "c-3", "c-4")
	}
	checkFunc := func(cont, expEpg string) func(bool) (bool, error) {
		return func(last bool) (bool, error) {
			ep := k.GetEpInfo("cell-1", cont)
			if ep != nil {
				return tu.WaitEqual(t, last, expEpg, ep.Epg), nil
			} else {
				return false, nil
			}
		}
	}
	var code int
	var value string
	obj := kind + "-1"

	// get non-existent
	code, value = doHttp(t, h, "GET", kind, obj, "")
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "", value)

	// add
	code, value = doHttp(t, h, "PUT", kind, obj, "add")
	assert.Equal(t, http.StatusNoContent, code)
	assert.Equal(t, "", value)

	// get
	code, value = doHttp(t, h, "GET", kind, obj, "")
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "add", value)

	for _, c := range cont_to_check {
		tu.WaitFor(t, "EPG update on add, cont " + c, 500*time.Millisecond, checkFunc(c, "auto|add"))
	}

	// update
	code, value = doHttp(t, h, "PUT", kind, obj, "update")
	assert.Equal(t, http.StatusNoContent, code)
	assert.Equal(t, "", value)

	// get
	code, value = doHttp(t, h, "GET", kind, obj, "")
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "update", value)

	for _, c := range cont_to_check {
		tu.WaitFor(t, "EPG update on update, cont " + c, 500*time.Millisecond, checkFunc(c, "auto|update"))
	}

	// delete
	code, value = doHttp(t, h, "DELETE", kind, obj, "")
	assert.Equal(t, http.StatusNoContent, code)
	assert.Equal(t, "", value)

	// get
	code, value = doHttp(t, h, "GET", kind, obj, "")
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "", value)

	for _, c := range cont_to_check {
		tu.WaitFor(t, "EPG update on delete, cont " + c, 500*time.Millisecond, checkFunc(c, "default|cf-app-default"))
	}

	// delete non-existent
	code, value = doHttp(t, h, "DELETE", kind, obj, "")
	assert.Equal(t, http.StatusNoContent, code)
	assert.Equal(t, "", value)
}

func TestAnnotationHttpOrgLifecycle(t *testing.T) {
	doTestAnnoHttpHandlerForKind(t, "org")
}

func TestAnnotationHttpSpaceLifecycle(t *testing.T) {
	doTestAnnoHttpHandlerForKind(t, "space")
}

func TestAnnotationHttpAppLifecycle(t *testing.T) {
	doTestAnnoHttpHandlerForKind(t, "app")
}

func TestAnnotationHttpInvalidKind(t *testing.T) {
	env := testCfEnvironment(t)
	h := &EpgAnnotationHttpHandler{env: env}
	kind := "foo"

	code, _ := doHttp(t, h, "GET", kind, "some-obj", "")
	assert.Equal(t, http.StatusNotFound, code)

	code, _ = doHttp(t, h, "PUT", kind, "some-obj", "")
	assert.Equal(t, http.StatusNotFound, code)

	code, _ = doHttp(t, h, "DELETE", kind, "some-obj", "")
	assert.Equal(t, http.StatusNotFound, code)
}

func TestAnnotationHttpInvalidPath(t *testing.T) {
	env := testCfEnvironment(t)
	handler := EpgAnnotationHttpHandler{env: env}
	paths := map[string]int{
		"foo": http.StatusInternalServerError,
		"networking-aci/epg/": http.StatusNotFound,
		"networking-aci/epg/app": http.StatusNotFound,
		"networking-aci/epg/org/foo/bar": http.StatusNotFound,
	}
	for p, c := range paths {
		req := httptest.NewRequest("GET", "http://localhost/" + p, nil)
		req.Header.Add("Authorization", "Bearer testtoken")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		resp := w.Result()
		assert.Equal(t, c, resp.StatusCode)
	}
}

func TestAnnotationHttpInvalidMethod(t *testing.T) {
	env := testCfEnvironment(t)
	h := &EpgAnnotationHttpHandler{env: env}

	for _, v := range []string{"POST", "HEAD", "PATCH"} {
		code, _ := doHttp(t, h, v, "org", "some-obj", "")
		assert.Equal(t, http.StatusMethodNotAllowed, code)
	}
}

func TestAnnotationHttpInvalidPutBody(t *testing.T) {
	env := testCfEnvironment(t)
	handler := EpgAnnotationHttpHandler{env: env}

	for _, kind := range []string{"org", "space", "app"} {
		for _, body := range []string{"{\"value\": \"\"}", "{", "{\"foo\": 123}"} {
			req := httptest.NewRequest("PUT", "http://localhost/networking-aci/epg/" + kind + "/123",
									  strings.NewReader(body))
			req.Header.Add("Authorization", "Bearer testtoken")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			resp := w.Result()
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		}
	}
}

func doTestAnnoHttpHandlerAuthForKind(t *testing.T, kind string) {
	env := testCfEnvironment(t)
	cc := env.fakeCcClient()
	ri := cfapi.NewUserRoleInfo("some")
	ri.Spaces["space-one"] = struct{}{}
	ri.AuditedSpaces["space-two"] = struct{}{}
	ri.ManagedSpaces["space-three"] = struct{}{}
	ri.Organizations["org-one"] = struct{}{}
	ri.AuditedOrganizations["org-two"] = struct{}{}
	ri.ManagedOrganizations["org-three"] = struct{}{}
	cc.GetUserRoleInfo_response = ri

	auth := env.fakeCfAuthClient()
	auth.FetchTokenInfo_response = &cfapi.TokenInfo{Scope: []string{}, UserId: "some", UserName: "someone"}

	h := &EpgAnnotationHttpHandler{env: env}
	var code int

	cc.GetAppSpace_response = "space-zero"
	code, _ = doHttp(t, h, "GET", kind, "zero", "")
	assert.Equal(t, http.StatusForbidden, code)

	code, _ = doHttp(t, h, "PUT", kind, "zero", "a")
	assert.Equal(t, http.StatusForbidden, code)

	code, _ = doHttp(t, h, "DELETE", kind, "zero", "")
	assert.Equal(t, http.StatusForbidden, code)

	cc.GetAppSpace_response = "space-one"
	code, _ = doHttp(t, h, "GET", kind, kind + "-one", "")
	assert.Equal(t, http.StatusNotFound, code)

	code, _ = doHttp(t, h, "PUT", kind, kind + "-one", "a")
	assert.Equal(t, http.StatusForbidden, code)

	code, _ = doHttp(t, h, "DELETE", kind, kind + "-one", "")
	assert.Equal(t, http.StatusForbidden, code)

	cc.GetAppSpace_response = "space-two"
	code, _ = doHttp(t, h, "GET", kind, kind + "-two", "")
	assert.Equal(t, http.StatusNotFound, code)

	code, _ = doHttp(t, h, "PUT", kind, kind + "-two", "a")
	assert.Equal(t, http.StatusForbidden, code)

	code, _ = doHttp(t, h, "DELETE", kind, kind + "-two", "")
	assert.Equal(t, http.StatusForbidden, code)

	cc.GetAppSpace_response = "space-three"
	code, _ = doHttp(t, h, "PUT", kind, kind + "-three", "a")
	assert.Equal(t, http.StatusNoContent, code)

	code, _ = doHttp(t, h, "GET", kind, kind + "-three", "")
	assert.Equal(t, http.StatusOK, code)

	code, _ = doHttp(t, h, "DELETE", kind, kind + "-three", "")
	assert.Equal(t, http.StatusNoContent, code)
}

func TestAnnotationHttpAuthOrg(t *testing.T) {
	doTestAnnoHttpHandlerAuthForKind(t, "org")
}

func TestAnnotationHttpAuthSpace(t *testing.T) {
	doTestAnnoHttpHandlerAuthForKind(t, "space")
}

func TestAnnotationHttpAuthApp(t *testing.T) {
	doTestAnnoHttpHandlerAuthForKind(t, "app")
}

