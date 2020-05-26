// Copyright 2018-2020 CERN
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
//
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	link "github.com/cs3org/go-cs3apis/cs3/sharing/link/v1beta1"
	"github.com/cs3org/reva/pkg/appctx"
	"github.com/cs3org/reva/pkg/rgrpc/status"
	"github.com/cs3org/reva/pkg/rgrpc/todo/pool"
	"github.com/golang/protobuf/jsonpb"
	"github.com/pkg/errors"
)

// GrantManager is a proof of concept that allows for having persistent link grants stored
// on a map structure for convenient access.
type GrantManager struct {
	lock sync.Mutex
	path string // path to db file.
}

// NewGrantManager initializes a new GrantManager.
func NewGrantManager(path string) (*GrantManager, error) {
	g := GrantManager{
		lock: sync.Mutex{},
		path: path,
	}

	_, err := os.Stat(g.path)
	if os.IsNotExist(err) {
		if err := ioutil.WriteFile(g.path, []byte("{}"), 0700); err != nil {
			err = errors.Wrap(err, "error opening/creating the file: "+g.path)
			return nil, err
		}
	}

	fileContents, err := ioutil.ReadFile(g.path)
	if err != nil {
		return nil, err
	}
	if len(fileContents) == 0 {
		err := ioutil.WriteFile(g.path, []byte("{}"), 0644)
		if err != nil {
			return nil, err
		}
	}

	return &g, nil
}

// Write writes a grant to the database.
func (g *GrantManager) Write(token string, l *link.Grant) error {
	g.lock.Lock()
	defer g.lock.Unlock()

	// u := jsonpb.Unmarshaler{}
	m := jsonpb.Marshaler{}

	buff := bytes.Buffer{}
	if err := m.Marshal(&buff, l); err != nil {
		return err
	}

	db := map[string]interface{}{}
	fileContents, err := ioutil.ReadFile(g.path)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(fileContents, &db); err != nil {
		return err
	}

	if _, ok := db[token]; !ok {
		db[token] = buff.String()
	} else {
		return errors.New("duplicated entry")
	}

	destJSON, err := json.Marshal(db)
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(g.path, destJSON, 0644); err != nil {
		return err
	}

	return nil
}

// ReadFromToken gets a grant from token.
func (g *GrantManager) ReadFromToken(token string) (*link.Grant, error) {
	db := map[string]interface{}{}
	u := jsonpb.Unmarshaler{}

	fileContents, err := ioutil.ReadFile(g.path)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(fileContents, &db); err != nil {
		return nil, err
	}

	grant := link.Grant{}
	r := bytes.NewBuffer([]byte(db[token].(string)))
	if err := u.Unmarshal(r, &grant); err != nil {
		return nil, err
	}

	return &grant, nil
}

func (s *svc) CreatePublicShare(ctx context.Context, req *link.CreatePublicShareRequest) (*link.CreatePublicShareResponse, error) {
	log := appctx.GetLogger(ctx)
	log.Info().Msg("create public share")

	if s.c.LinkGrantsFile == "" {
		return nil, fmt.Errorf("public manager used but no `link_grants_file` defined; define link_grants_file on the gateway in order to store link grants")
	}

	m, err := NewGrantManager(s.c.LinkGrantsFile)
	if err != nil {
		return nil, err
	}

	c, err := pool.GetPublicShareProviderClient(s.c.PublicShareProviderEndpoint)
	if err != nil {
		return nil, err
	}

	res, err := c.CreatePublicShare(ctx, req)
	if err != nil {
		return nil, err
	}

	if err := m.Write(res.Share.Token, req.Grant); err != nil {
		return nil, err
	}

	return res, nil
}

func (s *svc) RemovePublicShare(ctx context.Context, req *link.RemovePublicShareRequest) (*link.RemovePublicShareResponse, error) {
	log := appctx.GetLogger(ctx)
	log.Info().Msg("remove public share")

	return &link.RemovePublicShareResponse{
		Status: status.NewOK(ctx),
	}, nil
}

func (s *svc) GetPublicShareByToken(ctx context.Context, req *link.GetPublicShareByTokenRequest) (*link.GetPublicShareByTokenResponse, error) {
	log := appctx.GetLogger(ctx)
	log.Info().Msg("get public share by token")

	driver, err := pool.GetPublicShareProviderClient(s.c.PublicShareProviderEndpoint)
	if err != nil {
		return nil, err
	}

	pass := req.GetPassword()
	res, err := driver.GetPublicShareByToken(ctx, req)
	if err != nil {
		return nil, err
	}

	m, err := NewGrantManager(s.c.LinkGrantsFile)
	if err != nil {
		return nil, err
	}

	// here
	gr, err := m.ReadFromToken(req.Token)
	if err != nil {
		return nil, err
	}

	if res.Share.PasswordProtected && (gr.Password != pass) {
		return nil, fmt.Errorf("public share password missmatch")
	}

	return res, nil
}

func (s *svc) GetPublicShare(ctx context.Context, req *link.GetPublicShareRequest) (*link.GetPublicShareResponse, error) {
	log := appctx.GetLogger(ctx)
	log.Info().Msg("get public share")

	pClient, err := pool.GetPublicShareProviderClient(s.c.PublicShareProviderEndpoint)
	if err != nil {
		log.Err(err).Msg("error connecting to a public share provider")
		return &link.GetPublicShareResponse{
			Status: &rpc.Status{
				Code: rpc.Code_CODE_INTERNAL,
			},
		}, nil
	}

	return pClient.GetPublicShare(ctx, req)
}

func (s *svc) ListPublicShares(ctx context.Context, req *link.ListPublicSharesRequest) (*link.ListPublicSharesResponse, error) {
	log := appctx.GetLogger(ctx)
	log.Info().Msg("listing public shares")

	pClient, err := pool.GetPublicShareProviderClient(s.c.PublicShareProviderEndpoint)
	if err != nil {
		log.Err(err).Msg("error connecting to a public share provider")
		return &link.ListPublicSharesResponse{
			Status: &rpc.Status{
				Code: rpc.Code_CODE_INTERNAL,
			},
		}, nil
	}

	res, err := pClient.ListPublicShares(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "error listing shares")
	}

	return res, nil
}

func (s *svc) UpdatePublicShare(ctx context.Context, req *link.UpdatePublicShareRequest) (*link.UpdatePublicShareResponse, error) {
	log := appctx.GetLogger(ctx)
	log.Info().Msg("update public share")

	pClient, err := pool.GetPublicShareProviderClient(s.c.PublicShareProviderEndpoint)
	if err != nil {
		log.Err(err).Msg("error connecting to a public share provider")
		return &link.UpdatePublicShareResponse{
			Status: &rpc.Status{
				Code: rpc.Code_CODE_INTERNAL,
			},
		}, nil
	}

	res, err := pClient.UpdatePublicShare(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "error updating share")
	}
	return res, nil
}
