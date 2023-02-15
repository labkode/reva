// Copyright 2018-2023 CERN
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

package drives

import (
	"net/http"
	//"time"

	"github.com/ReneKroon/ttlcache/v2"
	"github.com/cs3org/reva/internal/http/services/owncloud/drives/config"
	"github.com/cs3org/reva/pkg/appctx"
	"github.com/cs3org/reva/pkg/rhttp/global"
	"github.com/go-chi/chi/v5"
	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog"
)

func init() {
	global.Register("drives", New)
}

type svc struct {
	c                  *config.Config
	router             *chi.Mux
	warmupCacheTracker *ttlcache.Cache
}

func New(m map[string]interface{}, log *zerolog.Logger) (global.Service, error) {
	conf := &config.Config{}
	if err := mapstructure.Decode(m, conf); err != nil {
		return nil, err
	}

	conf.Init()

	r := chi.NewRouter()
	s := &svc{
		c:      conf,
		router: r,
	}

	if err := s.routerInit(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *svc) Prefix() string {
	return s.c.Prefix
}

func (s *svc) Close() error {
	return nil
}

func (s *svc) Unprotected() []string {
	return []string{""}
	//return []string{"/v1.php/cloud/capabilities", "/v2.php/cloud/capabilities"}
}

func (s *svc) routerInit() error {
	s.router.Route("/v1.0/me/drives", func(r chi.Router) {
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		 })
	})
	return nil
}

func (s *svc) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log := appctx.GetLogger(r.Context())
		log.Debug().Str("path", r.URL.Path).Msg("graph/drives/me routing")
		s.router.ServeHTTP(w, r)
	})
}
