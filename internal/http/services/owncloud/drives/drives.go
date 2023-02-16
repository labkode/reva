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
			payload := `
{
   "value":[
      {
         "description":"Subtitle",
         "driveAlias":"/eos/project/c/cernbox",
         "driveType":"project",
         "id":"b1cd12df-349d-4084-a343-2ecc7aeff22e$b104d3cf-ab00-45fc-8b7d-5d92e7c96722",
         "lastModifiedDateTime":"2023-02-15T14:21:14.372351719Z",
         "name":"CERNBox",
         "owner":{
            "user":{
               "displayName":"cernbox-admins",
               "id":"b104d3cf-ab00-45fc-8b7d-5d92e7c96722"
            }
         },
         "quota":{
            "remaining":999999988,
            "state":"normal",
            "total":1000000000,
            "used":12
         },
         "root":{
            "eTag":"\"0d7e2dc1d7c21f70996344342464b765\"",
            "id":"b1cd12df-349d-4084-a343-2ecc7aeff22e$b104d3cf-ab00-45fc-8b7d-5d92e7c96722",
            "permissions":[
               {
                  "grantedToIdentities":[
                     {
                        "user":{
                           "displayName":"Albert Einstein",
                           "id":"4c510ada-c86b-4815-8820-42cdf82c3d51"
                        }
                     }
                  ],
                  "roles":[
                     "manager"
                  ]
               }
            ],
            "webDavUrl":"https://qa.cernbox.cern.ch/cernbox/desktop/remote.php/webdav/eos/user/g/gonzalhu"
         },
         "special":[
            {
               "eTag":"\"1ee3d6fe6cc57bca02579598b7f1ab83\"",
               "file":{
                  "mimeType":"text/markdown"
               },
               "id":"b1cd12df-349d-4084-a343-2ecc7aeff22e$b104d3cf-ab00-45fc-8b7d-5d92e7c96722!c92343c9-b3e0-4713-9f3b-f0a7f34964ed",
               "lastModifiedDateTime":"2023-02-15T14:21:14.370703927Z",
               "name":"readme.md",
               "size":12,
               "specialFolder":{
                  "name":"readme"
               },
               "webDavUrl":"https://cernbox.cern.ch/cernbox/desktop/remote.php/webdav/eos/project/c/cernbox/.space/readme.md"
            }
         ],
         "webUrl":"https://cernbox.cern.ch"
      },
      {
         "driveAlias":"/eos/user/g/gonzalhu",
         "driveType":"personal",
         "id":"b1cd12df-349d-4084-a343-2ecc7aeff22e$4c510ada-c86b-4815-8820-42cdf82c3d51",
         "lastModifiedDateTime":"2023-02-15T13:59:44.21698901Z",
         "name":"gonzalhu",
         "owner":{
            "user":{
               "displayName":"Hugo Gonzalez",
               "id":"4c510ada-c86b-4815-8820-42cdf82c3d51"
            }
         },
         "quota":{
            "remaining":50116665344,
            "state":"normal",
            "total":0,
            "used":0
         },
         "root":{
            "eTag":"\"8586495a490145412aa3699772d6350c\"",
            "id":"b1cd12df-349d-4084-a343-2ecc7aeff22e$4c510ada-c86b-4815-8820-42cdf82c3d51",
            "webDavUrl":"https://cernbox.cern.ch/cernbox/desktop/remote.php/webdav/eos/user/g/gonzalhu/"
         },
         "webUrl":"https://cernbox.cern.ch/files/spaces/eos/user/g/gonzalhu"
      },
      {
         "driveAlias":"virtual/shares",
         "driveType":"virtual",
         "id":"a0ca6a90-a365-4782-871e-d44447bbc668$a0ca6a90-a365-4782-871e-d44447bbc668",
         "name":"Shares",
         "quota":{
            "remaining":0,
            "state":"exceeded",
            "total":0,
            "used":0
         },
         "root":{
            "eTag":"DECAFC00FEE",
            "id":"a0ca6a90-a365-4782-871e-d44447bbc668$a0ca6a90-a365-4782-871e-d44447bbc668",
            "webDavUrl":"https://cernbox.cern.ch/cernbox/desktop/remote.php/webdav/eos/user/g/gonzalhu/"
         },
         "webUrl":"https://cernbox.cern.ch"
      }
   ]
}
`
			w.Write([]byte(payload))
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
