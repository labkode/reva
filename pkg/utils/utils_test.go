// Copyright 2018-2021 CERN
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

package utils

import (
	"errors"
	"testing"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
)

var skipTests = []struct {
	name string
	url  string
	base []string
	out  bool
}{
	{"valid subpath", "/a/b/c/d", []string{"/a/b/"}, true},
	{"invalid subpath", "/a/b/c", []string{"/a/b/c/d"}, false},
	{"equal values", "/a/b/c", []string{"/a/b/c"}, true},
}

func TestSkip(t *testing.T) {
	for _, tt := range skipTests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			r := Skip(tt.url, tt.base)
			if r != tt.out {
				t.Errorf("expected %v, want %v", r, tt.out)
			}
		})
	}
}
func TestIsRelativeReference(t *testing.T) {
	tests := []struct {
		ref      *provider.Reference
		expected bool
	}{
		{
			&provider.Reference{},
			false,
		},
		{
			&provider.Reference{
				Path: ".",
			},
			false,
		},
		{
			&provider.Reference{
				ResourceId: &provider.ResourceId{
					StorageId: "storageId",
					OpaqueId:  "opaqueId",
				},
				Path: "/folder",
			},
			false,
		},
		{
			&provider.Reference{
				ResourceId: &provider.ResourceId{
					StorageId: "storageId",
					OpaqueId:  "opaqueId",
				},
				Path: "./folder",
			},
			true,
		},
		{
			&provider.Reference{
				ResourceId: &provider.ResourceId{},
				Path:       "./folder",
			},
			true,
		},
	}

	for _, tt := range tests {
		result := IsRelativeReference(tt.ref)
		if result != tt.expected {
			t.Errorf("IsRelativeReference: ref %v expected %t got %t", tt.ref, tt.expected, result)
		}
	}
}
func TestIsAbsolutReference(t *testing.T) {
	tests := []struct {
		ref      *provider.Reference
		expected bool
	}{
		{
			&provider.Reference{},
			false,
		},
		{
			&provider.Reference{
				Path: ".",
			},
			false,
		},
		{
			&provider.Reference{
				ResourceId: &provider.ResourceId{
					StorageId: "storageId",
					OpaqueId:  "opaqueId",
				},
				Path: "/folder",
			},
			false,
		},
		{
			&provider.Reference{
				Path: "/folder",
			},
			true,
		},
		{
			&provider.Reference{
				ResourceId: &provider.ResourceId{},
			},
			true,
		},
		{
			&provider.Reference{
				ResourceId: &provider.ResourceId{
					StorageId: "storageId",
					OpaqueId:  "opaqueId",
				},
			},
			true,
		},
	}

	for _, tt := range tests {
		result := IsAbsoluteReference(tt.ref)
		if result != tt.expected {
			t.Errorf("IsAbsolutReference: ref %v expected %t got %t", tt.ref, tt.expected, result)
		}
	}
}

func TestMakeRelativePath(t *testing.T) {
	tests := []struct {
		path    string
		relPath string
	}{
		{"", "."},
		{"/", "."},
		{"..", "."},
		{"/folder", "./folder"},
		{"/folder/../folder2", "./folder2"},
		{"folder", "./folder"},
	}
	for _, tt := range tests {
		rel := MakeRelativePath(tt.path)
		if rel != tt.relPath {
			t.Errorf("expected %s, got %s", tt.relPath, rel)
		}
	}
}

func TestParseStorageSpaceReference(t *testing.T) {
	tests := []struct {
		sRef      string
		storageID string
		nodeID    string
		relPath   string
	}{
		{
			"1234!abcd/f1/f2",
			"1234",
			"abcd",
			"./f1/f2",
		},
		{
			"1234!abcd",
			"1234",
			"abcd",
			".",
		},
	}
	for _, tt := range tests {
		ref, _ := ParseStorageSpaceReference(tt.sRef)

		if ref.ResourceId.StorageId != tt.storageID {
			t.Errorf("Expected storageId %s got %s", tt.storageID, ref.ResourceId.StorageId)
		}
		if ref.ResourceId.OpaqueId != tt.nodeID {
			t.Errorf("Expected OpaqueId %s got %s", tt.nodeID, ref.ResourceId.OpaqueId)
		}
		if ref.Path != tt.relPath {
			t.Errorf("Expected path %s got %s", tt.relPath, ref.Path)
		}
	}
}

func TestFormatStorageSpaceReference(t *testing.T) {
	tests := []struct {
		ref         *provider.Reference
		expected    string
		expectedErr error
	}{
		{
			ref:         &provider.Reference{},
			expected:    "",
			expectedErr: errInvalidSpaceReference,
		},
		{
			ref:         &provider.Reference{ResourceId: &provider.ResourceId{}},
			expected:    "!",
			expectedErr: errInvalidSpaceReference,
		},
		{
			ref:         &provider.Reference{ResourceId: &provider.ResourceId{OpaqueId: "opaqueid"}, Path: "path"},
			expectedErr: errInvalidSpaceReference,
		},
		{
			ref:      &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "storageid"}, Path: "path"},
			expected: "storageid/path",
		},
		{
			ref:      &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "storageid", OpaqueId: "opaqueid"}, Path: "path"},
			expected: "storageid!opaqueid/path",
		},
		{
			ref:      &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "storageid", OpaqueId: "opaqueid"}, Path: "."},
			expected: "storageid!opaqueid",
		},
		{
			ref:      &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "storageid"}, Path: "."},
			expected: "storageid",
		},
		{
			ref:      &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "storageid"}},
			expected: "storageid",
		},
	}

	for _, tt := range tests {
		result, err := FormatStorageSpaceReference(tt.ref)
		if err != nil && !errors.Is(err, tt.expectedErr) {
			t.Errorf("FormateStorageSpaceRefence returned unexpected error: %v", err)
		}
		if err == nil && result != tt.expected {
			t.Errorf("Reference %v got formatted to %s, expected %s", tt.ref, result, tt.expected)
		}
	}
}

func TestFormatAndParseReference(t *testing.T) {
	tests := []struct {
		orig     *provider.Reference
		expected *provider.Reference
	}{
		{
			orig:     &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "storageid"}, Path: "./path"},
			expected: &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "storageid", OpaqueId: "storageid"}, Path: "./path"},
		},
		{
			orig:     &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "storageid", OpaqueId: "opaqueid"}, Path: "./path"},
			expected: &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "storageid", OpaqueId: "opaqueid"}, Path: "./path"},
		},
		{
			orig:     &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "storageid", OpaqueId: "opaqueid"}, Path: "."},
			expected: &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "storageid", OpaqueId: "opaqueid"}, Path: "."},
		},
		{
			orig:     &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "storageid"}, Path: "."},
			expected: &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "storageid", OpaqueId: "storageid"}, Path: "."},
		},
		{
			orig:     &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "storageid"}},
			expected: &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "storageid", OpaqueId: "storageid"}, Path: "."},
		},
	}

	for _, tt := range tests {
		formatted, _ := FormatStorageSpaceReference(tt.orig)
		parsed, err := ParseStorageSpaceReference(formatted)
		if err != nil {
			t.Errorf("failed to parse space reference: %s error: %s", formatted, err)
		}
		if !(ResourceIDEqual(parsed.ResourceId, tt.expected.ResourceId) && parsed.Path == tt.expected.Path) {
			t.Errorf("Formatted then parsed references don't match the original got: %v expected %v", parsed, tt.expected)
		}
	}
}
