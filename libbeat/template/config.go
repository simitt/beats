// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package template

import (
	"strings"

	"github.com/elastic/beats/v7/libbeat/mapping"
)

// TemplateConfig holds config information about the Elasticsearch template
type TemplateConfig struct {
	Enabled bool   `config:"enabled"`
	Name    string `config:"name"`
	Pattern string `config:"pattern"`
	Fields  string `config:"fields"`
	JSON    struct {
		Enabled bool   `config:"enabled"`
		Path    string `config:"path"`
		Name    string `config:"name"`
	} `config:"json"`
	AppendFields mapping.Fields    `config:"append_fields"`
	Overwrite    bool              `config:"overwrite"`
	Settings     TemplateSettings  `config:"settings"`
	Order        int               `config:"order"`
	Priority     int               `config:"priority"`
	Kind         Kind              `config:"kind"` // index or legacy (default: legacy)
	ComposedOf   []string          `config:"-"`
	DataStream   map[string]string `config:"-"`
}

// TemplateSettings are part of the Elasticsearch template and hold index and source specific information.
type TemplateSettings struct {
	Index  map[string]interface{} `config:"index"`
	Source map[string]interface{} `config:"_source"`
}

// Kind is used for enumerating the template kind that should be loaded.
// TODO(simitt): stringify
type Kind uint8

//go:generate stringer -type Kind -trimprefix Kind
const (
	KindLegacy Kind = iota
	KindIndex
	KindComponent
)

//Unpack creates enumeration values for template kind
func (k *Kind) Unpack(in string) error {
	in = strings.ToLower(in)
	switch in {
	case "legacy":
		*k = KindLegacy
	case "component":
		*k = KindComponent
	case "index":
		*k = KindIndex
	default:
		*k = KindLegacy
	}
	return nil
}

// DefaultConfig for template
func DefaultConfig() TemplateConfig {
	return TemplateConfig{
		Enabled:  true,
		Fields:   "",
		Order:    1,
		Priority: 150,
		Kind:     KindLegacy,
	}
}
