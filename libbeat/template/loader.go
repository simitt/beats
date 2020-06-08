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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/paths"
)

// TODO(simitt): which version supports data streams?
var minESVersionDataStreams = common.MustNewVersion("7.9.0")
var minESVersionIndexTemplate = common.MustNewVersion("7.8.0")

//Loader interface for loading templates
type Loader interface {
	SupportsDataStream() bool
	SupportsIndexTemplates() bool
	Load(config TemplateConfig, info beat.Info, fields []byte, migration bool) error
}

type templateBuilder struct {
	log *logp.Logger
}

func newTemplateBuilder() *templateBuilder {
	return &templateBuilder{log: logp.NewLogger("template")}
}

func (b *templateBuilder) template(config TemplateConfig, info beat.Info, esVersion common.Version, migration bool) (*Template, error) {
	if !config.Enabled {
		b.log.Info("template config not enabled")
		return nil, nil
	}
	tmpl, err := New(info.Version, info.IndexPrefix, esVersion, config, migration)
	if err != nil {
		return nil, fmt.Errorf("error creating template instance: %v", err)
	}
	return tmpl, nil
}

func (b *templateBuilder) buildBody(tmpl *Template, config TemplateConfig, fields []byte) (common.MapStr, error) {
	if config.Overwrite {
		b.log.Info("Existing template will be overwritten, as overwrite is enabled.")
	}

	var err error
	var body common.MapStr
	if config.JSON.Enabled {
		body, err = b.buildBodyFromJSON(config)
	} else if config.Fields != "" {
		body, err = b.buildBodyFromFile(tmpl, config)
	} else if fields == nil {
		body, err = b.buildMinimalTemplate(tmpl)
	} else {
		body, err = b.buildBodyFromFields(tmpl, fields)
	}
	if err != nil {
		return nil, err
	}

	moveTemplate := func() {
		// `settings`, `mappings` and `aliases` need to be nested under key `template`
		// for component and index templates
		var templateInfo map[string]interface{}
		for _, key := range []string{settingsKey, mappingsKey, aliasesKey} {
			if val, ok := body[key]; ok {
				if templateInfo == nil {
					templateInfo = map[string]interface{}{}
				}
				templateInfo[key] = val
				delete(body, key)
			}
		}
		if templateInfo != nil {
			body["template"] = templateInfo
		}
	}

	switch config.Kind {
	case KindComponent:
		delete(body, orderKey)
		delete(body, indexPatternsKey)
		moveTemplate()
		fallthrough
	case KindLegacy:
		delete(body, priorityKey)
		delete(body, composedOfKey)
		delete(body, dataStreamKey)
	case KindIndex:
		delete(body, orderKey)
		moveTemplate()
	}
	return body, nil
}

func (b *templateBuilder) buildBodyFromJSON(config TemplateConfig) (common.MapStr, error) {
	jsonPath := paths.Resolve(paths.Config, config.JSON.Path)
	if _, err := os.Stat(jsonPath); err != nil {
		return nil, fmt.Errorf("error checking json file %s for template: %v", jsonPath, err)
	}
	b.log.Debugf("Loading json template from file %s", jsonPath)
	content, err := ioutil.ReadFile(jsonPath)
	if err != nil {
		return nil, fmt.Errorf("error reading file %s for template: %v", jsonPath, err)

	}
	var body map[string]interface{}
	err = json.Unmarshal(content, &body)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal json template: %s", err)
	}
	return body, nil
}

func (b *templateBuilder) buildBodyFromFile(tmpl *Template, config TemplateConfig) (common.MapStr, error) {
	b.log.Debugf("Load fields.yml from file: %s", config.Fields)
	fieldsPath := paths.Resolve(paths.Config, config.Fields)
	body, err := tmpl.LoadFile(fieldsPath)
	if err != nil {
		return nil, fmt.Errorf("error creating template from file %s: %v", fieldsPath, err)
	}
	return body, nil
}

func (b *templateBuilder) buildBodyFromFields(tmpl *Template, fields []byte) (common.MapStr, error) {
	b.log.Debug("Load default fields")
	body, err := tmpl.LoadBytes(fields)
	if err != nil {
		return nil, fmt.Errorf("error creating template: %v", err)
	}
	return body, nil
}

func (b *templateBuilder) buildMinimalTemplate(tmpl *Template) (common.MapStr, error) {
	b.log.Debug("Load minimal template")
	body, err := tmpl.LoadMinimal()
	if err != nil {
		return nil, fmt.Errorf("error creating mimimal template: %v", err)
	}
	return body, nil
}

func esVersionParams(ver common.Version) map[string]string {
	if ver.Major == 6 && ver.Minor == 7 {
		return map[string]string{
			"include_type_name": "true",
		}
	}

	return nil
}
