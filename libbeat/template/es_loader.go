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
	"fmt"
	"net/http"
	"strings"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
)

const (
	indexTemplatePath = "/_index_template/"
)

//TODO(simitt): are index templates and data streams supported by 7.8 or 7.9, or 7.x?
var minESVersionIndexTemplate = common.MustNewVersion("7.9.0")

// ESLoader implements Loader interface for loading templates to Elasticsearch.
type ESLoader struct {
	client             ESClient
	builder            *templateBuilder
	supportsDataStream bool
	log                *logp.Logger
}

// ESClient is a subset of the Elasticsearch client API capable of
// loading the template.
type ESClient interface {
	Request(method, path string, pipeline string, params map[string]string, body interface{}) (int, []byte, error)
	GetVersion() common.Version
}

// NewESLoader creates a new template loader for ES
func NewESLoader(client ESClient) *ESLoader {
	version := client.GetVersion()
	return &ESLoader{
		client:             client,
		supportsDataStream: minESVersionIndexTemplate.LessThanOrEqual(true, &version),
		builder:            newTemplateBuilder(),
		log:                logp.NewLogger("template_loader")}
}

func (l *ESLoader) SupportsDataStream() bool {
	return l.supportsDataStream
}
func (l *ESLoader) LoadIndexTemplate(config TemplateConfig, info beat.Info, fields []byte, migration bool) error {
	template, templateName, err := l.templateInfo(config, info, migration)
	if err != nil {
		return err
	}

	if l.indexTemplateExists(templateName) && !config.Overwrite {
		l.log.Infof("Index Template %s already exists and will not be overwritten.", templateName)
		return nil
	}

	//loading template to ES
	body, err := l.builder.buildBody(template, config, fields)
	if err != nil {
		return err
	}
	if err := l.loadIndexTemplate(templateName, body); err != nil {
		return fmt.Errorf("could not load template. Elasticsearch returned: %v. Template is: %s", err, body.StringToPrint())
	}
	l.log.Infof("template with name '%s' loaded.", templateName)
	return nil
}

// LoadLegacyTemplate checks if the index mapping template should be loaded
// In case the template is not already loaded or overwriting is enabled, the
// template is built and written to index
func (l *ESLoader) LoadLegacyTemplate(config TemplateConfig, info beat.Info, fields []byte, migration bool) error {
	template, templateName, err := l.templateInfo(config, info, migration)
	if err != nil {
		return err
	}

	if l.legacyTemplateExists(templateName) && !config.Overwrite {
		l.log.Infof("Legacy Template %s already exists and will not be overwritten.", templateName)
		return nil
	}

	//loading template to ES
	body, err := l.builder.buildBody(template, config, fields)
	if err != nil {
		return err
	}
	if err := l.loadLegacyTemplate(templateName, body); err != nil {
		return fmt.Errorf("could not load template. Elasticsearch returned: %v. Template is: %s", err, body.StringToPrint())
	}
	l.log.Infof("template with name '%s' loaded.", templateName)
	return nil
}

func (l *ESLoader) templateInfo(config TemplateConfig, info beat.Info, migration bool) (*Template, string, error) {
	//build template from config
	template, err := l.builder.template(config, info, l.client.GetVersion(), migration)
	if err != nil || template == nil {
		return nil, "", err
	}
	// Check if template already exist or should be overwritten
	templateName := template.GetName()
	if config.JSON.Enabled {
		templateName = config.JSON.Name
	}
	return template, templateName, nil
}

// loadLegacyTemplate loads a template into Elasticsearch overwriting the existing
// template if it exists. If you wish to not overwrite an existing template
// then use CheckTemplate prior to calling this method.
func (l *ESLoader) loadLegacyTemplate(templateName string, template map[string]interface{}) error {
	l.log.Infof("Try loading legacy template %s to Elasticsearch", templateName)
	params := esVersionParams(l.client.GetVersion())
	// `priority` only exists in index_template
	delete(template, "priority")
	return l.loadTemplate("/_template/"+templateName, params, template)
}

func (l *ESLoader) loadIndexTemplate(templateName string, template map[string]interface{}) error {
	l.log.Infof("Try loading index template %s to Elasticsearch", templateName)
	// `order` only exists in legacy template
	delete(template, "order")
	// add data stream related information:
	//template["data_stream"] = map[string]string{"timestamp_field": "@timestamp"}

	//TODO(simitt): remove rollover_alias to remove ambiguity
	//if settings, ok := template["settings"].(common.MapStr); ok {
	//	if index, ok := settings["index"].(common.MapStr); ok {
	//		if lifecycle, ok := index["lifecycle"].(common.MapStr); ok {
	//delete(lifecycle, "rollover_alias")
	// rollover_alias setting will be ignored!
	//lifecycle["rollover_alias"] = lifecycle["rollover_alias"].(string) + "-simitt"
	//index["lifecycle"] = lifecycle
	//settings["index"] = index
	//template["settings"] = settings
	//}
	//}
	//}
	// `settings`, `mappings` and `aliases` need to be nested under key `template`
	templateInfo := map[string]interface{}{}
	for _, key := range []string{"settings", "mappings", "aliases"} {
		if val, ok := template[key]; ok {
			templateInfo[key] = val
			delete(template, key)
		}
	}
	template["template"] = templateInfo
	return l.loadTemplate(indexTemplatePath+templateName, nil, template)
}

func (l *ESLoader) loadTemplate(path string, params map[string]string, template map[string]interface{}) error {
	status, body, err := l.client.Request("PUT", path, "", params, template)
	if err != nil {
		return fmt.Errorf("couldn't load template: %v. Response body: %s", err, body)
	}
	if status > http.StatusMultipleChoices { //http status 300
		return fmt.Errorf("couldn't load json. Status: %v", status)
	}
	return nil
}

// legacyTemplateExists checks if a given template already exist. It returns true if
// and only if Elasticsearch returns with HTTP status code 200.
func (l *ESLoader) legacyTemplateExists(templateName string) bool {
	if l.client == nil {
		return false
	}
	status, body, _ := l.client.Request("GET", "/_cat/templates/"+templateName, "", nil, nil)
	return status == http.StatusOK && strings.Contains(string(body), templateName)
}

func (l *ESLoader) indexTemplateExists(templateName string) bool {
	if l.client == nil {
		return false
	}
	status, _, _ := l.client.Request("GET", indexTemplatePath+templateName, "", nil, nil)
	return status == http.StatusOK
}
