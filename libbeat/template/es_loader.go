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
	"sync"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
)

var templatePath = map[Kind]string{
	KindLegacy:    "/_template/",
	KindComponent: "/_component_template/",
	KindIndex:     "/_index_template/",
}

// ESLoader implements Loader interface for loading templates to Elasticsearch.
type ESLoader struct {
	client                                     ESClient
	builder                                    *templateBuilder
	supportsIndexTemplates, supportsDataStream bool
	onceDatastreamSupported                    sync.Once
	log                                        *logp.Logger
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
		client:                 client,
		supportsIndexTemplates: minESVersionIndexTemplate.LessThanOrEqual(true, &version),
		builder:                newTemplateBuilder(),
		log:                    logp.NewLogger("template_loader")}
}

// SupportsDataStream returns true if the configured ES connection supports data streams
func (l *ESLoader) SupportsDataStream() bool {
	l.onceDatastreamSupported.Do(func() {
		version := l.client.GetVersion()
		if version.LessThan(minESVersionDataStreams) {
			l.supportsDataStream = false
			return
		}
		//TODO(simitt): check if datastreams will be included as features (they aren't right now)
		status, _, err := l.client.Request("GET", "/_xpack", "", nil, nil)
		if status >= 400 || err != nil {
			l.supportsDataStream = false
		}
		l.supportsDataStream = true
	})
	return l.supportsDataStream
}

// SupportsIndexTemplates returns true if the configured ES version supports index templates
func (l *ESLoader) SupportsIndexTemplates() bool {
	return l.supportsIndexTemplates
}

// Load checks if the index mapping template should be loaded
// In case the template is not already loaded or overwriting is enabled, the
// template is built and written to index
func (l *ESLoader) Load(config TemplateConfig, info beat.Info, fields []byte, migration bool) error {
	template, templateName, err := l.templateInfo(config, info, migration)
	if err != nil {
		return err
	}

	// check if template already exists and if it should be overwritten
	if !config.Overwrite && l.exists(templatePath[config.Kind], templateName) {
		l.log.Infof("%s Template %s already exists and will not be overwritten.", config.Kind, templateName)
		return nil
	}

	// building template body
	body, err := l.builder.buildBody(template, config, fields)
	if err != nil {
		return err
	}

	// make request to Elasticsearch
	l.log.Infof("Try loading %s template %s to Elasticsearch", config.Kind, templateName)
	params := esVersionParams(l.client.GetVersion())
	if err := l.request(templatePath[config.Kind]+templateName, params, body); err != nil {
		return fmt.Errorf("could not load %s template. Elasticsearch returned: %v. Template is: %s", config.Kind.String(), err, body.StringToPrint())
	}
	l.log.Infof("%s template with name '%s' loaded.", config.Kind, templateName)
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

func (l *ESLoader) request(path string, params map[string]string, template map[string]interface{}) error {
	status, body, err := l.client.Request("PUT", path, "", params, template)
	if err != nil {
		return fmt.Errorf("couldn't load template: %v. Response body: %s", err, body)
	}
	if status > http.StatusMultipleChoices { //http status 300
		return fmt.Errorf("couldn't load json. Status: %v", status)
	}
	return nil
}

func (l *ESLoader) exists(path, name string) bool {
	if l.client == nil {
		return false
	}
	status, body, _ := l.client.Request("GET", path+name, "", nil, nil)
	return status == http.StatusOK && strings.Contains(string(body), name)
}
