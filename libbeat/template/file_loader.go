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

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
)

// FileLoader implements Loader interface for loading templates to a File.
type FileLoader struct {
	client  FileClient
	builder *templateBuilder
	log     *logp.Logger
}

// FileClient defines the minimal interface required for the FileLoader
type FileClient interface {
	GetVersion() common.Version
	Write(component string, name string, body string) error
}

// NewFileLoader creates a new template loader for the given file.
func NewFileLoader(c FileClient) *FileLoader {
	return &FileLoader{client: c, builder: newTemplateBuilder(), log: logp.NewLogger("file_template_loader")}
}

//TODO(simitt): respect `legacy` setting
// Load reads the template from the config, creates the template body and prints it to the configured file.
func (l *FileLoader) LoadLegacyTemplate(config TemplateConfig, info beat.Info, fields []byte, migration bool) error {
	//build template from config
	tmpl, err := l.builder.template(config, info, l.client.GetVersion(), migration)
	if err != nil || tmpl == nil {
		return err
	}

	//create body to print
	body, err := l.builder.buildBody(tmpl, config, fields)
	if err != nil {
		return err
	}

	str := fmt.Sprintf("%s\n", body.StringToPrint())
	if err := l.client.Write("template", tmpl.name, str); err != nil {
		return fmt.Errorf("error printing template: %v", err)
	}
	return nil
}

func (l *FileLoader) SupportsDataStream() bool {
	fmt.Println("---- Not implemented ----")
	return true
}
func (l *FileLoader) LoadIndexTemplate(config TemplateConfig, info beat.Info, fields []byte, migration bool) error {
	fmt.Println("---- Not implemented ----")
	return nil
}
