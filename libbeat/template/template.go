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
	"sync"
	"time"

	"github.com/elastic/go-ucfg/yaml"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/fmtstr"
	"github.com/elastic/beats/v7/libbeat/mapping"
)

const (
	aliasesKey       = "aliases"
	composedOfKey    = "composed_of"
	dataStreamKey    = "data_stream"
	indexPatternsKey = "index_patterns"
	mappingsKey      = "mappings"
	orderKey         = "order"
	priorityKey      = "priority"
	settingsKey      = "settings"
)

var (
	// Defaults used in the template
	defaultDateDetection         = false
	defaultTotalFieldsLimit      = 10000
	defaultNumberOfRoutingShards = 30

	// Array to store dynamicTemplate parts in
	dynamicTemplates []common.MapStr

	defaultFields []string
)

// Template holds information for the ES template.
type Template struct {
	sync.Mutex
	name        string
	pattern     string
	beatVersion common.Version
	beatName    string
	esVersion   common.Version
	config      TemplateConfig
	migration   bool
	order       int
	priority    int
}

// New creates a new template instance
func New(
	beatVersion string,
	beatName string,
	esVersion common.Version,
	config TemplateConfig,
	migration bool,
) (*Template, error) {
	bV, err := common.NewVersion(beatVersion)
	if err != nil {
		return nil, err
	}

	name := config.Name
	if name == "" {
		name = fmt.Sprintf("%s-%s", beatName, bV.String())
	}

	pattern := config.Pattern
	if pattern == "" {
		pattern = name + "-*"
	}

	event := &beat.Event{
		Fields: common.MapStr{
			// beat object was left in for backward compatibility reason for older configs.
			"beat": common.MapStr{
				"name":    beatName,
				"version": bV.String(),
			},
			"agent": common.MapStr{
				"name":    beatName,
				"version": bV.String(),
			},
			// For the Beats that have an observer role
			"observer": common.MapStr{
				"name":    beatName,
				"version": bV.String(),
			},
		},
		Timestamp: time.Now(),
	}

	name, err = formatStr(event, name)
	if err != nil {
		return nil, err
	}

	pattern, err = formatStr(event, pattern)
	if err != nil {
		return nil, err
	}

	var composedOf []string
	for _, c := range config.ComposedOf {
		fmtd, err := formatStr(event, c)
		if err != nil {
			return nil, err
		}
		composedOf = append(composedOf, fmtd)
	}
	config.ComposedOf = composedOf

	// In case no esVersion is set, it is assumed the same as beat version
	if !esVersion.IsValid() {
		esVersion = *bV
	}

	return &Template{
		pattern:     pattern,
		name:        name,
		beatVersion: *bV,
		esVersion:   esVersion,
		beatName:    beatName,
		config:      config,
		migration:   migration,
		order:       config.Order,
		priority:    config.Priority,
	}, nil
}

func formatStr(event *beat.Event, toCompile string) (string, error) {
	fmt, err := fmtstr.CompileEvent(toCompile)
	if err != nil {
		return "", err
	}
	return fmt.Run(event)
}

func (t *Template) load(fields mapping.Fields) (common.MapStr, error) {

	// Locking to make sure dynamicTemplates and defaultFields is not accessed in parallel
	t.Lock()
	defer t.Unlock()

	dynamicTemplates = nil
	defaultFields = nil

	var err error
	if len(t.config.AppendFields) > 0 {
		fields, err = mapping.ConcatFields(fields, t.config.AppendFields)
		if err != nil {
			return nil, err
		}
	}

	// Start processing at the root
	properties := common.MapStr{}
	processor := Processor{EsVersion: t.esVersion, Migration: t.migration}
	if err := processor.Process(fields, nil, properties); err != nil {
		return nil, err
	}
	output := t.Generate(properties, dynamicTemplates)

	return output, nil
}

// LoadFile loads the the template from the given file path
func (t *Template) LoadFile(file string) (common.MapStr, error) {
	fields, err := mapping.LoadFieldsYaml(file)
	if err != nil {
		return nil, err
	}

	return t.load(fields)
}

// LoadBytes loads the template from the given byte array
func (t *Template) LoadBytes(data []byte) (common.MapStr, error) {
	fields, err := loadYamlByte(data)
	if err != nil {
		return nil, err
	}

	return t.load(fields)
}

// LoadMinimal loads the template only with the given configuration
func (t *Template) LoadMinimal() (common.MapStr, error) {
	m := t.baseSettings()
	if t.config.Settings.Index != nil {
		m[settingsKey] = common.MapStr{
			"index": t.config.Settings.Index,
		}
	}
	if t.config.Settings.Source != nil {
		m[mappingsKey] = buildMappings(
			t.beatVersion, t.esVersion, t.beatName,
			nil, nil,
			t.config.Settings.Source)
	}
	return m, nil
}

// GetName returns the name of the template
func (t *Template) GetName() string {
	return t.name
}

// GetPattern returns the pattern of the template
func (t *Template) GetPattern() string {
	return t.pattern
}

// Generate generates the full template
// The default values are taken from the default variable.
func (t *Template) Generate(properties common.MapStr, dynamicTemplates []common.MapStr) common.MapStr {
	m := t.baseSettings()
	m[mappingsKey] = buildMappings(
		t.beatVersion, t.esVersion, t.beatName,
		properties,
		append(dynamicTemplates, buildDynTmpl(t.esVersion)),
		common.MapStr(t.config.Settings.Source))
	m[settingsKey] = common.MapStr{
		"index": buildIdxSettings(
			t.esVersion,
			t.config.Settings.Index,
		),
	}
	return m
}

func (t *Template) baseSettings() common.MapStr {
	keyPattern, patterns := buildPatternSettings(t.esVersion, t.GetPattern())
	m := common.MapStr{
		keyPattern:  patterns,
		orderKey:    t.order,
		priorityKey: t.priority,
	}
	if len(t.config.ComposedOf) > 0 {
		m[composedOfKey] = t.config.ComposedOf
	}
	if t.config.DataStream != nil {
		m[dataStreamKey] = t.config.DataStream
	}
	return m
}

func buildPatternSettings(ver common.Version, pattern string) (string, interface{}) {
	if ver.Major < 6 {
		return "template", pattern
	}
	return indexPatternsKey, []string{pattern}
}

func buildMappings(
	beatVersion, esVersion common.Version,
	beatName string,
	properties common.MapStr,
	dynTmpls []common.MapStr,
	source common.MapStr,
) common.MapStr {
	mapping := common.MapStr{
		"_meta": common.MapStr{
			"version": beatVersion.String(),
			"beat":    beatName,
		},
		"date_detection":    defaultDateDetection,
		"dynamic_templates": dynTmpls,
		"properties":        properties,
	}

	if len(source) > 0 {
		mapping["_source"] = source
	}

	major := esVersion.Major
	switch {
	case major == 2:
		mapping.Put("_all.norms.enabled", false)
		mapping = common.MapStr{
			"_default_": mapping,
		}
	case major < 6:
		mapping = common.MapStr{
			"_default_": mapping,
		}
	case major == 6:
		mapping = common.MapStr{
			"doc": mapping,
		}
	case major >= 7:
		// keep typeless structure
	}

	return mapping
}

func buildDynTmpl(ver common.Version) common.MapStr {
	strMapping := common.MapStr{
		"ignore_above": 1024,
		"type":         "keyword",
	}
	if ver.Major == 2 {
		strMapping["type"] = "string"
		strMapping["index"] = "not_analyzed"
	}

	return common.MapStr{
		"strings_as_keyword": common.MapStr{
			"mapping":            strMapping,
			"match_mapping_type": "string",
		},
	}
}

func buildIdxSettings(ver common.Version, userSettings common.MapStr) common.MapStr {
	indexSettings := common.MapStr{
		"refresh_interval": "5s",
		"mapping": common.MapStr{
			"total_fields": common.MapStr{
				"limit": defaultTotalFieldsLimit,
			},
		},
	}

	// number_of_routing shards is only supported for ES version >= 6.1
	// If ES >= 7.0 we can exclude this setting as well.
	version61, _ := common.NewVersion("6.1.0")
	if !ver.LessThan(version61) && ver.Major < 7 {
		indexSettings.Put("number_of_routing_shards", defaultNumberOfRoutingShards)
	}

	if ver.Major >= 7 {
		// copy defaultFields, as defaultFields is shared global slice.
		fields := make([]string, len(defaultFields))
		copy(fields, defaultFields)
		fields = append(fields, "fields.*")

		indexSettings.Put("query.default_field", fields)
	}

	indexSettings.DeepUpdate(userSettings)
	return indexSettings
}

func loadYamlByte(data []byte) (mapping.Fields, error) {
	cfg, err := yaml.NewConfig(data)
	if err != nil {
		return nil, err
	}

	var keys []mapping.Field
	err = cfg.Unpack(&keys)
	if err != nil {
		return nil, err
	}

	fields := mapping.Fields{}
	for _, key := range keys {
		fields = append(fields, key.Fields...)
	}
	return fields, nil
}
