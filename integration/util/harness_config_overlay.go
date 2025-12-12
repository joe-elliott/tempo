package util

import (
	"bytes"
	"fmt"
	"html/template"
	"os"

	"github.com/grafana/e2e"
	"go.yaml.in/yaml/v2"
)

// applyConfigOverlay applies a config overlay file onto the shared config.yaml file,
// with optional template rendering. The overlay is merged onto the existing shared config
// and written back to shared config.yaml.
func applyConfigOverlay(s *e2e.Scenario, overlayPath string, templateData map[string]any) error {
	configPath := s.SharedDir() + "/config.yaml" // make a shared func somewhere

	// Read and parse current shared config
	baseBuff, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read shared config file: %w", err)
	}

	var baseMap map[any]any
	err = yaml.Unmarshal(baseBuff, &baseMap)
	if err != nil {
		return fmt.Errorf("failed to parse shared config file: %w", err)
	}

	// If there's an overlay, apply it
	if overlayPath != "" {
		// Read overlay file
		overlayBuff, err := os.ReadFile(overlayPath)
		if err != nil {
			return fmt.Errorf("failed to read config overlay file: %w", err)
		}

		// Apply template rendering if template data is provided
		if len(templateData) > 0 {
			tmpl, err := template.New("config").Parse(string(overlayBuff))
			if err != nil {
				return fmt.Errorf("failed to parse config overlay template: %w", err)
			}

			var renderedBuff bytes.Buffer
			err = tmpl.Execute(&renderedBuff, templateData)
			if err != nil {
				return fmt.Errorf("failed to execute config overlay template: %w", err)
			}

			overlayBuff = renderedBuff.Bytes()
		}

		// Parse overlay
		var overlayMap map[any]any
		err = yaml.Unmarshal(overlayBuff, &overlayMap)
		if err != nil {
			return fmt.Errorf("failed to parse config overlay file: %w", err)
		}

		// Merge overlay onto base
		baseMap = mergeMaps(baseMap, overlayMap)
	}

	// Marshal and write the result back to shared config jpe - use writeFileToSharedDir func?
	outputBytes, err := yaml.Marshal(baseMap)
	if err != nil {
		return fmt.Errorf("failed to marshal merged config: %w", err)
	}

	err = os.WriteFile(configPath, outputBytes, 0644)
	if err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// mergeMaps recursively merges overlay map onto base map
// Values in overlay take precedence over base values
func mergeMaps(base, overlay map[any]any) map[any]any {
	result := make(map[any]any)

	// Copy all base values
	for k, v := range base {
		result[k] = v
	}

	// Overlay values, recursively merging nested maps
	for k, v := range overlay {
		if v == nil {
			result[k] = v
			continue
		}

		// If both base and overlay have a map at this key, merge recursively
		if baseVal, exists := result[k]; exists {
			baseMap, baseIsMap := toMapAnyAny(baseVal)
			overlayMap, overlayIsMap := toMapAnyAny(v)

			if baseIsMap && overlayIsMap {
				result[k] = mergeMaps(baseMap, overlayMap)
				continue
			}
		}

		// Otherwise, overlay value replaces base value
		result[k] = v
	}

	return result
}

// toMapAnyAny converts various map types to map[any]any
func toMapAnyAny(v any) (map[any]any, bool) {
	switch m := v.(type) {
	case map[any]any:
		return m, true
	case map[string]any:
		result := make(map[any]any)
		for k, v := range m {
			result[k] = v
		}
		return result, true
	default:
		return nil, false
	}
}
