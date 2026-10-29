package kube

import (
	"encoding/json"
	"os"
	"path/filepath"
)

func portForwardsStorePath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "klustr", "port-forwards.json"), nil
}

func loadPortForwards(path string) ([]PortForwardSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []PortForwardSpec{}, nil
		}
		return nil, err
	}
	specs := []PortForwardSpec{}
	if err := json.Unmarshal(data, &specs); err != nil {
		return nil, err
	}
	out := specs[:0]
	for _, s := range specs {
		if s.ID == "" || (s.Kind != "Pod" && s.Kind != "Service") || s.Port == 0 {
			continue
		}
		s.Protocol = normalizePFProtocol(s.Protocol)
		out = append(out, s)
	}
	return out, nil
}

// savePortForwards writes through a temp file so a crash mid-write cannot
// leave a truncated file that drops every saved forward.
func savePortForwards(path string, specs []PortForwardSpec) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(specs, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
