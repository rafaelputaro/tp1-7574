package lib

import (
	"github.com/pkg/errors"
	"github.com/spf13/viper"

	f "tp1/server/workers/filter/lib/filter"
)

func LoadConfig() (*f.FilterConfig, error) {
	viper.AutomaticEnv()

	filterType := viper.GetString("FILTER_TYPE")
	if filterType == "" {
		return nil, errors.New("missing FILTER_TYPE environment variable")
	}

	filterNum := viper.GetInt("FILTER_NUM")
	if filterNum == 0 {
		return nil, errors.New("missing or invalid FILTER_NUM environment variable")
	}

	return &f.FilterConfig{
		Type: filterType,
		ID:   filterNum,
	}, nil
}
