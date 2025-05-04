package utils

func GetOrInitKeyMap[T any](aMap *map[string]T, key string, initKey func() T) T {
	found, ok := (*aMap)[key]
	if ok {
		return found
	} else {
		(*aMap)[key] = initKey()
		return (*aMap)[key]
	}
}

func InitEOFCount() int {
	return 0
}
