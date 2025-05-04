package utils

// It allows you to obtain the value of a key in a map and if said key does not
// exist, it is initialized with the parameter function, returning the initialized value.
func GetOrInitKeyMap[T any](aMap *map[string]T, key string, initKey func() T) T {
	found, ok := (*aMap)[key]
	if ok {
		return found
	} else {
		(*aMap)[key] = initKey()
		return (*aMap)[key]
	}
}

// It allows you to obtain the value of a key in a map and if said key does not
// exist, it is initialized with the parameter function, returning the initialized value.
func GetOrInitKeyMapWithKey[T any](aMap *map[string]T, key string, initKey func(string) *T) T {
	found, ok := (*aMap)[key]
	if ok {
		return found
	} else {
		(*aMap)[key] = *initKey(key)
		return (*aMap)[key]
	}
}

// Callback to initialize the eof counter
func InitEOFCount() int {
	return 0
}
