package internal

import "testing"

func TestNormalizeJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			"Basic conversion",
			"{'a': None, 'b': 'test'}",
			`{"a": null, "b": "test"}`,
		},
		{
			"Double-double quotes unwrapped",
			`{'character': ""Dragon Jin's wife""}`,
			`{"character": "Dragon Jin's wife"}`,
		},
		{
			"None inside string remains",
			`{'key': ""None""}`,
			`{"key": "None"}`,
		},
		{
			"Empty string preserved",
			`{'key': ""}`,
			`{"key": ""}`,
		},
		{
			"Standard JSON untouched",
			`{"key": "value"}`,
			`{"key": "value"}`,
		},
		{
			"Nested with 'None' and valid quotes",
			`{'key': {'inner': None, 'another': ""empty""}}`,
			`{"key": {"inner": null, "another": "empty"}}`,
		},
		{
			"Handles apostrophes in names 1",
			`{'name': ""O'Connor""}`,
			`{"name": "O'Connor"}`,
		},
		{
			"Handles apostrophes in names 2",
			`{'name': "O'Connor"}`,
			`{"name": "O'Connor"}`,
		},
		{
			"Handles apostrophes in names 3",
			`{"name": "O'Connor"}`,
			`{"name": "O'Connor"}`,
		},
		{
			"Handles single quote in middle of word",
			`{'note': 'don''t stop'}`,
			`{"note": "don't stop"}`, // Note: this only works if input is truly malformed with doubled quotes
		},
		{
			name:     "Complex cast/crew with apostrophes and None",
			input:    `[{'cast_id': 4, 'character': 'Stuart', 'credit_id': '58f7031a9251415dec00923f', 'gender': 2, 'id': 1219597, 'name': ""Jim O'Heir"", 'order': 4, 'profile_path': None}]`,
			expected: `[{"cast_id": 4, "character": "Stuart", "credit_id": "58f7031a9251415dec00923f", "gender": 2, "id": 1219597, "name": "Jim O'Heir", "order": 4, "profile_path": null}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeJSON(tt.input)
			if got != tt.expected {
				t.Errorf("\ninput:\n  %s\ngot:\n  %s\nwant:\n  %s", tt.input, got, tt.expected)
			}
		})
	}
}
