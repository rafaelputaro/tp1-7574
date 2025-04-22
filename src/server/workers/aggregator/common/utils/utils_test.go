package utils

import (
	"testing"
	"tp1/protobuf/protopb"
)

func TestTop5(t *testing.T) {
	top1 := CreateEmptyTop5()
	top1.Budget[2] = 500
	top2 := protopb.Top5Country{
		Budget:              []int32{50, 2000, 10000, 500},
		ProductionCountries: []string{"Argentine", "England", "USA", "France"},
	}
	sorted := protopb.Top5Country{
		Budget:              []int32{10000, 2000, 500, 500, 50},
		ProductionCountries: []string{"USA", "England", "Empty2", "France", "Argentine"},
	}
	globalTop := ReduceTop5(&top1, &top2)
	if Top5ToString(globalTop) != Top5ToString(&sorted) {
		t.Fatal("Error on sort")
	}
}
