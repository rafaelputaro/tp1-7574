package utils

import (
	"google.golang.org/protobuf/proto"
	"slices"
	"tp1/protobuf/protopb"
)

type ActorData struct {
	Name        string
	ProfilePath string
	CountMovies int64
}

// key: path profile
type ActorsData struct {
	index map[string]int //key path profile
	data  []ActorData
}

func newActorData(name string, profile string, count int64) *ActorData {
	return &ActorData{
		Name:        name,
		ProfilePath: profile,
		CountMovies: count,
	}
}

func NewActorsData() *ActorsData {
	return &ActorsData{
		index: make(map[string]int),
		data:  []ActorData{},
	}
}

func InitActorsData(clientId string) *ActorsData {
	return NewActorsData()
}

func (actorsData *ActorsData) UpdateCount(actor *protopb.Actor) {
	key := actor.GetProfilePath()
	foundIndex, existsData := actorsData.index[key]
	if existsData {
		// update
		foundActorData := actorsData.data[foundIndex]
		newData := *newActorData(*actor.Name, actor.GetProfilePath(), foundActorData.CountMovies+actor.GetCountMovies())
		actorsData.data[foundIndex] = newData
	} else {
		// append
		newIndex := len(actorsData.data)
		actorsData.index[key] = newIndex
		newData := *newActorData(actor.GetName(), actor.GetProfilePath(), actor.GetCountMovies())
		actorsData.data = append(actorsData.data, newData)
	}
}

// Returns: 1 coun1 < count2; 0 count1 == count2; -1 count1 > count2
func cmpData(data1, data2 ActorData) int {
	if data1.CountMovies != data2.CountMovies {
		return int(data2.CountMovies - data1.CountMovies)
	}

	if data1.Name < data2.Name {
		return -1
	} else if data1.Name > data2.Name {
		return 1
	}

	return 0
}

// After this function is called, the index becomes inconsistent.
func (actorsData *ActorsData) sort() {
	slices.SortFunc(actorsData.data, cmpData)
}

func (actorsData *ActorsData) GetTop10(clientID string) *protopb.Top10 {
	actorsData.sort()
	lenTop := 10
	lenData := len(actorsData.data)
	if lenTop > lenData {
		lenTop = lenData
	}
	toReturn := protopb.Top10{
		Names:        []string{},
		ProfilePaths: []string{},
		CountMovies:  []int64{},
		ClientId:     proto.String(clientID),
	}
	for index := range lenTop {
		toReturn.Names = append(toReturn.Names, actorsData.data[index].Name)
		toReturn.ProfilePaths = append(toReturn.ProfilePaths, actorsData.data[index].ProfilePath)
		toReturn.CountMovies = append(toReturn.CountMovies, actorsData.data[index].CountMovies)
	}
	return &toReturn
}
